// Package reconciler implements an embeddable, in-process declarative
// resource reconciler over a local store and a local side-effect bus.
package reconciler

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/h3yow/declarative-resource-reconciler-go/pkg/api"
	"github.com/h3yow/declarative-resource-reconciler-go/pkg/bus"
	"github.com/h3yow/declarative-resource-reconciler-go/pkg/store"
)

// Reconciler serializes work per resource id, converges desired state with
// idempotent writes, retries transient failures with backoff, records
// permanent failures as stable conditions, and keeps deleting resources
// until cleanup completes.
type Reconciler struct {
	store  *store.Store
	bus    *bus.Bus
	logger *log.Logger
	bo     api.BackoffConfig

	mu         sync.Mutex
	desired    map[string]api.DesiredState
	deleting   map[string]bool
	queues     map[string]*keyQueue
	workerStop map[string]context.CancelFunc
	decisions  map[string][]api.Decision
	compatSeen map[string][]string

	stats api.Stats

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// New constructs a reconciler over the given store and effect bus.
func New(st *store.Store, effectBus *bus.Bus, logger *log.Logger, bo api.BackoffConfig) *Reconciler {
	if logger == nil {
		logger = log.New(os.Stderr, "reconciler ", log.LstdFlags|log.Lmicroseconds)
	}
	if bo.Initial <= 0 || bo.Max <= 0 || bo.Multiplier <= 1 {
		bo = api.DefaultBackoff()
	}
	return &Reconciler{
		store:      st,
		bus:        effectBus,
		logger:     logger,
		bo:         bo,
		desired:    map[string]api.DesiredState{},
		deleting:   map[string]bool{},
		queues:     map[string]*keyQueue{},
		workerStop: map[string]context.CancelFunc{},
		decisions:  map[string][]api.Decision{},
		compatSeen: map[string][]string{},
	}
}

// Start launches reconciler internals.
func (r *Reconciler) Start(ctx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cancel != nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	r.ctx, r.cancel = context.WithCancel(ctx)
}

// Stop cancels all per-resource workers and waits for in-flight passes to
// drain. No trigger is dropped silently: callers should observe final state
// before stopping.
func (r *Reconciler) Stop() {
	r.mu.Lock()
	cancel := r.cancel
	stops := make([]context.CancelFunc, 0, len(r.workerStop))
	for _, c := range r.workerStop {
		stops = append(stops, c)
	}
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	for _, c := range stops {
		c()
	}
	r.wg.Wait()
}

// Trigger submits (or refreshes) desired state. The request is never
// dropped: every generation is applied at least once; same-id triggers are
// serialized. A stale (lower-generation) trigger is accepted for ordering
// but never overwrites newer desired state.
func (r *Reconciler) Trigger(d api.DesiredState) {
	if d.ID == "" {
		r.logger.Printf("trigger rejected: empty id: %+v", d)
		return
	}
	atomic.AddInt64(&r.stats.Triggers, 1)
	r.logger.Printf("trigger input id=%s generation=%d spec=%+v", d.ID, d.Generation, d.Spec)

	r.mu.Lock()
	if cur, ok := r.desired[d.ID]; !ok || d.Generation >= cur.Generation {
		r.desired[d.ID] = d
	} else {
		r.logger.Printf("trigger id=%s generation=%d is older than accepted generation=%d; queued but will not overwrite",
			d.ID, d.Generation, cur.Generation)
	}
	delete(r.deleting, d.ID)
	q := r.lockedQueue(d.ID)
	ctx := r.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	r.lockedEnsureWorkerLocked(d.ID, ctx)
	r.mu.Unlock()

	q.enqueue()
}

// Delete marks a resource for deletion; removal waits for cleanup of the
// external effect. Triggers arriving after Delete cancel the deletion.
func (r *Reconciler) Delete(req api.DeleteRequest) {
	if req.ID == "" {
		r.logger.Printf("delete rejected: empty id: %+v", req)
		return
	}
	atomic.AddInt64(&r.stats.Triggers, 1)
	r.logger.Printf("delete input id=%s generation=%d", req.ID, req.Generation)

	r.mu.Lock()
	r.deleting[req.ID] = true
	if cur, ok := r.desired[req.ID]; ok && req.Generation >= cur.Generation {
		delete(r.desired, req.ID)
	}
	q := r.lockedQueue(req.ID)
	ctx := r.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	r.lockedEnsureWorkerLocked(req.ID, ctx)
	r.mu.Unlock()

	q.enqueue()
}

// Desired returns the latest accepted desired state for id.
func (r *Reconciler) Desired(id string) (api.DesiredState, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	d, ok := r.desired[id]
	return d, ok
}

// Stats returns a point-in-time copy of internal counters.
func (r *Reconciler) Stats() api.Stats {
	s := api.Stats{
		Passes:       atomic.LoadInt64(&r.stats.Passes),
		Triggers:     atomic.LoadInt64(&r.stats.Triggers),
		Retries:      atomic.LoadInt64(&r.stats.Retries),
		Writes:       atomic.LoadInt64(&r.stats.Writes),
		Skips:        atomic.LoadInt64(&r.stats.Skips),
		EnsureCalls:  r.bus.TotalEnsureCalls(),
		CleanupCalls: r.bus.TotalCleanupCalls(),
	}
	return s
}

// CompatFields returns unknown/legacy field names observed while
// reconciling id. Safe to call concurrently with reconciliation.
func (r *Reconciler) CompatFields(id string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.compatSeen[id]...)
}

// Decisions returns the explainable outcome log for id (oldest first).
func (r *Reconciler) Decisions(id string) []api.Decision {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]api.Decision(nil), r.decisions[id]...)
}

// DecisionsText renders the full decision log as a deterministic string,
// handy for test output and local verification.
func (r *Reconciler) DecisionsText(w io.Writer, id string) {
	for _, d := range r.Decisions(id) {
		fmt.Fprintf(w, "%s id=%s attempt=%d action=%s reason=%s duration=%s detail=%s\n",
			d.At.Format("15:04:05.000"), d.ID, d.Attempt, d.Action, d.Reason, d.Duration, d.Detail)
	}
}

func (r *Reconciler) lockedQueue(key string) *keyQueue {
	q, ok := r.queues[key]
	if !ok {
		q = newKeyQueue()
		r.queues[key] = q
	}
	return q
}

// lockedEnsureWorkerLocked starts (once) the single serial worker goroutine
// for key. Caller holds r.mu.
func (r *Reconciler) lockedEnsureWorkerLocked(key string, parent context.Context) {
	if _, ok := r.workerStop[key]; ok {
		return
	}
	ctx, cancel := context.WithCancel(parent)
	r.workerStop[key] = cancel
	r.wg.Add(1)
	go r.runWorker(key, ctx, cancel)
}

// runWorker is the sole goroutine touching a given id: serialization means
// concurrent triggers can never overwrite each other or be lost.
func (r *Reconciler) runWorker(key string, ctx context.Context, cancel context.CancelFunc) {
	defer r.wg.Done()
	defer cancel()
	q := func() *keyQueue {
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.queues[key]
	}()
	var attempt int
	for {
		q.wait(ctx)
		if ctx.Err() != nil {
			return
		}
		for {
			// Each iteration consumes exactly one accounted trigger.
			q.consume()
			atomic.AddInt64(&r.stats.Passes, 1)
			d := r.reconcileOnce(ctx, key, attempt+1)
			r.recordDecision(d)
			switch d.Action {
			case api.ActionRetry:
				attempt = d.Attempt
				atomic.AddInt64(&r.stats.Retries, 1)
				if !q.waitWithBackoff(ctx, nextBackoff(r.bo, attempt)) {
					return
				}
				// Do not consume: the retry belongs to the same trigger.
				continue
			default:
				attempt = 0
			}
			if q.pendingCount() == 0 {
				break
			}
		}
	}
}

// nextBackoff maps the 1-based attempt number to a capped delay.
func nextBackoff(bo api.BackoffConfig, attempt int) time.Duration {
	d := bo.Initial
	for i := 1; i < attempt; i++ {
		d = time.Duration(float64(d) * bo.Multiplier)
		if d >= bo.Max {
			return bo.Max
		}
	}
	if d > bo.Max {
		d = bo.Max
	}
	return d
}

func (r *Reconciler) recordDecision(d api.Decision) {
	r.mu.Lock()
	r.decisions[d.ID] = append(r.decisions[d.ID], d)
	r.mu.Unlock()
	r.logger.Printf("decision id=%s attempt=%d action=%s reason=%s detail=%s",
		d.ID, d.Attempt, d.Action, d.Reason, d.Detail)
}

// reconcileOnce runs exactly one converge pass for key. attempt is 1-based
// within the current retry sequence.
func (r *Reconciler) reconcileOnce(ctx context.Context, key string, attempt int) (decision api.Decision) {
	start := time.Now()
	defer func() {
		if rec := recover(); rec != nil {
			// A malformed local state must never crash the worker loop.
			decision = api.Decision{
				ID: key, Attempt: attempt, Action: api.ActionRetry,
				Reason:   api.ReasonRetryingTransient,
				Detail:   fmt.Sprintf("recovered panic: %v", rec),
				Duration: time.Since(start), At: time.Now(),
			}
		}
	}()

	d := func() api.Decision {
		r.mu.Lock()
		desired, hasDesired := r.desired[key]
		isDeleting := r.deleting[key]
		r.mu.Unlock()

		obs, exists, err := r.store.Get(key)
		if err != nil {
			return api.Decision{ID: key, Attempt: attempt, Action: api.ActionRetry,
				Reason: api.ReasonRetryingTransient,
				Detail: "read observed state: " + err.Error()}
		}

		switch {
		case isDeleting:
			return r.convergeDelete(key, attempt, obs, exists, start)
		case !exists:
			if !hasDesired {
				// Nothing requested, nothing observed: fully idle.
				atomic.AddInt64(&r.stats.Skips, 1)
				return api.Decision{ID: key, Attempt: attempt, Action: api.ActionSkip,
					Reason: api.ReasonIdle, Detail: "no desired and no observed state",
					Duration: time.Since(start), At: time.Now()}
			}
			return r.convergeCreate(key, desired, attempt, start)
		default:
			return r.convergeUpdate(key, desired, hasDesired, obs, attempt, start)
		}
	}()
	if d.At.IsZero() {
		d.At = time.Now()
	}
	if d.Duration == 0 {
		d.Duration = time.Since(start)
	}
	return d
}

// convergeCreate brings a non-existent resource into existence and ensures
// its external effect within the same trigger where possible.
func (r *Reconciler) convergeCreate(key string, desired api.DesiredState, attempt int, start time.Time) api.Decision {
	res := api.Resource{
		ID:          key,
		Generation:  desired.Generation,
		Spec:        desired.Spec,
		SpecHash:    api.SpecHash(desired.Spec),
		Annotations: map[string]string{},
		Conditions:  map[string]api.Condition{},
	}
	if desired.Spec.KeyFieldMissing() {
		setCondition(&res, api.ConditionReady, api.StatusFalse,
			api.ReasonMissingFieldRebuilt,
			"desired state is missing required field image; resource recorded but effect cannot be created")
	} else {
		setCondition(&res, api.ConditionReady, api.StatusUnknown,
			api.ReasonReconciled, "resource created; ensuring external effect")
	}
	saved, err := r.store.Create(res)
	if err != nil {
		if api.IsTransient(err) {
			return api.Decision{ID: key, Attempt: attempt, Action: api.ActionRetry,
				Reason: api.ReasonRetryingTransient, Detail: "create resource: " + err.Error(),
				Duration: time.Since(start), At: time.Now()}
		}
		return api.Decision{ID: key, Attempt: attempt, Action: api.ActionFail,
			Reason: api.ReasonPermanentFailure, Detail: "create resource: " + err.Error(),
			Duration: time.Since(start), At: time.Now()}
	}
	atomic.AddInt64(&r.stats.Writes, 1)

	if desired.Spec.KeyFieldMissing() {
		return api.Decision{ID: key, Attempt: attempt, Action: api.ActionCreate,
			Reason:   api.ReasonMissingFieldRebuilt,
			Detail:   "created resource; effect blocked by missing key field and will not be retried until desired state changes",
			Duration: time.Since(start), At: time.Now()}
	}
	return r.ensureEffect(key, saved, api.ReasonCreated, api.ActionCreate, attempt, start)
}

// convergeUpdate compares observed state with desired state and rebuilds
// when drift, missing key fields, orphans or version skew are detected.
func (r *Reconciler) convergeUpdate(key string, desired api.DesiredState, hasDesired bool, obs api.Resource, attempt int, start time.Time) api.Decision {
	if !hasDesired {
		// Orphan: desired state was never submitted in this process.
		// Do not create external effects; record an explainable condition
		// only when it is new.
		changed := setCondition(&obs, api.ConditionReady, api.StatusFalse,
			api.ReasonPermanentFailure,
			"resource has no accepted desired state in this process; refusing to manage effects")
		if !changed {
			atomic.AddInt64(&r.stats.Skips, 1)
			return api.Decision{ID: key, Attempt: attempt, Action: api.ActionSkip,
				Reason: api.ReasonIdle, Detail: "orphan resource; condition already recorded",
				Duration: time.Since(start), At: time.Now()}
		}
		if _, err := r.commit(key, obs); err != nil {
			return transientDecision(key, attempt, "record orphan condition: "+err.Error(), start)
		}
		return api.Decision{ID: key, Attempt: attempt, Action: api.ActionFail,
			Reason: api.ReasonPermanentFailure, Detail: "recorded orphan-resource condition",
			Duration: time.Since(start), At: time.Now()}
	}

	// Unknown / old-version fields: never dropped, never fatal. Surface
	// them in logs and in a thread-safe observation point so compatibility
	// handling is explainable without racing the log writer.
	if keys := sortedExtraKeys(obs.Extra); len(keys) > 0 {
		r.logger.Printf("compat id=%s preserving unknown/legacy fields: %s", key, strings.Join(keys, ","))
		r.mu.Lock()
		r.compatSeen[key] = append(r.compatSeen[key], keys...)
		r.mu.Unlock()
	}

	driftReason := ""
	switch {
	case obs.Spec.KeyFieldMissing():
		// External actor cleared an essential field on a managed resource.
		driftReason = api.ReasonMissingFieldRebuilt
	case obs.SpecHash == "":
		// Resource written by an old/foreign version: rebuild desired.
		driftReason = api.ReasonDriftRebuilt
	case obs.SpecHash != api.SpecHash(desired.Spec) || obs.Generation != desired.Generation:
		// Spec or generation differs from the latest accepted desire.
		driftReason = api.ReasonDriftRebuilt
	}

	if driftReason != "" {
		detail := fmt.Sprintf("observed generation=%d hash=%s desired generation=%d hash=%s",
			obs.Generation, obs.SpecHash, desired.Generation, api.SpecHash(desired.Spec))
		obs.Generation = desired.Generation
		obs.Spec = desired.Spec
		obs.SpecHash = api.SpecHash(desired.Spec)
		if obs.Annotations == nil {
			obs.Annotations = map[string]string{}
		}
		// Drift invalidates a previous effect reference only when the
		// payload changed; a missing image means no effect can exist yet.
		if desired.Spec.KeyFieldMissing() {
			setCondition(&obs, api.ConditionReady, api.StatusFalse,
				api.ReasonMissingFieldRebuilt, "required field image missing; effect not created")
		} else {
			delete(obs.Annotations, api.AnnEffectRef)
			setCondition(&obs, api.ConditionReady, api.StatusUnknown,
				driftReason, "observed state diverged; desired state rebuilt; re-ensuring effect")
		}
		saved, err := r.commit(key, obs)
		if err != nil {
			return transientDecision(key, attempt, "rebuild drifted state: "+err.Error(), start)
		}
		r.logger.Printf("drift rebuilt id=%s reason=%s %s", key, driftReason, detail)
		if desired.Spec.KeyFieldMissing() {
			return api.Decision{ID: key, Attempt: attempt, Action: api.ActionUpdate,
				Reason: api.ReasonMissingFieldRebuilt, Detail: detail,
				Duration: time.Since(start), At: time.Now()}
		}
		return r.ensureEffect(key, saved, driftReason, api.ActionUpdate, attempt, start)
	}

	// Observed state already encodes the desired payload and the effect
	// reference is anchored: full steady state. Keep the Ready condition
	// reason stable (Reconciled) so re-triggers never cause write churn;
	// the Create/Update distinction lives in the Decision log.
	if ref := obs.Annotations[api.AnnEffectRef]; ref != "" {
		changed := setCondition(&obs, api.ConditionReady, api.StatusTrue,
			api.ReasonReconciled, "observed state and external effect already satisfy desired state")
		if !changed {
			atomic.AddInt64(&r.stats.Skips, 1)
			return api.Decision{ID: key, Attempt: attempt, Action: api.ActionSkip,
				Reason: api.ReasonIdle, Detail: "observed matches desired; write skipped",
				Duration: time.Since(start), At: time.Now()}
		}
		if _, err := r.commit(key, obs); err != nil {
			return transientDecision(key, attempt, "mark ready: "+err.Error(), start)
		}
		return api.Decision{ID: key, Attempt: attempt, Action: api.ActionUpdate,
			Reason: api.ReasonReconciled, Detail: "condition corrected to Ready; no payload write needed",
			Duration: time.Since(start), At: time.Now()}
	}

	// Desired payload present but external effect not yet anchored.
	if desired.Spec.KeyFieldMissing() {
		changed := setCondition(&obs, api.ConditionReady, api.StatusFalse,
			api.ReasonMissingFieldRebuilt, "required field image missing; effect not created")
		if !changed {
			atomic.AddInt64(&r.stats.Skips, 1)
			return api.Decision{ID: key, Attempt: attempt, Action: api.ActionSkip,
				Reason: api.ReasonIdle, Detail: "missing key field already recorded",
				Duration: time.Since(start), At: time.Now()}
		}
		if _, err := r.commit(key, obs); err != nil {
			return transientDecision(key, attempt, "record missing-field condition: "+err.Error(), start)
		}
		return api.Decision{ID: key, Attempt: attempt, Action: api.ActionUpdate,
			Reason: api.ReasonMissingFieldRebuilt, Detail: "recorded missing-key-field condition",
			Duration: time.Since(start), At: time.Now()}
	}
	return r.ensureEffect(key, obs, api.ReasonReconciled, api.ActionUpdate, attempt, start)
}

// ensureEffect drives the idempotent external side effect and anchors its
// reference on the resource. Classification:
//   - transient error  -> ActionRetry with attempt-aware backoff;
//   - permanent error  -> stable condition, ActionFail, no further retry;
//   - success          -> Ready=True, effect reference anchored once.
func (r *Reconciler) ensureEffect(key string, res api.Resource, successReason, successAction string, attempt int, start time.Time) api.Decision {
	ref, err := r.bus.Ensure(key, res.Spec)
	if err != nil {
		if _, permanent := api.AsPermanent(err); permanent {
			setCondition(&res, api.ConditionReady, api.StatusFalse,
				api.ReasonPermanentFailure, err.Error())
			if _, cerr := r.commit(key, res); cerr != nil {
				return transientDecision(key, attempt, "record permanent failure: "+cerr.Error(), start)
			}
			return api.Decision{ID: key, Attempt: attempt, Action: api.ActionFail,
				Reason: api.ReasonPermanentFailure, Detail: err.Error(),
				Duration: time.Since(start), At: time.Now()}
		}
		// Transient: keep a distinguishable condition but keep retrying;
		// the request is retained in the per-key queue.
		setCondition(&res, api.ConditionReady, api.StatusFalse,
			api.ReasonRetryingTransient,
			fmt.Sprintf("attempt %d: %s", attempt, err.Error()))
		if _, cerr := r.commit(key, res); cerr != nil {
			r.logger.Printf("retry id=%s attempt=%d could not persist retry condition: %v", key, attempt, cerr)
		}
		return api.Decision{ID: key, Attempt: attempt, Action: api.ActionRetry,
			Reason: api.ReasonRetryingTransient,
			Detail: fmt.Sprintf("attempt %d transient failure; backing off %s: %s",
				attempt, nextBackoff(r.bo, attempt), err.Error()),
			Duration: time.Since(start), At: time.Now()}
	}

	if res.Annotations == nil {
		res.Annotations = map[string]string{}
	}
	res.Annotations[api.AnnEffectRef] = ref
	if !containsFinalizer(res.Finalizers, api.FinalizerExample) {
		res.Finalizers = append(res.Finalizers, api.FinalizerExample)
	}
	setCondition(&res, api.ConditionReady, api.StatusTrue, api.ReasonReconciled,
		fmt.Sprintf("external effect %s ensured idempotently", ref))
	if _, err := r.commit(key, res); err != nil {
		return transientDecision(key, attempt, "anchor effect reference: "+err.Error(), start)
	}
	return api.Decision{ID: key, Attempt: attempt, Action: successAction,
		Reason: successReason, Detail: "effect ensured idempotently: " + ref,
		Duration: time.Since(start), At: time.Now()}
}

// convergeDelete implements finalizer semantics: the resource stays until
// cleanup succeeds; transient cleanup failures retry with backoff.
func (r *Reconciler) convergeDelete(key string, attempt int, obs api.Resource, exists bool, start time.Time) api.Decision {
	if !exists {
		// External deletion while marked: nothing to release.
		r.mu.Lock()
		delete(r.deleting, key)
		r.mu.Unlock()
		atomic.AddInt64(&r.stats.Skips, 1)
		return api.Decision{ID: key, Attempt: attempt, Action: api.ActionSkip,
			Reason: api.ReasonIdle, Detail: "already absent during deletion",
			Duration: time.Since(start), At: time.Now()}
	}

	now := time.Now()
	if obs.DeletionTimestamp == nil {
		obs.DeletionTimestamp = &now
	}
	readyBefore := obs.Annotations[api.AnnEffectRef] != ""
	hadFinalizer := containsFinalizer(obs.Finalizers, api.FinalizerExample)
	setCondition(&obs, api.ConditionDeleting, api.StatusTrue,
		api.ReasonCleanupInProgress, "deletion requested; waiting for effect cleanup")
	setCondition(&obs, api.ConditionReady, api.StatusFalse,
		api.ReasonCleanupInProgress, "resource is deleting")

	if !hadFinalizer {
		// Never managed an effect (failed-before-ready or external state):
		// safe to remove immediately.
		r.store.Remove(key)
		atomic.AddInt64(&r.stats.Writes, 1)
		r.mu.Lock()
		delete(r.deleting, key)
		r.mu.Unlock()
		reason := api.ReasonDeleted
		if !readyBefore {
			reason = api.ReasonDeletedBeforeReady
		}
		return api.Decision{ID: key, Attempt: attempt, Action: api.ActionDelete,
			Reason: reason, Detail: "no effect finalizer present; removed without cleanup",
			Duration: time.Since(start), At: time.Now()}
	}

	if err := r.bus.Cleanup(key); err != nil {
		if api.IsTransient(err) {
			setCondition(&obs, api.ConditionDeleting, api.StatusFalse,
				api.ReasonCleanupTransient,
				fmt.Sprintf("attempt %d: %s", attempt, err.Error()))
			if _, cerr := r.commit(key, obs); cerr != nil {
				r.logger.Printf("cleanup retry id=%s attempt=%d condition persist failed: %v", key, attempt, cerr)
			}
			return api.Decision{ID: key, Attempt: attempt, Action: api.ActionRetry,
				Reason: api.ReasonCleanupTransient,
				Detail: fmt.Sprintf("attempt %d cleanup transient; resource retained; backing off %s: %s",
					attempt, nextBackoff(r.bo, attempt), err.Error()),
				Duration: time.Since(start), At: time.Now()}
		}
		return api.Decision{ID: key, Attempt: attempt, Action: api.ActionFail,
			Reason: api.ReasonPermanentFailure, Detail: "cleanup: " + err.Error(),
			Duration: time.Since(start), At: time.Now()}
	}

	// Cleanup complete: only now may the resource be removed.
	r.store.Remove(key)
	atomic.AddInt64(&r.stats.Writes, 1)
	r.mu.Lock()
	delete(r.deleting, key)
	r.mu.Unlock()
	return api.Decision{ID: key, Attempt: attempt, Action: api.ActionDelete,
		Reason: api.ReasonDeleted, Detail: "effect cleaned; finalizer released; resource removed",
		Duration: time.Since(start), At: time.Now()}
}

// commit persists mutated observed state with optimistic concurrency.
func (r *Reconciler) commit(key string, res api.Resource) (api.Resource, error) {
	saved, err := r.store.UpdateFunc(key, func(cur *api.Resource) (bool, error) {
		// Preserve unknown/legacy fields even when the external world
		// rewrote the resource between our read and this write.
		if len(cur.Extra) > 0 {
			if res.Extra == nil {
				res.Extra = map[string]any{}
			}
			for k, v := range cur.Extra {
				if _, ok := res.Extra[k]; !ok {
					res.Extra[k] = v
				}
			}
		}
		*cur = res
		return true, nil
	})
	if err != nil {
		return api.Resource{}, err
	}
	atomic.AddInt64(&r.stats.Writes, 1)
	return saved, nil
}

// setCondition installs/updates cond and reports whether it changed.
// Retry-count updates do not count as explainable changes (the Type/Status/
// Reason/Message tuple is what callers distinguish).
func setCondition(res *api.Resource, typ, status, reason, message string) bool {
	if res.Conditions == nil {
		res.Conditions = map[string]api.Condition{}
	}
	cur, ok := res.Conditions[typ]
	next := api.Condition{
		Type: typ, Status: status, Reason: reason, Message: message,
		Count: 1, Updated: time.Now(),
	}
	if ok {
		if cur.Equal(next) {
			// Same explainable tuple: merely re-affirmed, no write churn.
			next.Count = cur.Count + 1
			next.Updated = cur.Updated
			res.Conditions[typ] = next
			return false
		}
		// New tuple -> first observation of this condition.
	}
	res.Conditions[typ] = next
	return true
}

func containsFinalizer(fs []string, want string) bool {
	for _, f := range fs {
		if f == want {
			return true
		}
	}
	return false
}

func sortedExtraKeys(extra map[string]any) []string {
	keys := make([]string, 0, len(extra))
	for k := range extra {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func transientDecision(key string, attempt int, detail string, start time.Time) api.Decision {
	return api.Decision{ID: key, Attempt: attempt, Action: api.ActionRetry,
		Reason: api.ReasonRetryingTransient, Detail: detail,
		Duration: time.Since(start), At: time.Now()}
}
