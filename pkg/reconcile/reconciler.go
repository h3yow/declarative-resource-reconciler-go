package reconcile

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/h3yow/declarative-resource-reconciler-go/pkg/api"
	"github.com/h3yow/declarative-resource-reconciler-go/pkg/state"
)

// Config tunes the reconciler loop.
type Config struct {
	Workers       int
	BaseBackoff   time.Duration
	MaxBackoff    time.Duration
	MaxRetries    int
	FinalizerName string
}

// DefaultConfig returns sensible local defaults (millisecond-scale
// backoff so the whole thing is verifiable without waiting).
func DefaultConfig() Config {
	return Config{
		Workers:       4,
		BaseBackoff:   10 * time.Millisecond,
		MaxBackoff:    200 * time.Millisecond,
		MaxRetries:    5,
		FinalizerName: "drr.finalizer/cleanup",
	}
}

const (
	conditionTrue  = "True"
	conditionFalse = "False"

	defaultReplicas = 1
	defaultImage    = "default/image:latest"
	// legacyReplicasField is a field emitted by old API versions; it is
	// migrated instead of crashing or being ignored.
	legacyReplicasField = "replicas_v1"
)

// Reconciler runs convergence over a state.Store.
type Reconciler struct {
	store  *state.Store
	cfg    Config
	queue  *KeyedQueue
	stopCh chan struct{}
	wg     sync.WaitGroup

	stopOnce sync.Once

	// Logf, when set, receives one line per reconciliation decision.
	Logf func(format string, args ...any)
}

// New constructs a reconciler.
func New(store *state.Store, cfg Config) *Reconciler {
	if cfg.Workers <= 0 {
		cfg = DefaultConfig()
	}
	if cfg.FinalizerName == "" {
		cfg.FinalizerName = DefaultConfig().FinalizerName
	}
	if cfg.BaseBackoff <= 0 {
		cfg.BaseBackoff = DefaultConfig().BaseBackoff
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = DefaultConfig().MaxBackoff
	}
	return &Reconciler{
		store:  store,
		cfg:    cfg,
		queue:  NewKeyedQueue(),
		stopCh: make(chan struct{}),
	}
}

// Trigger requests reconciliation of a resource; safe for concurrent use.
// It never blocks: requests are merged in the keyed queue, never dropped.
func (r *Reconciler) Trigger(key string) { r.queue.Trigger(key) }

// Start launches worker goroutines.
func (r *Reconciler) Start() {
	for i := 0; i < r.cfg.Workers; i++ {
		r.wg.Add(1)
		go r.worker(i)
	}
}

// Stop cancels pending backoff retries, closes the queue and waits for
// in-flight work to finish. Safe to call multiple times.
func (r *Reconciler) Stop() {
	r.stopOnce.Do(func() { close(r.stopCh) })
	r.queue.Close()
	r.wg.Wait()
}

func (r *Reconciler) logf(format string, args ...any) {
	if r.Logf != nil {
		r.Logf(format, args...)
	}
}

// worker is the per-key-serialized processing loop. Serialization itself
// is guaranteed by the queue: a key lives in the "work" set until Done,
// so a second worker can never take the same key concurrently.
func (r *Reconciler) worker(id int) {
	defer r.wg.Done()
	for {
		key, attempts, ok := r.queue.Get()
		if !ok {
			return
		}
		decision, err := r.process(key, attempts)
		r.logf("worker=%d key=%s attempt=%d decision=%q err=%v",
			id, key, attempts, decision, err)

		switch api.ClassOf(err) {
		case api.ClassTransient:
			if attempts >= r.cfg.MaxRetries {
				// Give up on endless transient failure: turn it into a
				// stable, distinguishable permanent condition.
				r.markRetriesExhausted(key, attempts)
				r.queue.Done(key, attempts, false)
				continue
			}
			delay := backoff(r.cfg, attempts+1)
			r.logf("worker=%d key=%s transient retry #%d after %s",
				id, key, attempts+1, delay)
			// Schedule the retry in a tracked goroutine that Stop() can
			// cancel, so shutdown never returns while a requeue is pending
			// and a pending requeue never touches a closed queue.
			r.wg.Add(1)
			go func(k string, a int) {
				defer r.wg.Done()
				timer := time.NewTimer(delay)
				defer timer.Stop()
				select {
				case <-timer.C:
					r.queue.Done(k, a, true)
				case <-r.stopCh:
					// Cancelled shutdown: settle the in-flight item
					// without requeue so the queue cannot deadlock.
					r.queue.Done(k, a, false)
				}
			}(key, attempts)
		default:
			r.queue.Done(key, attempts, false)
		}
	}
}

// process performs one convergence pass.
func (r *Reconciler) process(key string, attempts int) (string, error) {
	res, ok := r.store.GetDesired(key)
	if !ok {
		return "desired resource gone; nothing to reconcile", nil
	}
	if res.DeletionTimestamp != nil {
		return r.reconcileDelete(res, attempts)
	}
	return r.reconcileDesired(res, attempts)
}

// ReconcileOnce performs one deterministic convergence pass (no queue).
// Exported for tests that want to assert single-pass behavior.
func (r *Reconciler) ReconcileOnce(key string) (string, error) {
	return r.process(key, 0)
}

// markRetriesExhausted records the permanent terminal condition.
func (r *Reconciler) markRetriesExhausted(key string, attempts int) {
	res, ok := r.store.GetDesired(key)
	if !ok {
		return
	}
	res.Status.Phase = api.PhaseError
	res.Status.SetCondition(api.Condition{
		Type:      api.ConditionReady,
		Status:    conditionFalse,
		Reason:    api.ReasonPermanentFailure,
		Message:   fmt.Sprintf("transient retries exhausted after %d attempts", attempts+1),
		Retries:   attempts + 1,
		UpdatedAt: time.Now(),
	})
	r.store.UpdateDesired(res)
}

// ensureFinalizer appends the cleanup finalizer once.
func (r *Reconciler) ensureFinalizer(res *api.Resource) bool {
	for _, f := range res.Finalizers {
		if f == r.cfg.FinalizerName {
			return false
		}
	}
	res.Finalizers = append(res.Finalizers, r.cfg.FinalizerName)
	return true
}

// reconcileDesired converges create/update and applies idempotent skips.
func (r *Reconciler) reconcileDesired(res *api.Resource, attempts int) (string, error) {
	prevStatus := res.Status

	// 1) Finalizer first: deletion must never race a successful create.
	finalizerAdded := r.ensureFinalizer(res)

	// 2) Compatibility: migrate legacy fields, rebuild missing fields,
	// preserve unknown fields. Nothing here crashes or is silently dropped.
	reason, msg, compatChanged := r.applyCompatibility(res)

	// Persist metadata/compat mutations (not an external side effect).
	if finalizerAdded || compatChanged {
		r.store.UpdateDesired(res)
	}

	// 3) Compute the desired live object.
	want := &state.LiveObject{Replicas: res.Spec.Replicas, Image: res.Spec.Image}

	live, exists := r.store.GetLive(res.Key())

	// 4) Converge the live object.
	var writeErr error
	var actionMsg string
	switch {
	case !exists:
		writeErr = r.store.ApplyWrite(res.Key(), want)
		if writeErr != nil {
			return r.failStatus(res, prevStatus, attempts, reason,
				"live object missing; recreate attempted but write failed: "+writeErr.Error(), writeErr)
		}
		r.recordObserve(res, want)
		if res.Status.ObservedGen == 0 {
			// Never converged before: ordinary initial creation, not drift.
			actionMsg = "managed object created from desired state"
			if reason == "" {
				reason = api.ReasonReady
			}
		} else {
			actionMsg = "managed object had been externally deleted; recreated from desired state"
			if reason == "" {
				reason = api.ReasonRecreateAfterDrift
			}
		}
	case live.Replicas != want.Replicas || live.Image != want.Image || live.ExternalTag != "":
		writeErr = r.store.ApplyWrite(res.Key(), want)
		if writeErr != nil {
			return r.failStatus(res, prevStatus, attempts, reason,
				"drift detected but corrective write failed: "+writeErr.Error(), writeErr)
		}
		r.recordObserve(res, want)
		if live.ExternalTag != "" {
			actionMsg = "external mutation tag=" + live.ExternalTag + " observed; overwritten with desired state"
		} else {
			actionMsg = fmt.Sprintf("live drift observed {replicas:%d image:%q} -> desired {replicas:%d image:%q}",
				live.Replicas, live.Image, want.Replicas, want.Image)
		}
		if reason == "" {
			reason = api.ReasonDriftCorrected
		}
	default:
		// Observed already satisfies desired: no write at all.
		r.recordObserve(res, live)
		actionMsg = "observed state already equals desired state; write skipped (idempotent)"
		if reason == "" {
			reason = api.ReasonReady
		}
	}
	if writeErr != nil {
		return "", writeErr
	}
	if msg != "" {
		msg = msg + "; " + actionMsg
	} else {
		msg = actionMsg
	}

	// 5) External side effect: at most once per token value.
	if res.Spec.SideEffectToken != "" && res.Spec.SideEffectToken != res.Status.AppliedToken {
		r.store.CallSideEffect(res.Key(), res.Spec.SideEffectToken)
		res.Status.AppliedToken = res.Spec.SideEffectToken
		msg += "; side effect executed for token " + res.Spec.SideEffectToken
	}

	// 6) Success status; persist only if something actually changed.
	res.Status.Phase = api.PhaseReady
	res.Status.ObservedGen = res.Generation
	res.Status.ReconciledAt = time.Now()
	res.Status.SetCondition(api.Condition{
		Type:      api.ConditionReady,
		Status:    conditionTrue,
		Reason:    reason,
		Message:   msg,
		Retries:   0,
		UpdatedAt: time.Now(),
	})
	if !reflect.DeepEqual(prevStatus, res.Status) {
		r.store.UpdateDesired(res)
	}
	return "converged: " + msg, nil
}

// recordObserve mirrors the live object into observed status fields.
func (r *Reconciler) recordObserve(res *api.Resource, live *state.LiveObject) {
	res.Status.LiveReplicas = live.Replicas
	res.Status.LiveImage = live.Image
}

// failStatus records a failure condition while preserving the transient/
// permanent distinction, then returns the error for queue classification.
func (r *Reconciler) failStatus(res *api.Resource, prev api.Status, attempts int,
	baseReason api.ConditionReason, msg string, err error) (string, error) {
	res.Status.LiveReplicas = 0
	switch api.ClassOf(err) {
	case api.ClassPermanent:
		res.Status.Phase = api.PhaseError
		res.Status.SetCondition(api.Condition{
			Type: api.ConditionReady, Status: conditionFalse,
			Reason: api.ReasonPermanentFailure, Message: msg,
			Retries: attempts, UpdatedAt: time.Now(),
		})
	default:
		res.Status.Phase = api.PhaseProgressing
		res.Status.SetCondition(api.Condition{
			Type: api.ConditionReady, Status: conditionFalse,
			Reason: api.ReasonTransientFailure, Message: msg,
			Retries: attempts + 1, UpdatedAt: time.Now(),
		})
	}
	if !reflect.DeepEqual(prev, res.Status) {
		r.store.UpdateDesired(res)
	}
	return "failure: " + msg, err
}

// applyCompatibility handles legacy migration, missing-field rebuild and
// unknown-field preservation. Returns the stable reason/message (if any)
// and whether desired state was mutated.
func (r *Reconciler) applyCompatibility(res *api.Resource) (api.ConditionReason, string, bool) {
	var reason api.ConditionReason
	var msgs []string
	changed := false

	// Legacy field: old versions wrote replicas under replicas_v1.
	if raw, ok := res.Unknown[legacyReplicasField]; ok {
		if n, ok := numberToInt(raw); ok {
			if res.Spec.Replicas == 0 {
				res.Spec.Replicas = n
				changed = true
				msgs = append(msgs, fmt.Sprintf("legacy field %s=%d migrated to spec.replicas", legacyReplicasField, n))
			} else {
				msgs = append(msgs, fmt.Sprintf("legacy field %s present but spec.replicas already set; preserved as-is", legacyReplicasField))
			}
			if reason == "" {
				reason = api.ReasonLegacyFieldMigrated
			}
		}
	}

	// Missing required fields: rebuild desired state with safe defaults.
	if res.Spec.Replicas <= 0 {
		res.Spec.Replicas = defaultReplicas
		msgs = append(msgs, "missing/invalid spec.replicas rebuilt to default 1")
		if reason == "" {
			reason = api.ReasonMissingFieldRebuilt
		}
		changed = true
	}
	if res.Spec.Image == "" {
		res.Spec.Image = defaultImage
		msgs = append(msgs, "missing spec.image rebuilt to default "+defaultImage)
		if reason == "" {
			reason = api.ReasonMissingFieldRebuilt
		}
		changed = true
	}

	// Unknown fields: never crash, never silently drop.
	if len(res.Unknown) > 0 {
		keys := make([]string, 0, len(res.Unknown))
		for k := range res.Unknown {
			keys = append(keys, k)
		}
		if res.Status.UnknownFields == nil {
			res.Status.UnknownFields = map[string]string{}
		}
		for _, k := range keys {
			res.Status.UnknownFields[k] = "preserved"
		}
		msgs = append(msgs, fmt.Sprintf("unknown fields %v preserved (forward compatible)", keys))
		if reason == "" {
			reason = api.ReasonUnknownKept
		}
	}
	return reason, joinMsgs(msgs), changed
}

func joinMsgs(msgs []string) string {
	out := ""
	for i, m := range msgs {
		if i > 0 {
			out += "; "
		}
		out += m
	}
	return out
}

// numberToInt accepts json.Number / float64 / int produced by loose JSON.
func numberToInt(v any) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case float64:
		return int(n), true
	case json.Number:
		i, err := n.Int64()
		if err != nil {
			return 0, false
		}
		return int(i), true
	default:
		return 0, false
	}
}

// reconcileDelete keeps the resource until cleanup is done.
func (r *Reconciler) reconcileDelete(res *api.Resource, attempts int) (string, error) {
	hasFinalizer := false
	for _, f := range res.Finalizers {
		if f == r.cfg.FinalizerName {
			hasFinalizer = true
		}
	}

	if !hasFinalizer {
		r.store.DeleteDesired(res.Key())
		return "no finalizer; desired record removed", nil
	}

	if err := r.store.Cleanup(res.Key()); err != nil {
		// Cleanup unfinished: keep resource + finalizer, retry with backoff.
		prev := res.Status
		res.Status.Phase = api.PhaseDeleting
		res.Status.SetCondition(api.Condition{
			Type: api.ConditionReady, Status: conditionFalse,
			Reason:  api.ReasonDeleteCleanupWaiting,
			Message: "cleanup not finished, resource retained: " + err.Error(),
			Retries: attempts + 1, UpdatedAt: time.Now(),
		})
		if !reflect.DeepEqual(prev, res.Status) {
			r.store.UpdateDesired(res)
		}
		return "cleanup blocked; finalizer retained; will retry", err
	}

	// Cleanup complete: strip finalizer, then remove the record.
	res.Finalizers = nil
	res.Status.Phase = ""
	res.Status.SetCondition(api.Condition{
		Type: api.ConditionReady, Status: conditionTrue,
		Reason: api.ReasonCleanupComplete, Message: "external resources released",
		UpdatedAt: time.Now(),
	})
	r.store.DeleteDesired(res.Key())
	return "cleanup complete; finalizer removed and resource deleted", nil
}

// backoff returns exponential delay for 1-based attempt n.
func backoff(cfg Config, n int) time.Duration {
	d := cfg.BaseBackoff
	for i := 1; i < n; i++ {
		d *= 2
		if d >= cfg.MaxBackoff {
			return cfg.MaxBackoff
		}
	}
	if d > cfg.MaxBackoff {
		return cfg.MaxBackoff
	}
	return d
}
