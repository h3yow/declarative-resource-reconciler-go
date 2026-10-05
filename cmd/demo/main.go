// Command demo runs the reconciler entirely in-process against the
// local simulated store: no cluster, network or credentials required.
package main

import (
	"log"
	"time"

	"github.com/h3yow/declarative-resource-reconciler-go/pkg/api"
	"github.com/h3yow/declarative-resource-reconciler-go/pkg/reconcile"
	"github.com/h3yow/declarative-resource-reconciler-go/pkg/state"
)

func main() {
	logger := log.New(log.Writer(), "demo ", log.LstdFlags|log.Lmicroseconds)
	store := state.NewStore()
	rec := reconcile.New(store, reconcile.DefaultConfig())
	rec.Logf = func(format string, a ...any) { logger.Printf(format, a...) }

	key := "Widget/demo"

	// Step 1: declare desired state.
	logger.Printf("input: create desired %s replicas=3 token=t1", key)
	store.PutDesired(&api.Resource{
		Kind: "Widget", APIVersion: "v1", Name: "demo",
		Spec: api.Spec{Replicas: 3, Image: "app:1", SideEffectToken: "t1"},
	})
	run(logger, rec, store, key, "first reconcile")

	// Step 2: repeat reconciliation — must be a no-op.
	run(logger, rec, store, key, "repeat reconcile (expect no writes/side effects)")

	// Step 3: external drift — someone mutates the live object.
	logger.Printf("input: external mutation sets replicas=9 and a drift tag")
	store.ExternalMutate(key, func(o *state.LiveObject) {
		o.Replicas = 9
		o.ExternalTag = "ops-tuning"
	})
	run(logger, rec, store, key, "after external drift")

	// Step 4: transient failures then heal — two injected flaky writes.
	logger.Printf("input: delete live object externally + inject 2 transient faults")
	store.ExternalMutate(key, func(o *state.LiveObject) { o.Missing = true })
	store.SetFaults(key, state.Faults{TransientTimes: 2})
	for i := 1; ; i++ {
		_, err := rec.ReconcileOnce(key)
		logger.Printf("manual pass %d err=%v", i, err)
		if err == nil {
			break
		}
		if api.ClassOf(err) != api.ClassTransient {
			logger.Fatalf("unexpected permanent error: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	logger.Printf("after transient retries: counters=%+v", store.Counters())

	// Step 5: deletion with cleanup that fails once.
	logger.Printf("input: request deletion with 1 transient cleanup fault")
	store.SetFaults(key, state.Faults{CleanupTimes: 1})
	now := time.Now()
	if res, ok := store.GetDesired(key); ok {
		res.DeletionTimestamp = &now
		store.UpdateDesired(res)
	}
	for i := 1; ; i++ {
		_, err := rec.ReconcileOnce(key)
		logger.Printf("delete pass %d err=%v", i, err)
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, ok := store.GetDesired(key); ok {
		logger.Fatalf("resource still present after cleanup completed")
	}
	logger.Printf("final counters: %+v", store.Counters())

	// Step 6: multi-resource dependency orchestration.
	logger.Printf("--- dependency orchestration ---")
	db, app := "Widget/db", "Widget/app"
	store.PutDesired(&api.Resource{
		Kind: "Widget", APIVersion: "v1", Name: "db",
		Spec: api.Spec{Replicas: 1, Image: "db:1"},
	})
	store.PutDesired(&api.Resource{
		Kind: "Widget", APIVersion: "v1", Name: "app",
		Spec: api.Spec{Replicas: 2, Image: "app:1", DependsOn: []string{db}},
	})
	rec.Start()
	defer rec.Stop()

	logger.Printf("input: trigger dependent %s BEFORE its dependency %s", app, db)
	rec.Trigger(app)
	waitForLog(logger, store, app, api.ReasonDependencyWaiting)
	logger.Printf("input: trigger dependency %s; dependent must auto-continue", db)
	rec.Trigger(db)
	waitForLog(logger, store, db, api.ReasonReady)
	waitForLog(logger, store, app, api.ReasonReady)

	// Step 7: cascade delete — upstream is held until downstream is gone.
	logger.Printf("--- cascade delete ---")
	logger.Printf("input: delete upstream %s while %s still exists", db, app)
	markDeleting(store, db)
	rec.Trigger(db)
	waitForLog(logger, store, db, api.ReasonDeleteWaitingDependents)
	logger.Printf("input: delete downstream %s; upstream must unwind automatically", app)
	markDeleting(store, app)
	rec.Trigger(app)
	deadline := time.Now().Add(2 * time.Second)
	for len(store.ListDesired()) > 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if n := len(store.ListDesired()); n > 0 {
		logger.Fatalf("cascade delete left %d desired records behind", n)
	}
	logger.Printf("cascade delete complete; final counters: %+v", store.Counters())
	logger.Printf("demo finished successfully")
}

func markDeleting(store *state.Store, key string) {
	res, ok := store.GetDesired(key)
	if !ok {
		return
	}
	now := time.Now()
	res.DeletionTimestamp = &now
	store.UpdateDesired(res)
}

func waitForLog(logger *log.Logger, store *state.Store, key string, want api.ConditionReason) {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if res, ok := store.GetDesired(key); ok {
			if c := res.Status.GetCondition(api.ConditionReady); c != nil && c.Reason == want {
				logger.Printf("key=%s reached reason=%s msg=%q", key, c.Reason, c.Message)
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	logger.Fatalf("key=%s never reached reason=%s", key, want)
}

func run(logger *log.Logger, rec *reconcile.Reconciler, store *state.Store, key, label string) {
	decision, err := rec.ReconcileOnce(key)
	res, _ := store.GetDesired(key)
	cond := res.Status.GetCondition(api.ConditionReady)
	logger.Printf("%s:\n  decision=%s err=%v\n  phase=%s reason=%s live={replicas:%d image:%q} counters=%+v",
		label, decision, err, res.Status.Phase, safeReason(cond),
		res.Status.LiveReplicas, res.Status.LiveImage, store.Counters())
}

func safeReason(c *api.Condition) api.ConditionReason {
	if c == nil {
		return "<none>"
	}
	return c.Reason
}
