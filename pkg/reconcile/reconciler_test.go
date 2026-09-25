package reconcile

import (
	"sync"
	"testing"
	"time"

	"github.com/h3yow/declarative-resource-reconciler-go/pkg/api"
	"github.com/h3yow/declarative-resource-reconciler-go/pkg/state"
)

func testConfig() Config {
	c := DefaultConfig()
	c.Workers = 4
	c.BaseBackoff = 5 * time.Millisecond
	c.MaxBackoff = 20 * time.Millisecond
	return c
}

func newResource(name string) *api.Resource {
	return &api.Resource{
		Kind: "Widget", APIVersion: "v1", Name: name,
		Spec: api.Spec{Replicas: 3, Image: "app:1", SideEffectToken: "tok-" + name},
	}
}

func waitFor(t *testing.T, timeout time.Duration, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}

func condReason(t *testing.T, st *state.Store, key string) api.ConditionReason {
	t.Helper()
	res, ok := st.GetDesired(key)
	if !ok {
		return "<deleted>"
	}
	c := res.Status.GetCondition(api.ConditionReady)
	if c == nil {
		return "<none>"
	}
	t.Logf("condition key=%s status=%s reason=%s retries=%d msg=%q",
		key, c.Status, c.Reason, c.Retries, c.Message)
	return c.Reason
}

// TestIdempotentConvergence asserts repeated reconciliation creates no
// duplicate resources and no duplicate side effects, and that a second
// side-effect token re-runs exactly once while the same token is skipped.
func TestIdempotentConvergence(t *testing.T) {
	st := state.NewStore()
	r := New(st, testConfig())
	key := "Widget/a"
	st.PutDesired(newResource("a"))

	d1, err := r.ReconcileOnce(key)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("trigger=%s pass=1 decision=%q", key, d1)
	for i := 2; i <= 5; i++ {
		d, err := r.ReconcileOnce(key)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("trigger=%s pass=%d decision=%q (basis: observed==desired)", key, i, d)
	}
	c := st.Counters()
	if c.Creates != 1 || c.Updates != 0 || c.SideCalls != 1 {
		t.Fatalf("after repeats counters=%+v, want Creates=1 Updates=0 SideCalls=1", c)
	}
	res, _ := st.GetDesired(key)
	if got := condReason(t, st, key); got != api.ReasonReady {
		t.Fatalf("reason=%s, want Ready", got)
	}

	// New token -> side effect re-runs once; identical repeat -> no-op.
	res.Spec.SideEffectToken = "tok-a-2"
	st.UpdateDesired(res)
	if _, err := r.ReconcileOnce(key); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReconcileOnce(key); err != nil {
		t.Fatal(err)
	}
	if st.Counters().SideCalls != 2 {
		t.Fatalf("SideCalls=%d, want 2 after new token", st.Counters().SideCalls)
	}
}

// TestConcurrentTriggers: 100 goroutines trigger the same resource; the
// per-key serialization must converge to exactly one create/side effect.
func TestConcurrentTriggers(t *testing.T) {
	st := state.NewStore()
	cfg := testConfig()
	r := New(st, cfg)
	r.Logf = func(format string, a ...any) { t.Logf(format, a...) }
	key := "Widget/c"
	st.PutDesired(newResource("c"))
	r.Start()
	defer r.Stop()

	t.Logf("trigger input: 100 concurrent Trigger(%s) from 100 goroutines", key)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); r.Trigger(key) }()
	}
	wg.Wait()

	waitFor(t, time.Second, func() bool {
		res, ok := st.GetDesired(key)
		return ok && res.Status.Phase == api.PhaseReady
	})
	// Give any accidental extra passes time to surface.
	time.Sleep(50 * time.Millisecond)
	c := st.Counters()
	t.Logf("final counters=%+v reason=%s", c, condReason(t, st, key))
	if c.Creates != 1 || c.SideCalls != 1 {
		t.Fatalf("concurrent triggers produced duplicate effects: %+v", c)
	}
}

// TestTransientFailureRetried: injected flaky writes must be retried with
// backoff and converge; the request is never dropped.
func TestTransientFailureRetried(t *testing.T) {
	st := state.NewStore()
	cfg := testConfig()
	r := New(st, cfg)
	r.Logf = func(format string, a ...any) { t.Logf(format, a...) }
	key := "Widget/t"
	st.PutDesired(newResource("t"))
	st.SetFaults(key, state.Faults{TransientTimes: 3})
	r.Start()
	defer r.Stop()

	t.Logf("trigger input=%s with TransientTimes=3; expect retry #1..#3 then Ready", key)
	r.Trigger(key)
	waitFor(t, 2*time.Second, func() bool {
		res, ok := st.GetDesired(key)
		return ok && res.Status.Phase == api.PhaseReady
	})
	c := st.Counters()
	t.Logf("retry result: counters=%+v", c)
	if c.Creates != 1 {
		t.Fatalf("Creates=%d, want exactly 1 successful create after retries", c.Creates)
	}
	res, _ := st.GetDesired(key)
	if cond := res.Status.GetCondition(api.ConditionReady); cond.Reason != api.ReasonReady {
		t.Fatalf("final reason=%s, want Ready", cond.Reason)
	}
}

// TestPermanentFailureStops: a permanent failure records a stable,
// distinguishable condition and no retries are attempted.
func TestPermanentFailureStops(t *testing.T) {
	st := state.NewStore()
	cfg := testConfig()
	r := New(st, cfg)
	r.Logf = func(format string, a ...any) { t.Logf(format, a...) }
	key := "Widget/p"
	st.PutDesired(newResource("p"))
	st.SetFaults(key, state.Faults{Permanent: true})
	r.Start()
	defer r.Stop()

	t.Logf("trigger input=%s with permanent fault; expect stop with PermanentFailure", key)
	r.Trigger(key)
	waitFor(t, time.Second, func() bool {
		res, ok := st.GetDesired(key)
		return ok && res.Status.Phase == api.PhaseError
	})
	time.Sleep(50 * time.Millisecond)
	c1 := st.Counters()
	time.Sleep(50 * time.Millisecond)
	c2 := st.Counters()
	if c1 != c2 {
		t.Fatalf("permanent failure kept retrying: %+v then %+v", c1, c2)
	}
	if got := condReason(t, st, key); got != api.ReasonPermanentFailure {
		t.Fatalf("reason=%s, want PermanentFailure", got)
	}
	if c1.Creates != 0 {
		t.Fatalf("permanent failure must not create resources: %+v", c1)
	}
}

// TestRetriesExhaustedBecomesPermanent: transient failures that never heal
// eventually become a stable permanent condition instead of looping.
func TestRetriesExhaustedBecomesPermanent(t *testing.T) {
	st := state.NewStore()
	cfg := testConfig()
	cfg.MaxRetries = 2
	r := New(st, cfg)
	r.Logf = func(format string, a ...any) { t.Logf(format, a...) }
	key := "Widget/x"
	st.PutDesired(newResource("x"))
	st.SetFaults(key, state.Faults{TransientTimes: 1000})
	r.Start()
	defer r.Stop()

	t.Logf("trigger input=%s with endless transient faults, MaxRetries=2", key)
	r.Trigger(key)
	waitFor(t, 2*time.Second, func() bool {
		res, ok := st.GetDesired(key)
		return ok && res.Status.Phase == api.PhaseError
	})
	res, _ := st.GetDesired(key)
	c := res.Status.GetCondition(api.ConditionReady)
	if c.Reason != api.ReasonPermanentFailure || c.Retries < 3 {
		t.Fatalf("condition reason=%s retries=%d, want PermanentFailure retries>=3",
			c.Reason, c.Retries)
	}
	t.Logf("decision basis: %s (retries observed=%d)", c.Message, c.Retries)
}

// TestDeletionWaitsForCleanup: the resource and finalizer must remain
// while cleanup is failing; once cleanup succeeds the record is removed.
func TestDeletionWaitsForCleanup(t *testing.T) {
	st := state.NewStore()
	r := New(st, testConfig())
	key := "Widget/d"
	st.PutDesired(newResource("d"))
	if _, err := r.ReconcileOnce(key); err != nil {
		t.Fatal(err)
	}

	st.SetFaults(key, state.Faults{CleanupTimes: 2})
	now := time.Now()
	res, _ := st.GetDesired(key)
	res.DeletionTimestamp = &now
	st.UpdateDesired(res)
	t.Logf("trigger input=delete(%s) with CleanupTimes=2", key)

	for i := 1; i <= 2; i++ {
		_, err := r.ReconcileOnce(key)
		if api.ClassOf(err) != api.ClassTransient {
			t.Fatalf("cleanup pass %d: err=%v, want transient", i, err)
		}
		if _, ok := st.GetDesired(key); !ok {
			t.Fatalf("resource removed before cleanup finished on pass %d", i)
		}
		if got := condReason(t, st, key); got != api.ReasonDeleteCleanupWaiting {
			t.Fatalf("reason during cleanup=%s, want DeleteCleanupWaiting", got)
		}
		t.Logf("retry %d: cleanup unfinished; finalizer retained", i)
	}
	if _, err := r.ReconcileOnce(key); err != nil {
		t.Fatal(err)
	}
	if _, ok := st.GetDesired(key); ok {
		t.Fatal("resource still present after cleanup completed")
	}
	if c := st.Counters(); c.Deletes != 1 {
		t.Fatalf("Deletes=%d, want 1", c.Deletes)
	}
	t.Logf("decision basis: CleanupComplete then removal; counters=%+v", st.Counters())
}

// TestExternalDriftAndRecreation covers both out-of-band mutation and
// out-of-band deletion, asserting distinct reasons and no duplicates.
func TestExternalDriftAndRecreation(t *testing.T) {
	st := state.NewStore()
	r := New(st, testConfig())
	key := "Widget/g"
	st.PutDesired(newResource("g"))
	if _, err := r.ReconcileOnce(key); err != nil {
		t.Fatal(err)
	}

	// External mutation.
	st.ExternalMutate(key, func(o *state.LiveObject) {
		o.Replicas = 9
		o.ExternalTag = "ops-tuning"
	})
	d, err := r.ReconcileOnce(key)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("trigger=%s after external mutation: decision=%q", key, d)
	if got := condReason(t, st, key); got != api.ReasonDriftCorrected {
		t.Fatalf("reason=%s, want DriftCorrected", got)
	}

	// External deletion -> recreate.
	st.ExternalMutate(key, func(o *state.LiveObject) { o.Missing = true })
	d, err = r.ReconcileOnce(key)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("trigger=%s after external deletion: decision=%q", key, d)
	if got := condReason(t, st, key); got != api.ReasonRecreateAfterDrift {
		t.Fatalf("reason=%s, want RecreatedAfterDrift", got)
	}
	c := st.Counters()
	if c.Creates != 2 {
		t.Fatalf("Creates=%d, want 2 (initial + recreate)", c.Creates)
	}
}

// TestStopDuringBackoffReturns: starting a never-healing transient
// reconciliation then stopping during the backoff wait must return
// promptly without deadlock or a goroutine touching a closed queue.
func TestStopDuringBackoffReturns(t *testing.T) {
	st := state.NewStore()
	cfg := testConfig()
	cfg.BaseBackoff = 5 * time.Second // backoff far longer than the test
	cfg.MaxBackoff = 10 * time.Second
	r := New(st, cfg)
	key := "Widget/s"
	st.PutDesired(newResource("s"))
	st.SetFaults(key, state.Faults{TransientTimes: 1000})
	r.Start()
	r.Trigger(key)
	time.Sleep(30 * time.Millisecond) // let the first attempt enter backoff

	done := make(chan struct{})
	go func() { r.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Stop deadlocked while a backoff retry was pending")
	}
}

// TestDistinctKeysProcessedInParallel: a slow/failing key must not block
// a healthy key; both keys converge with independent side-effect counts.
func TestDistinctKeysProcessedInParallel(t *testing.T) {
	st := state.NewStore()
	cfg := testConfig()
	cfg.Workers = 4
	r := New(st, cfg)
	st.PutDesired(newResource("k1"))
	st.PutDesired(newResource("k2"))
	st.SetFaults("Widget/k1", state.Faults{TransientTimes: 50}) // stays failing
	r.Start()
	defer r.Stop()

	r.Trigger("Widget/k1")
	r.Trigger("Widget/k2")
	waitFor(t, time.Second, func() bool {
		res, ok := st.GetDesired("Widget/k2")
		return ok && res.Status.Phase == api.PhaseReady
	})
	c := st.Counters()
	if c.Creates != 1 {
		t.Fatalf("healthy key blocked by failing key: counters=%+v", c)
	}
	t.Logf("independent keys: k2 Ready while k1 retries; counters=%+v", c)
}
