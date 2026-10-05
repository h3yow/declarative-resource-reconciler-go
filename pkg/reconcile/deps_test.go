package reconcile

import (
	"sync"
	"testing"
	"time"

	"github.com/h3yow/declarative-resource-reconciler-go/pkg/api"
	"github.com/h3yow/declarative-resource-reconciler-go/pkg/state"
)

func depResource(name string, deps ...string) *api.Resource {
	r := newResource(name)
	r.Spec.DependsOn = deps
	return r
}

func liveExists(st *state.Store, key string) bool {
	_, ok := st.GetLive(key)
	return ok
}

// TestDependencyOrderUnderConcurrentTriggers: the dependent is triggered
// many times, concurrently and BEFORE its dependency is ever triggered.
// It must never land early; once the dependency converges it proceeds
// automatically, and everything is created exactly once.
func TestDependencyOrderUnderConcurrentTriggers(t *testing.T) {
	st := state.NewStore()
	r := New(st, testConfig())
	r.Logf = func(format string, a ...any) { t.Logf(format, a...) }
	up, down := "Widget/up", "Widget/down"
	st.PutDesired(newResource("up"))
	st.PutDesired(depResource("down", up))
	r.Start()
	defer r.Stop()

	// 50 concurrent triggers of the dependent while the dependency has
	// never been triggered (not Ready, maybe never will be).
	t.Logf("trigger input: 50x concurrent Trigger(%s) before %s is ready", down, up)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); r.Trigger(down) }()
	}
	wg.Wait()

	waitFor(t, time.Second, func() bool {
		res, ok := st.GetDesired(down)
		if !ok {
			return false
		}
		c := res.Status.GetCondition(api.ConditionReady)
		return c != nil && c.Reason == api.ReasonDependencyWaiting
	})
	t.Logf("dependent held: reason=%s (basis: dependency %s not ready)",
		condReason(t, st, down), up)
	if liveExists(st, down) {
		t.Fatal("dependent landed before its dependency was ready")
	}
	if c := st.Counters(); c.Creates != 0 {
		t.Fatalf("dependent produced writes while waiting: %+v", c)
	}

	// Now converge the dependency: the dependent must continue WITHOUT
	// any further manual trigger.
	t.Logf("trigger input: Trigger(%s); expect %s to auto-continue afterwards", up, down)
	r.Trigger(up)
	waitFor(t, 2*time.Second, func() bool {
		res, ok := st.GetDesired(down)
		return ok && res.Status.Phase == api.PhaseReady
	})
	t.Logf("dependency ready; dependent auto-continued: down reason=%s up reason=%s",
		condReason(t, st, down), condReason(t, st, up))

	// Re-trigger both many times: nothing may be duplicated.
	for i := 0; i < 20; i++ {
		r.Trigger(up)
		r.Trigger(down)
	}
	time.Sleep(100 * time.Millisecond)
	c := st.Counters()
	t.Logf("final counters=%+v (basis: each resource created once, no duplicate side effects)", c)
	if c.Creates != 2 || c.SideCalls != 2 {
		t.Fatalf("counters=%+v, want Creates=2 SideCalls=2", c)
	}
}

// TestDependencyRecoveryAfterTransientFault: a dependency stuck on
// transient failures holds the dependent in a distinguishable waiting
// state with no hot retry loop; when the dependency heals, the dependent
// continues on its own and coalesced triggers are not lost.
func TestDependencyRecoveryAfterTransientFault(t *testing.T) {
	st := state.NewStore()
	cfg := testConfig()
	cfg.BaseBackoff = 30 * time.Millisecond
	cfg.MaxBackoff = 60 * time.Millisecond
	cfg.MaxRetries = 20
	r := New(st, cfg)
	r.Logf = func(format string, a ...any) { t.Logf(format, a...) }
	up, down := "Widget/up", "Widget/down"
	st.PutDesired(newResource("up"))
	st.PutDesired(depResource("down", up))
	st.SetFaults(up, state.Faults{TransientTimes: 1000}) // stuck for now
	r.Start()
	defer r.Stop()

	t.Logf("trigger input: Trigger(%s) + Trigger(%s); %s has stuck transient faults", up, down, up)
	r.Trigger(up)
	r.Trigger(down)

	waitFor(t, 2*time.Second, func() bool {
		res, ok := st.GetDesired(down)
		if !ok {
			return false
		}
		c := res.Status.GetCondition(api.ConditionReady)
		return c != nil && c.Reason == api.ReasonDependencyWaiting
	})
	t.Logf("dependent waiting on faulty dependency: reason=%s (distinct from own TransientFailure)",
		condReason(t, st, down))

	// While the dependency is stuck the dependent must not spin: no
	// writes, and its conclusion stays stable. Extra triggers coalesce.
	c1 := st.Counters()
	for i := 0; i < 10; i++ {
		r.Trigger(down)
	}
	time.Sleep(120 * time.Millisecond)
	c2 := st.Counters()
	t.Logf("while dependency stuck: counters %+v -> %+v", c1, c2)
	if c2.Creates != 0 || c2.SideCalls != 0 {
		t.Fatalf("dependent did work while dependency unready: %+v", c2)
	}
	if liveExists(st, down) {
		t.Fatal("dependent landed while dependency was failing")
	}

	// Heal the dependency. No manual re-trigger of the dependent: the
	// fan-out from the dependency's recovery must wake it.
	t.Logf("input: clear faults on %s; expect %s to resume without manual trigger", up, down)
	st.SetFaults(up, state.Faults{})
	waitFor(t, 2*time.Second, func() bool {
		res, ok := st.GetDesired(down)
		return ok && res.Status.Phase == api.PhaseReady
	})
	t.Logf("recovered: up reason=%s down reason=%s",
		condReason(t, st, up), condReason(t, st, down))
	c := st.Counters()
	if c.Creates != 2 || c.SideCalls != 2 {
		t.Fatalf("after recovery counters=%+v, want Creates=2 SideCalls=2 (merged triggers not lost, not duplicated)", c)
	}
}

// TestDependencyPermanentFailure: a terminally failed dependency yields
// a stable, distinguishable conclusion on the dependent and retries stop.
func TestDependencyPermanentFailure(t *testing.T) {
	st := state.NewStore()
	r := New(st, testConfig())
	r.Logf = func(format string, a ...any) { t.Logf(format, a...) }
	up, down := "Widget/up", "Widget/down"
	st.PutDesired(newResource("up"))
	st.PutDesired(depResource("down", up))
	st.SetFaults(up, state.Faults{Permanent: true})
	r.Start()
	defer r.Stop()

	t.Logf("trigger input: Trigger(%s)+Trigger(%s); %s permanently failing", up, down, up)
	r.Trigger(up)
	r.Trigger(down)
	waitFor(t, 2*time.Second, func() bool {
		res, ok := st.GetDesired(down)
		if !ok {
			return false
		}
		c := res.Status.GetCondition(api.ConditionReady)
		return c != nil && c.Reason == api.ReasonDependencyFailed
	})
	t.Logf("dependent conclusion: reason=%s (up reason=%s)",
		condReason(t, st, down), condReason(t, st, up))

	res, _ := st.GetDesired(down)
	if res.Status.Phase != api.PhaseError {
		t.Fatalf("dependent phase=%s, want Error", res.Status.Phase)
	}
	// Stable: no retries, no writes, conclusion does not flap.
	c1 := st.Counters()
	time.Sleep(100 * time.Millisecond)
	c2 := st.Counters()
	if c1 != c2 {
		t.Fatalf("dependent kept retrying after terminal dependency failure: %+v -> %+v", c1, c2)
	}
	if c2.Creates != 0 || liveExists(st, down) {
		t.Fatalf("dependent must not land after terminal dependency failure: %+v", c2)
	}
}

// TestDependencyMissing: referencing a resource that does not exist
// yields a stable explanation, never a crash, never a stuck reconciler.
func TestDependencyMissing(t *testing.T) {
	st := state.NewStore()
	r := New(st, testConfig())
	key := "Widget/orphan"
	st.PutDesired(depResource("orphan", "Widget/ghost"))

	t.Logf("trigger input=%s dependsOn=[Widget/ghost] (absent)", key)
	d, err := r.ReconcileOnce(key)
	t.Logf("decision=%q err=%v", d, err)
	if api.ClassOf(err) != api.ClassPermanent {
		t.Fatalf("err=%v, want permanent classification", err)
	}
	if got := condReason(t, st, key); got != api.ReasonDependencyMissing {
		t.Fatalf("reason=%s, want DependencyMissing", got)
	}
	if liveExists(st, key) {
		t.Fatal("resource with missing dependency must not land")
	}
	// A healthy resource still reconciles: the reconciler is not stuck.
	st.PutDesired(newResource("healthy"))
	if _, err := r.ReconcileOnce("Widget/healthy"); err != nil {
		t.Fatalf("reconciler stuck after missing dependency: %v", err)
	}
	t.Logf("decision basis: missing dependency is a stable config explanation; other keys unaffected")
}

// TestDependencyCycle: mutual (and self) references are reported as a
// distinguishable configuration problem; the reconciler keeps working.
func TestDependencyCycle(t *testing.T) {
	st := state.NewStore()
	r := New(st, testConfig())
	r.Logf = func(format string, a ...any) { t.Logf(format, a...) }
	st.PutDesired(depResource("a", "Widget/b"))
	st.PutDesired(depResource("b", "Widget/a"))
	st.PutDesired(depResource("self", "Widget/self"))
	st.PutDesired(newResource("free"))
	r.Start()
	defer r.Stop()

	t.Logf("trigger input: a<->b cycle, self->self loop, plus healthy free")
	for _, k := range []string{"Widget/a", "Widget/b", "Widget/self", "Widget/free"} {
		r.Trigger(k)
	}
	waitFor(t, 2*time.Second, func() bool {
		for _, k := range []string{"Widget/a", "Widget/b", "Widget/self"} {
			res, ok := st.GetDesired(k)
			if !ok {
				return false
			}
			c := res.Status.GetCondition(api.ConditionReady)
			if c == nil || c.Reason != api.ReasonDependencyCycle {
				return false
			}
		}
		return true
	})
	for _, k := range []string{"Widget/a", "Widget/b", "Widget/self"} {
		t.Logf("cycle member %s: reason=%s", k, condReason(t, st, k))
		if liveExists(st, k) {
			t.Fatalf("cyclic resource %s must never land", k)
		}
	}
	waitFor(t, time.Second, func() bool {
		res, ok := st.GetDesired("Widget/free")
		return ok && res.Status.Phase == api.PhaseReady
	})
	t.Logf("reconciler not jammed: Widget/free reason=%s", condReason(t, st, "Widget/free"))

	// The cycle conclusion is stable: it does not spin.
	c1 := st.Counters()
	time.Sleep(100 * time.Millisecond)
	if c2 := st.Counters(); c2 != c1 {
		t.Fatalf("cyclic resources kept churning: %+v -> %+v", c1, c2)
	}
}

// TestCascadeDelete: an upstream resource cannot be released while
// dependents still exist; once the downstream chain is cleaned up the
// upstream finishes under the original cleanup rules, with no double
// counting and no leftovers.
func TestCascadeDelete(t *testing.T) {
	st := state.NewStore()
	r := New(st, testConfig())
	r.Logf = func(format string, a ...any) { t.Logf(format, a...) }
	up, mid, down := "Widget/up", "Widget/mid", "Widget/down"
	st.PutDesired(newResource("up"))
	st.PutDesired(depResource("mid", up))
	st.PutDesired(depResource("down", mid))
	r.Start()
	defer r.Stop()

	for _, k := range []string{up, mid, down} {
		r.Trigger(k)
	}
	waitFor(t, 2*time.Second, func() bool {
		for _, k := range []string{up, mid, down} {
			res, ok := st.GetDesired(k)
			if !ok || res.Status.Phase != api.PhaseReady {
				return false
			}
		}
		return true
	})
	t.Logf("chain ready: up<-mid<-down; counters=%+v", st.Counters())

	markDeleting := func(key string) {
		res, _ := st.GetDesired(key)
		now := time.Now()
		res.DeletionTimestamp = &now
		st.UpdateDesired(res)
		r.Trigger(key)
	}

	// Delete the head of the chain first: it must be held, not released.
	t.Logf("trigger input: delete(%s) while mid/down still exist", up)
	markDeleting(up)
	waitFor(t, time.Second, func() bool {
		res, ok := st.GetDesired(up)
		if !ok {
			return false
		}
		c := res.Status.GetCondition(api.ConditionReady)
		return c != nil && c.Reason == api.ReasonDeleteWaitingDependents
	})
	t.Logf("upstream held: reason=%s (basis: downstream records still present)",
		condReason(t, st, up))
	if !liveExists(st, up) {
		t.Fatal("upstream live object released while dependents exist")
	}
	if c := st.Counters(); c.Deletes != 0 {
		t.Fatalf("upstream cleanup ran early: %+v", c)
	}

	// Deleting mid is held by down; deleting down completes and the
	// whole cascade then unwinds automatically.
	t.Logf("trigger input: delete(%s), delete(%s); expect cascade mid->up to auto-complete", mid, down)
	markDeleting(mid)
	markDeleting(down)
	waitFor(t, 2*time.Second, func() bool {
		return len(st.ListDesired()) == 0
	})
	for _, k := range []string{up, mid, down} {
		if liveExists(st, k) {
			t.Fatalf("leftover live object for %s after cascade delete", k)
		}
	}
	c := st.Counters()
	t.Logf("cascade complete: counters=%+v (basis: each cleanup ran exactly once, no residue)", c)
	if c.Deletes != 3 {
		t.Fatalf("Deletes=%d, want exactly 3 (up, mid, down)", c.Deletes)
	}

	// Re-triggering deleted keys is a harmless no-op: no double count.
	for _, k := range []string{up, mid, down} {
		r.Trigger(k)
	}
	time.Sleep(80 * time.Millisecond)
	if c2 := st.Counters(); c2.Deletes != 3 {
		t.Fatalf("delete double-counted after extra triggers: %+v", c2)
	}
}
