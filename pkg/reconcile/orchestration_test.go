package reconcile

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/h3yow/declarative-resource-reconciler-go/pkg/api"
	"github.com/h3yow/declarative-resource-reconciler-go/pkg/state"
)

// newResourceDeps builds a resource that declares dependencies on other
// keys in the same desired set.
func newResourceDeps(name string, deps ...string) *api.Resource {
	r := newResource(name)
	r.Spec.DependsOn = deps
	return r
}

// markDelete sets the deletion timestamp the way an external caller would.
func markDelete(t *testing.T, st *state.Store, key string) {
	t.Helper()
	res, ok := st.GetDesired(key)
	if !ok {
		t.Fatalf("markDelete: %s not found", key)
	}
	now := time.Now()
	res.DeletionTimestamp = &now
	st.UpdateDesired(res)
	t.Logf("trigger input=delete(%s)", key)
}

// TestDependencyGateDeterministic walks the dependency gate in single
// passes: out-of-order reconciliation parks the dependent without any
// write, and once the dependency converges the dependent converges.
// Repeated passes with unchanged state are observable no-ops.
func TestDependencyGateDeterministic(t *testing.T) {
	st := state.NewStore()
	r := New(st, testConfig())
	st.PutDesired(newResource("base"))
	top := newResourceDeps("top", "Widget/base")
	st.PutDesired(top)

	// Out of order: dependent first. It must park, not create.
	d, err := r.ReconcileOnce("Widget/top")
	if err != nil {
		t.Fatalf("parked pass must not error, got %v", err)
	}
	t.Logf("trigger=Widget/top (dep Widget/base not ready) decision=%q", d)
	if got := condReason(t, st, "Widget/top"); got != api.ReasonWaitingForDependency {
		t.Fatalf("reason=%s, want WaitingForDependency", got)
	}
	if _, ok := st.GetLive("Widget/top"); ok {
		t.Fatal("dependent created its live object before the dependency was ready")
	}
	if c := st.Counters(); c.Creates != 0 {
		t.Fatalf("Creates=%d while dependency unready, want 0", c.Creates)
	}

	// Dependency converges.
	if _, err := r.ReconcileOnce("Widget/base"); err != nil {
		t.Fatal(err)
	}
	t.Logf("trigger=Widget/base decision=converged; basis: no deps declared")

	// Now the dependent may land.
	d, err = r.ReconcileOnce("Widget/top")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("trigger=Widget/top (dep ready) decision=%q", d)
	if got := condReason(t, st, "Widget/top"); got != api.ReasonReady {
		t.Fatalf("reason=%s, want Ready", got)
	}

	// Unchanged desired+observed: further passes are observable no-ops.
	before := st.Counters()
	for i := 0; i < 3; i++ {
		if _, err := r.ReconcileOnce("Widget/top"); err != nil {
			t.Fatal(err)
		}
		if _, err := r.ReconcileOnce("Widget/base"); err != nil {
			t.Fatal(err)
		}
	}
	after := st.Counters()
	if before != after {
		t.Fatalf("counters moved on no-op passes: before=%+v after=%+v", before, after)
	}
	t.Logf("no-op basis: counters unchanged at %+v (Creates=2 total, one per resource)", after)
}

// TestDependencyOrderingConcurrentTriggers hammers a base->mid->top
// chain with concurrent, duplicated, out-of-order triggers and asserts
// the chain converges exactly once per resource with no duplicates.
func TestDependencyOrderingConcurrentTriggers(t *testing.T) {
	st := state.NewStore()
	r := New(st, testConfig())
	r.Logf = func(format string, args ...any) { t.Logf(format, args...) }
	st.PutDesired(newResource("base"))
	st.PutDesired(newResourceDeps("mid", "Widget/base"))
	st.PutDesired(newResourceDeps("top", "Widget/mid"))
	r.Start()
	defer r.Stop()

	// Reverse declaration order, many duplicates, all concurrent.
	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		for _, key := range []string{"Widget/top", "Widget/mid", "Widget/base"} {
			wg.Add(1)
			go func(k string, n int) {
				defer wg.Done()
				t.Logf("trigger #%d key=%s", n, k)
				r.Trigger(k)
			}(key, i)
		}
	}
	wg.Wait()

	for _, key := range []string{"Widget/base", "Widget/mid", "Widget/top"} {
		k := key
		waitFor(t, 5*time.Second, func() bool {
			return condReason(t, st, k) == api.ReasonReady
		})
	}
	c := st.Counters()
	if c.Creates != 3 || c.Updates != 0 || c.SideCalls != 3 {
		t.Fatalf("counters=%+v, want Creates=3 Updates=0 SideCalls=3 (exactly once per resource)", c)
	}
	t.Logf("decision basis: all three Ready after concurrent storm; counters=%+v", c)
}

// TestDependencyTransientFailureAndRecovery: the dependent parks in a
// distinguishable waiting state while its dependency fights a transient
// fault, and resumes automatically once the dependency heals — without
// any manual re-trigger of the dependent.
func TestDependencyTransientFailureAndRecovery(t *testing.T) {
	st := state.NewStore()
	r := New(st, testConfig())
	r.Logf = func(format string, args ...any) { t.Logf(format, args...) }
	st.PutDesired(newResource("base"))
	st.PutDesired(newResourceDeps("top", "Widget/base"))
	st.SetFaults("Widget/base", state.Faults{TransientTimes: 2})
	r.Start()
	defer r.Stop()

	t.Logf("trigger input=Widget/top,Widget/base with base TransientTimes=2")
	r.Trigger("Widget/top")
	r.Trigger("Widget/base")

	// The dependent parks on the dependency, distinguishable from its
	// own failure: WaitingForDependency, not TransientFailure.
	waitFor(t, 5*time.Second, func() bool {
		return condReason(t, st, "Widget/top") == api.ReasonWaitingForDependency
	})
	t.Logf("Widget/top parked: waiting on Widget/base (not its own failure)")
	if _, ok := st.GetLive("Widget/top"); ok {
		t.Fatal("dependent landed while dependency was still failing")
	}

	// No further manual trigger of Widget/top: the base healing must
	// wake it automatically.
	waitFor(t, 5*time.Second, func() bool {
		return condReason(t, st, "Widget/base") == api.ReasonReady
	})
	waitFor(t, 5*time.Second, func() bool {
		return condReason(t, st, "Widget/top") == api.ReasonReady
	})
	c := st.Counters()
	if c.Creates != 2 || c.SideCalls != 2 {
		t.Fatalf("counters=%+v, want Creates=2 SideCalls=2", c)
	}
	t.Logf("decision basis: base healed after transient retries, woke top automatically; counters=%+v", c)
}

// TestDependencyPermanentFailure: a terminally failed dependency yields
// a stable, distinguishable conclusion on the dependent, which stops
// instead of retrying.
func TestDependencyPermanentFailure(t *testing.T) {
	st := state.NewStore()
	r := New(st, testConfig())
	r.Logf = func(format string, args ...any) { t.Logf(format, args...) }
	st.PutDesired(newResource("base"))
	st.PutDesired(newResourceDeps("top", "Widget/base"))
	st.SetFaults("Widget/base", state.Faults{Permanent: true})
	r.Start()
	defer r.Stop()

	t.Logf("trigger input=Widget/top,Widget/base with base permanent write failure")
	r.Trigger("Widget/top")
	r.Trigger("Widget/base")

	waitFor(t, 5*time.Second, func() bool {
		return condReason(t, st, "Widget/base") == api.ReasonPermanentFailure
	})
	waitFor(t, 5*time.Second, func() bool {
		return condReason(t, st, "Widget/top") == api.ReasonDependencyFailed
	})
	res, _ := st.GetDesired("Widget/top")
	if res.Status.Phase != api.PhaseError {
		t.Fatalf("dependent phase=%s, want Error (stable terminal conclusion)", res.Status.Phase)
	}
	if _, ok := st.GetLive("Widget/top"); ok {
		t.Fatal("dependent landed despite terminally failed dependency")
	}

	// Stability: nothing keeps spinning after the conclusion.
	time.Sleep(50 * time.Millisecond)
	if n := r.queue.Len(); n != 0 {
		t.Fatalf("queue still has %d items; dependent should have stopped retrying", n)
	}
	if got := condReason(t, st, "Widget/top"); got != api.ReasonDependencyFailed {
		t.Fatalf("conclusion drifted to %s, want stable DependencyFailed", got)
	}
	c := st.Counters()
	if c.Creates != 0 {
		t.Fatalf("Creates=%d, want 0 (dependency never converged)", c.Creates)
	}
	t.Logf("decision basis: base PermanentFailure -> top DependencyFailed, queue drained, no spin")
}

// TestDependencyMissingThenAppears: referencing a nonexistent resource
// is a stable configuration conclusion, not a crash and not a hang; if
// the dependency is later declared and converges, the dependent
// recovers automatically.
func TestDependencyMissingThenAppears(t *testing.T) {
	st := state.NewStore()
	r := New(st, testConfig())
	r.Logf = func(format string, args ...any) { t.Logf(format, args...) }
	st.PutDesired(newResourceDeps("top", "Widget/ghost"))
	r.Start()
	defer r.Stop()

	t.Logf("trigger input=Widget/top with dependsOn=Widget/ghost (absent)")
	r.Trigger("Widget/top")
	waitFor(t, 5*time.Second, func() bool {
		return condReason(t, st, "Widget/top") == api.ReasonDependencyMissing
	})
	res, _ := st.GetDesired("Widget/top")
	if res.Status.Phase != api.PhaseError {
		t.Fatalf("phase=%s, want Error for missing dependency", res.Status.Phase)
	}
	if c := st.Counters(); c.Creates != 0 {
		t.Fatalf("Creates=%d, want 0 while dependency missing", c.Creates)
	}

	// The missing dependency appears later: no manual re-trigger of top.
	st.PutDesired(newResource("ghost"))
	t.Logf("trigger input=Widget/ghost (declared late)")
	r.Trigger("Widget/ghost")
	waitFor(t, 5*time.Second, func() bool {
		return condReason(t, st, "Widget/top") == api.ReasonReady
	})
	t.Logf("decision basis: late-declared dependency converged and woke the dependent automatically")
}

// TestDependencyCycle: a dependency cycle is reported as a
// distinguishable configuration problem on every member; nothing is
// created and the reconciler neither crashes nor spins.
func TestDependencyCycle(t *testing.T) {
	st := state.NewStore()
	r := New(st, testConfig())
	r.Logf = func(format string, args ...any) { t.Logf(format, args...) }
	st.PutDesired(newResourceDeps("a", "Widget/b"))
	st.PutDesired(newResourceDeps("b", "Widget/a"))
	r.Start()
	defer r.Stop()

	t.Logf("trigger input=Widget/a,Widget/b forming cycle a->b->a")
	r.Trigger("Widget/a")
	r.Trigger("Widget/b")

	for _, key := range []string{"Widget/a", "Widget/b"} {
		k := key
		waitFor(t, 5*time.Second, func() bool {
			return condReason(t, st, k) == api.ReasonDependencyCycle
		})
		res, _ := st.GetDesired(k)
		if res.Status.Phase != api.PhaseError {
			t.Fatalf("%s phase=%s, want Error", k, res.Status.Phase)
		}
		t.Logf("%s cycle conclusion: %s", k, res.Status.GetCondition(api.ConditionReady).Message)
	}

	// The wake-up cascade must terminate: parked conditions are
	// idempotent, so nothing keeps re-triggering.
	time.Sleep(50 * time.Millisecond)
	if n := r.queue.Len(); n != 0 {
		t.Fatalf("queue still has %d items; cycle detection must not spin", n)
	}
	if c := st.Counters(); c.Creates != 0 || c.SideCalls != 0 {
		t.Fatalf("counters=%+v, want no side effects for cyclic declarations", c)
	}
	t.Logf("decision basis: cycle reported on both members, queue drained, zero side effects")
}

// TestCascadingDelete: an upstream cannot be released while downstream
// resources still exist; once downstream cleanup completes the upstream
// deletion resumes automatically, and the whole cascade counts each
// delete exactly once.
func TestCascadingDelete(t *testing.T) {
	st := state.NewStore()
	r := New(st, testConfig())
	r.Logf = func(format string, args ...any) { t.Logf(format, args...) }
	st.PutDesired(newResource("base"))
	st.PutDesired(newResourceDeps("mid", "Widget/base"))
	st.PutDesired(newResourceDeps("top", "Widget/mid"))
	r.Start()
	defer r.Stop()

	for _, key := range []string{"Widget/base", "Widget/mid", "Widget/top"} {
		r.Trigger(key)
	}
	for _, key := range []string{"Widget/base", "Widget/mid", "Widget/top"} {
		k := key
		waitFor(t, 5*time.Second, func() bool {
			return condReason(t, st, k) == api.ReasonReady
		})
	}
	t.Logf("chain converged; now deleting everything at once")

	// Slow downstream cleanup down so the upstream's parked state is
	// deterministically observable while downstream resources exist.
	st.SetFaults("Widget/top", state.Faults{CleanupTimes: 1})
	st.SetFaults("Widget/mid", state.Faults{CleanupTimes: 2})

	// Delete the whole chain concurrently, upstream first.
	markDelete(t, st, "Widget/base")
	markDelete(t, st, "Widget/mid")
	markDelete(t, st, "Widget/top")
	r.Trigger("Widget/base")
	r.Trigger("Widget/mid")
	r.Trigger("Widget/top")

	// The upstream parks in a distinguishable state while downstream
	// resources still exist.
	waitFor(t, 5*time.Second, func() bool {
		return condReason(t, st, "Widget/base") == api.ReasonDeleteWaitingDependents
	})
	if _, ok := st.GetLive("Widget/base"); !ok {
		t.Fatal("upstream live object released while downstream resources still exist")
	}
	t.Logf("Widget/base parked: waiting for downstream cleanup (distinct from its own cleanup)")

	// The cascade completes on its own: downstream removal wakes the
	// upstream, which then runs its ordinary cleanup.
	for _, key := range []string{"Widget/top", "Widget/mid", "Widget/base"} {
		k := key
		waitFor(t, 5*time.Second, func() bool {
			_, ok := st.GetDesired(k)
			return !ok
		})
		t.Logf("%s fully removed", k)
	}
	c := st.Counters()
	if c.Deletes != 3 {
		t.Fatalf("Deletes=%d, want exactly 3 (no double counting, no residue)", c.Deletes)
	}
	for _, key := range []string{"Widget/base", "Widget/mid", "Widget/top"} {
		if _, ok := st.GetLive(key); ok {
			t.Fatalf("live object %s leaked after cascade delete", key)
		}
	}
	t.Logf("decision basis: cascade completed downstream-first; counters=%+v; no residue", c)
}

// TestMixedBatchIndependentResourcesUnchanged: resources without any
// dependency declaration in the same batch as dependent ones behave
// exactly as before.
func TestMixedBatchIndependentResourcesUnchanged(t *testing.T) {
	st := state.NewStore()
	r := New(st, testConfig())
	r.Logf = func(format string, args ...any) { t.Logf(format, args...) }
	st.PutDesired(newResource("solo"))
	st.PutDesired(newResourceDeps("dependent", "Widget/solo"))
	r.Start()
	defer r.Stop()

	t.Logf("trigger input=Widget/dependent,Widget/solo (solo has no deps)")
	r.Trigger("Widget/dependent")
	r.Trigger("Widget/solo")

	// The independent resource converges immediately, first pass.
	waitFor(t, 5*time.Second, func() bool {
		return condReason(t, st, "Widget/solo") == api.ReasonReady
	})
	waitFor(t, 5*time.Second, func() bool {
		return condReason(t, st, "Widget/dependent") == api.ReasonReady
	})
	c := st.Counters()
	if c.Creates != 2 || c.SideCalls != 2 {
		t.Fatalf("counters=%+v, want Creates=2 SideCalls=2", c)
	}
	fmt.Println()
	t.Logf("decision basis: no-dependsOn resource converged on first pass, dependent followed")
}
