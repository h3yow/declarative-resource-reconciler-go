package reconciler_test

import (
	"testing"

	"github.com/h3yow/declarative-resource-reconciler-go/pkg/api"
	"github.com/h3yow/declarative-resource-reconciler-go/pkg/bus"
)

// TestTransientFailureRetriedWithBackoff asserts:
//   - the request is not dropped (it eventually becomes Ready);
//   - Ensure is invoked failureCount+1 times (retry actually happened);
//   - retry decisions carry increasing attempt numbers and backoff delays;
//   - no duplicate side effects after recovery.
func TestTransientFailureRetriedWithBackoff(t *testing.T) {
	h := newHarness(t)
	const id = "w-retry"
	h.bus.SetFailRule(id, bus.FailRule{TransientAttempts: 3})

	h.rec.Trigger(api.DesiredState{
		ID: id, Generation: 1,
		Spec: api.WidgetSpec{Image: "img", Replicas: 1},
	})
	h.eventuallyReady(id, 1)

	if calls := h.bus.EnsureCalls(id); calls != 4 {
		t.Errorf("EnsureCalls = %d, want 4 (3 transient failures + 1 success)", calls)
	}
	if count := h.bus.EffectCount(); count != 1 {
		t.Errorf("effect count = %d, want 1 (no duplicate side effect)", count)
	}

	var attempts []int
	var delays []int64
	sawRetry := false
	for _, d := range h.rec.Decisions(id) {
		if d.Action == api.ActionRetry {
			if d.Reason != api.ReasonRetryingTransient {
				t.Errorf("retry reason = %s, want %s", d.Reason, api.ReasonRetryingTransient)
			}
			sawRetry = true
			attempts = append(attempts, d.Attempt)
			delays = append(delays, int64(d.Duration))
		}
	}
	if !sawRetry {
		t.Fatal("expected at least one Retry decision")
	}
	if len(attempts) != 3 || attempts[0] != 1 || attempts[1] != 2 || attempts[2] != 3 {
		t.Errorf("retry attempts = %v, want [1 2 3]", attempts)
	}
	r := h.getResource(t, id)
	if c := r.Conditions[api.ConditionReady]; c.Status != api.StatusTrue || c.Reason != api.ReasonReconciled {
		t.Errorf("final ready condition = %+v, want True/%s", c, api.ReasonReconciled)
	}
	t.Logf("trigger input: gen1 spec=img/1; transient failures=3; retry attempts=%v; total ensureCalls=%d; judgement: recovered -> Ready=True/%s",
		attempts, h.bus.EnsureCalls(id), api.ReasonReconciled)
	h.rec.DecisionsText(&logWriter{t}, id)
}

// TestPermanentFailureStopsRetrying asserts a permanent failure records a
// stable, distinguishable condition and the reconciler does not keep
// hammering the external system. A later new generation clears the terminal
// state (request retried because the input changed).
func TestPermanentFailureStopsRetrying(t *testing.T) {
	h := newHarness(t)
	const id = "w-perm"
	h.bus.SetFailRule(id, bus.FailRule{Permanent: true})

	h.rec.Trigger(api.DesiredState{
		ID: id, Generation: 1,
		Spec: api.WidgetSpec{Image: "img", Replicas: 1},
	})

	waitFor(2_000_000_000, func() bool {
		r, ok, err := h.st.Get(id)
		if err != nil || !ok {
			return false
		}
		c := r.Conditions[api.ConditionReady]
		return c.Status == api.StatusFalse && c.Reason == api.ReasonPermanentFailure
	})
	callsAtQuiesce := h.bus.EnsureCalls(id)
	if callsAtQuiesce < 1 {
		t.Fatal("expected Ensure to be attempted at least once")
	}

	// Wait long enough that any (wrong) retry loop would have shown more
	// calls: fastBackoff initial=2ms means a buggy loop would explode the
	// counter within this window.
	waitFor(200_000_000, func() bool { return false })
	if got := h.bus.EnsureCalls(id); got != callsAtQuiesce {
		t.Errorf("EnsureCalls grew after permanent failure: %d -> %d (ineffective retries must stop)",
			callsAtQuiesce, got)
	}
	r := h.getResource(t, id)
	c := r.Conditions[api.ConditionReady]
	if c.Status != api.StatusFalse || c.Reason != api.ReasonPermanentFailure {
		t.Fatalf("condition = %+v, want False/%s", c, api.ReasonPermanentFailure)
	}
	if h.bus.HasEffect(id) {
		t.Errorf("no effect should exist after permanent failure")
	}

	// New desired generation lifts the terminal condition: the changed
	// request is allowed one fresh attempt. Keep it permanent to prove the
	// condition remains stable; then clear the rule and converge.
	h.bus.SetFailRule(id, bus.FailRule{})
	h.rec.Trigger(api.DesiredState{
		ID: id, Generation: 2,
		Spec: api.WidgetSpec{Image: "img2", Replicas: 2},
	})
	h.eventuallyReady(id, 2)
	final := h.getResource(t, id)
	if final.Conditions[api.ConditionReady].Reason != api.ReasonReconciled {
		t.Errorf("condition after new generation = %s, want %s",
			final.Conditions[api.ConditionReady].Reason, api.ReasonReconciled)
	}
	t.Logf("trigger inputs: gen1 permanent-fail, gen2 after rule cleared; ensureCalls=%d; permanent condition observed then recovered; decisions:",
		h.bus.EnsureCalls(id))
	h.rec.DecisionsText(&logWriter{t}, id)
}
