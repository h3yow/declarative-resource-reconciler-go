package reconciler_test

import (
	"testing"
	"time"

	"github.com/h3yow/declarative-resource-reconciler-go/pkg/api"
)

// TestIdempotentConvergence verifies:
//   - repeated identical triggers produce no duplicate side effects;
//   - once observed state matches desired, writes are skipped;
//   - changing desired state converges to the new state exactly once.
func TestIdempotentConvergence(t *testing.T) {
	h := newHarness(t)
	const id = "w-1"

	spec := api.WidgetSpec{Image: "img:v1", Replicas: 2, Config: "a"}
	h.rec.Trigger(api.DesiredState{ID: id, Generation: 1, Spec: spec})
	h.eventuallyReady(id, 2)

	// Drain the post-create Unknown->True condition transition: the first
	// settling trigger performs the correction, the second must skip.
	decisionsBefore := len(h.rec.Decisions(id))
	h.rec.Trigger(api.DesiredState{ID: id, Generation: 1, Spec: spec})
	h.rec.Trigger(api.DesiredState{ID: id, Generation: 1, Spec: spec})
	waitFor(2*time.Second, func() bool {
		for _, d := range h.rec.Decisions(id)[decisionsBefore:] {
			if d.Action == api.ActionSkip {
				return true
			}
		}
		return false
	})
	beforeCalls := h.bus.EnsureCalls(id)
	beforeVersion := h.getResource(t, id).ResourceVersion
	beforeSkips := h.rec.Stats().Skips

	// Repeat the same desired state many times: convergence is idempotent.
	for i := 0; i < 5; i++ {
		h.rec.Trigger(api.DesiredState{ID: id, Generation: 1, Spec: spec})
	}
	waitFor(2*time.Second, func() bool {
		return h.rec.Stats().Skips-beforeSkips >= 5
	})

	if got := h.bus.EnsureCalls(id); got != beforeCalls {
		t.Errorf("Ensure calls changed after identical re-triggers: before=%d after=%d (want no duplicate effects)",
			beforeCalls, got)
	}
	if got := h.bus.EffectCount(); got != 1 {
		t.Errorf("live effect count = %d, want 1 (no duplicate side effects)", got)
	}
	if v := h.getResource(t, id).ResourceVersion; v != beforeVersion {
		t.Errorf("resource version changed after identical re-triggers: before=%d after=%d (want skip writes)",
			beforeVersion, v)
	}

	// A genuine change converges once to the new state.
	spec2 := api.WidgetSpec{Image: "img:v2", Replicas: 3}
	h.rec.Trigger(api.DesiredState{ID: id, Generation: 2, Spec: spec2})
	h.eventuallyReady(id, 3)

	r := h.getResource(t, id)
	if r.SpecHash != api.SpecHash(spec2) {
		t.Errorf("spec hash = %s, want %s", r.SpecHash, api.SpecHash(spec2))
	}
	if r.Generation != 2 {
		t.Errorf("generation = %d, want 2", r.Generation)
	}
	if h.bus.HasEffect(id) != true {
		t.Errorf("effect missing after drift rebuild")
	}

	t.Logf("trigger inputs: gen1 x6, gen2 x1; ensureCalls=%d effectCount=%d finalVersion=%d; decisions:\n",
		h.bus.EnsureCalls(id), h.bus.EffectCount(), r.ResourceVersion)
	h.rec.DecisionsText(&logWriter{t}, id)
}

// TestSkipWhenSatisfied directly asserts the skip decision reason.
func TestSkipWhenSatisfied(t *testing.T) {
	h := newHarness(t)
	const id = "w-skip"
	spec := api.WidgetSpec{Image: "img:v1", Replicas: 1}
	h.rec.Trigger(api.DesiredState{ID: id, Generation: 1, Spec: spec})
	h.eventuallyReady(id, 1)

	// First post-ready trigger settles the condition tuple; the second must
	// be a pure skip with no write.
	h.rec.Trigger(api.DesiredState{ID: id, Generation: 1, Spec: spec})
	h.rec.Trigger(api.DesiredState{ID: id, Generation: 1, Spec: spec})
	waitFor(2*time.Second, func() bool {
		for _, d := range h.rec.Decisions(id) {
			if d.Action == api.ActionSkip && d.Reason == api.ReasonIdle {
				return true
			}
		}
		return false
	})
	found := false
	for _, d := range h.rec.Decisions(id) {
		if d.Action == api.ActionSkip && d.Reason == api.ReasonIdle {
			found = true
			t.Logf("judgement basis: action=%s reason=%s detail=%q", d.Action, d.Reason, d.Detail)
		}
	}
	if !found {
		t.Fatal("expected an Idle skip decision after re-trigger")
	}
}
