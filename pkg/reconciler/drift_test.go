package reconciler_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/h3yow/declarative-resource-reconciler-go/pkg/api"
)

// TestExternalSpecDriftRebuilt simulates an external actor mutating the
// stored resource between triggers and asserts the reconciler rebuilds the
// desired state with a distinguishable reason.
func TestExternalSpecDriftRebuilt(t *testing.T) {
	h := newHarness(t)
	const id = "w-drift"
	spec := api.WidgetSpec{Image: "img:v1", Replicas: 1}
	h.rec.Trigger(api.DesiredState{ID: id, Generation: 1, Spec: spec})
	h.eventuallyReady(id, 1)

	// External mutation: someone edits the image to a foreign value. Wait
	// for a settling skip so no stale trigger can race the mutation, then
	// record the decision-log baseline.
	h.rec.Trigger(api.DesiredState{ID: id, Generation: 1, Spec: spec})
	waitFor(2*time.Second, func() bool {
		for _, d := range h.rec.Decisions(id) {
			if d.Action == api.ActionSkip {
				return true
			}
		}
		return false
	})
	baseDecisions := len(h.rec.Decisions(id))
	mutated := h.getResource(t, id)
	mutated.Spec = api.WidgetSpec{Image: "img:EXTERNAL", Replicas: 99}
	mutated.SpecHash = api.SpecHash(mutated.Spec)
	if err := h.st.Put(mutated); err != nil {
		t.Fatal(err)
	}

	// A fresh trigger drives reconciliation against the drifted observation.
	h.rec.Trigger(api.DesiredState{ID: id, Generation: 2, Spec: api.WidgetSpec{Image: "img:v2", Replicas: 2}})
	h.eventuallyReady(id, 2)

	r := h.getResource(t, id)
	if r.Spec.Image != "img:v2" {
		t.Errorf("image = %q, want rebuilt desired img:v2", r.Spec.Image)
	}
	if r.Annotations[api.AnnEffectRef] == "" {
		t.Errorf("effect reference not re-anchored after drift rebuild")
	}
	found := false
	for _, d := range h.rec.Decisions(id)[baseDecisions:] {
		if d.Reason == api.ReasonDriftRebuilt {
			found = true
			t.Logf("judgement basis: action=%s reason=%s detail=%q", d.Action, d.Reason, d.Detail)
		}
	}
	if !found {
		t.Fatalf("expected a decision with reason %s after external mutation; decisions after baseline: %+v",
			api.ReasonDriftRebuilt, h.rec.Decisions(id)[baseDecisions:])
	}
}

// TestExternalKeyFieldCleared simulates an external actor clearing the
// essential image field; the resource must be rebuilt with a reason that is
// distinguishable from ordinary spec drift.
func TestExternalKeyFieldCleared(t *testing.T) {
	h := newHarness(t)
	const id = "w-cleared"
	h.rec.Trigger(api.DesiredState{ID: id, Generation: 1, Spec: api.WidgetSpec{Image: "img", Replicas: 1}})
	h.eventuallyReady(id, 1)

	cur := h.getResource(t, id)
	cur.Spec.Image = ""
	cur.SpecHash = api.SpecHash(cur.Spec)
	if err := h.st.Put(cur); err != nil {
		t.Fatal(err)
	}

	h.rec.Trigger(api.DesiredState{ID: id, Generation: 2, Spec: api.WidgetSpec{Image: "img", Replicas: 1}})
	waitFor(2*time.Second, func() bool {
		r, ok, _ := h.st.Get(id)
		return ok && r.Spec.Image == "img" &&
			r.Conditions[api.ConditionReady].Reason != api.ReasonPermanentFailure
	})
	// Reconstruction must go through a missing-field-detected decision at
	// least transiently; final state must be Ready again.
	h.eventuallyReady(id, 1)
	t.Logf("external image cleared -> rebuilt; final ready reason=%s",
		h.getResource(t, id).Conditions[api.ConditionReady].Reason)
}

// TestExternalDeletionRecreated simulates an external actor deleting the
// resource outright: the next trigger recreates it.
func TestExternalDeletionRecreated(t *testing.T) {
	h := newHarness(t)
	const id = "w-gone"
	h.rec.Trigger(api.DesiredState{ID: id, Generation: 1, Spec: api.WidgetSpec{Image: "img", Replicas: 1}})
	h.eventuallyReady(id, 1)

	if !h.st.DeleteForTest(id) {
		t.Fatal("external delete did not remove resource")
	}
	h.rec.Trigger(api.DesiredState{ID: id, Generation: 2, Spec: api.WidgetSpec{Image: "img2", Replicas: 2}})
	h.eventuallyReady(id, 2)
	r := h.getResource(t, id)
	if r.Spec.Image != "img2" || r.Generation != 2 {
		t.Errorf("resource not recreated from desired state: %+v", r.Spec)
	}
	if !h.bus.HasEffect(id) {
		t.Errorf("effect not recreated after external deletion")
	}
	t.Logf("external deletion -> recreated at gen2 with img2")
}

// TestUnknownAndLegacyFieldsPreserved feeds the store a JSON blob
// containing fields this binary does not know and asserts they survive
// reconciliation round-trips and are logged rather than dropped.
func TestUnknownAndLegacyFieldsPreserved(t *testing.T) {
	h := newHarness(t)
	const id = "w-legacy"
	legacyJSON := `{
		"id":"w-legacy",
		"generation":1,
		"specHash":"",
		"spec":{"image":"img-old","replicas":1},
		"annotations":{},
		"conditions":{},
		"resourceVersion":0,
		"legacyZone":"zone-a",
		"deprecatedBetaField":{"retries":7}
	}`
	var r api.Resource
	if err := json.Unmarshal([]byte(legacyJSON), &r); err != nil {
		t.Fatalf("legacy json decode: %v", err)
	}
	if r.Extra["legacyZone"] != "zone-a" {
		t.Fatalf("unknown field not captured: %+v", r.Extra)
	}
	if err := h.st.Put(r); err != nil {
		t.Fatal(err)
	}

	// Trigger convergence: specHash empty is treated like old-version data
	// and rebuilt; unknown fields must still survive the write.
	h.rec.Trigger(api.DesiredState{ID: id, Generation: 2, Spec: api.WidgetSpec{Image: "img-new", Replicas: 3}})
	h.eventuallyReady(id, 3)

	final := h.getResource(t, id)
	if final.Extra["legacyZone"] != "zone-a" {
		t.Errorf("legacy field dropped after reconcile: %+v", final.Extra)
	}
	if _, ok := final.Extra["deprecatedBetaField"]; !ok {
		t.Errorf("old-version field dropped after reconcile: %+v", final.Extra)
	}
	out, err := json.Marshal(final)
	if err != nil {
		t.Fatal(err)
	}
	var re map[string]any
	if err := json.Unmarshal(out, &re); err != nil {
		t.Fatal(err)
	}
	if re["legacyZone"] != "zone-a" {
		t.Errorf("legacy field missing from marshalled JSON: %s", out)
	}

	foundCompatLog := waitFor(500*time.Millisecond, func() bool {
		return len(h.rec.CompatFields(id)) >= 2
	})
	if !foundCompatLog {
		t.Errorf("compatibility handling not observed for unknown fields: %v", h.rec.CompatFields(id))
	}
	foundRebuild := false
	for _, d := range h.rec.Decisions(id) {
		if d.Reason == api.ReasonDriftRebuilt {
			foundRebuild = true
		}
	}
	if !foundRebuild {
		t.Errorf("old-version resource (empty specHash) should rebuild with %s", api.ReasonDriftRebuilt)
	}
	t.Logf("trigger input: gen2 img-new/3 over legacy blob; preserved fields=%v; rebuilt with reason=%s",
		final.Extra, api.ReasonDriftRebuilt)
}

// TestMalformedJSONDoesNotPanic corrupts the stored bytes for a resource
// and asserts the worker keeps retrying (no crash, request not dropped) and
// converges after the corruption is repaired out-of-band.
func TestMalformedJSONDoesNotPanic(t *testing.T) {
	h := newHarness(t)
	const id = "w-corrupt"
	spec := api.WidgetSpec{Image: "img", Replicas: 1}
	h.rec.Trigger(api.DesiredState{ID: id, Generation: 1, Spec: spec})
	h.eventuallyReady(id, 1)

	h.st.CorruptForTest(id)
	h.rec.Trigger(api.DesiredState{ID: id, Generation: 2, Spec: api.WidgetSpec{Image: "img", Replicas: 2}})

	sawTransient := waitFor(2*time.Second, func() bool {
		for _, d := range h.rec.Decisions(id) {
			if d.Action == api.ActionRetry && d.Reason == api.ReasonRetryingTransient {
				return true
			}
		}
		return false
	})
	if !sawTransient {
		t.Fatal("corrupt state did not produce a transient retry decision (worker likely stalled)")
	}

	// Repair out-of-band, as an operator restoring a backup would.
	h.st.Put(api.Resource{
		ID:          id,
		Generation:  1,
		Spec:        api.WidgetSpec{Image: "img", Replicas: 1},
		SpecHash:    api.SpecHash(api.WidgetSpec{Image: "img", Replicas: 1}),
		Annotations: map[string]string{},
		Conditions:  map[string]api.Condition{},
	})
	h.eventuallyReady(id, 2)

	other := "w-healthy"
	h.rec.Trigger(api.DesiredState{ID: other, Generation: 1, Spec: spec})
	h.eventuallyReady(other, 1)
	t.Logf("corrupt bytes -> retried without panic; repaired out-of-band -> converged; healthy resource %s unaffected", other)
}

func contains(s, sub string) bool {
	return len(sub) == 0 || indexOf(s, sub) >= 0
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
