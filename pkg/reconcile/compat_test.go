package reconcile

import (
	"testing"

	"github.com/h3yow/declarative-resource-reconciler-go/pkg/api"
	"github.com/h3yow/declarative-resource-reconciler-go/pkg/state"
)

// TestMissingFieldsRebuilt: key desired fields absent -> defaults are
// applied, a distinguishable reason is reported, and convergence happens.
func TestMissingFieldsRebuilt(t *testing.T) {
	st := state.NewStore()
	r := New(st, testConfig())
	key := "Widget/m"
	st.PutDesired(&api.Resource{
		Kind: "Widget", APIVersion: "v1", Name: "m",
		Spec: api.Spec{}, // both replicas and image missing
	})
	d, err := r.ReconcileOnce(key)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("trigger=%s with empty spec: decision=%q", key, d)
	res, _ := st.GetDesired(key)
	if res.Spec.Replicas != defaultReplicas || res.Spec.Image != defaultImage {
		t.Fatalf("defaults not rebuilt: %+v", res.Spec)
	}
	live, ok := st.GetLive(key)
	if !ok || live.Replicas != defaultReplicas || live.Image != defaultImage {
		t.Fatalf("live object not built from defaults: %+v ok=%v", live, ok)
	}
	c := res.Status.GetCondition(api.ConditionReady)
	if c.Reason != api.ReasonMissingFieldRebuilt {
		t.Fatalf("reason=%s, want MissingFieldRebuilt", c.Reason)
	}

	// Second pass: defaults now present -> ordinary idempotent Ready.
	d, err = r.ReconcileOnce(key)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("trigger=%s second pass: decision=%q", key, d)
	res, _ = st.GetDesired(key)
	if got := res.Status.GetCondition(api.ConditionReady).Reason; got != api.ReasonReady {
		t.Fatalf("second pass reason=%s, want Ready", got)
	}
	if st.Counters().Updates != 0 {
		t.Fatalf("rebuilt defaults caused spurious update: %+v", st.Counters())
	}
}

// TestLegacyFieldMigratedAndUnknownPreserved exercises forward/backward
// compatibility through the JSON codec end to end.
func TestLegacyFieldMigratedAndUnknownPreserved(t *testing.T) {
	st := state.NewStore()
	r := New(st, testConfig())
	key := "Widget/l"

	raw := []byte(`{
		"kind":"Widget","apiVersion":"v1","name":"l",
		"spec":{"image":"legacy:9"},
		"replicas_v1": 5,
		"experimental": {"x": 1}
	}`)
	res, err := api.DecodeResource(raw)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("trigger input: legacy object unknown=%v", res.Unknown)
	st.PutDesired(res)

	d, err := r.ReconcileOnce(key)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("decision=%q", d)
	got, _ := st.GetDesired(key)
	if got.Spec.Replicas != 5 {
		t.Fatalf("legacy replicas not migrated: %d", got.Spec.Replicas)
	}
	c := got.Status.GetCondition(api.ConditionReady)
	if c.Reason != api.ReasonLegacyFieldMigrated {
		t.Fatalf("reason=%s, want LegacyFieldMigrated", c.Reason)
	}
	if got.Status.UnknownFields["experimental"] != "preserved" {
		t.Fatalf("unknown field not surfaced in status: %+v", got.Status.UnknownFields)
	}
	if _, stillThere := got.Unknown["experimental"]; !stillThere {
		t.Fatal("unknown field must remain on the object, not be dropped")
	}
	live, _ := st.GetLive(key)
	if live.Replicas != 5 {
		t.Fatalf("live object not created from migrated value: %+v", live)
	}
}
