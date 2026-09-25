package api_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/h3yow/declarative-resource-reconciler-go/pkg/api"
)

func TestUnknownFieldsRoundTrip(t *testing.T) {
	in := `{"id":"x","generation":3,"spec":{"image":"i","replicas":2},"futureField":{"a":1},"oldFlag":true}`
	var r api.Resource
	if err := json.Unmarshal([]byte(in), &r); err != nil {
		t.Fatal(err)
	}
	if r.Extra["oldFlag"] != true {
		t.Errorf("oldFlag not preserved: %+v", r.Extra)
	}
	if _, ok := r.Extra["futureField"]; !ok {
		t.Errorf("futureField not preserved: %+v", r.Extra)
	}
	if _, ok := r.Extra["id"]; ok {
		t.Errorf("known field leaked into Extra: %+v", r.Extra)
	}
	out, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	var back map[string]any
	if err := json.Unmarshal(out, &back); err != nil {
		t.Fatal(err)
	}
	if back["oldFlag"] != true || back["id"] != "x" {
		t.Errorf("round-trip lost data: %s", out)
	}
	future, _ := back["futureField"].(map[string]any)
	if future["a"] != float64(1) {
		t.Errorf("futureField malformed after round-trip: %s", out)
	}
}

func TestEmptyKnownFieldsNotTreatedAsUnknown(t *testing.T) {
	// Explicit empty optional fields must not be copied into Extra.
	in := `{"id":"x","generation":0,"annotations":{},"conditions":{},"finalizers":[]}`
	var r api.Resource
	if err := json.Unmarshal([]byte(in), &r); err != nil {
		t.Fatal(err)
	}
	if len(r.Extra) != 0 {
		t.Errorf("empty known fields captured as unknown: %+v", r.Extra)
	}
}

func TestSpecHashStableAndDistinguishing(t *testing.T) {
	a := api.WidgetSpec{Image: "i", Replicas: 1}
	h1 := api.SpecHash(a)
	h2 := api.SpecHash(a)
	if h1 != h2 {
		t.Fatal("hash not stable for identical specs")
	}
	b := api.WidgetSpec{Image: "i", Replicas: 2}
	if h1 == api.SpecHash(b) {
		t.Fatal("hash identical for different specs")
	}
	if !strings.HasPrefix(h1, "sha256:") {
		t.Fatalf("unexpected hash format: %s", h1)
	}
	if !a.KeyFieldMissing() {
	}
	empty := api.WidgetSpec{}
	if !empty.KeyFieldMissing() {
		t.Fatal("empty image must be reported as missing key field")
	}
}

func TestConditionEqualityIgnoresCount(t *testing.T) {
	c1 := api.Condition{Type: "Ready", Status: "True", Reason: "R", Count: 1}
	c2 := c1
	c2.Count = 9
	if !c1.Equal(c2) {
		t.Fatal("conditions with same tuple must be equal across counts")
	}
	c3 := c1
	c3.Reason = "Other"
	if c1.Equal(c3) {
		t.Fatal("conditions with different reasons must differ")
	}
}

func TestErrorClassification(t *testing.T) {
	tr := &api.TransientError{Op: "x"}
	pm := &api.PermanentError{Op: "x"}
	cf := &api.ConflictError{ID: "x"}
	if !api.IsTransient(tr) || api.IsTransient(pm) || !api.IsTransient(cf) {
		t.Fatal("error classification wrong")
	}
	if _, ok := api.AsPermanent(pm); !ok {
		t.Fatal("permanent extraction failed")
	}
}
