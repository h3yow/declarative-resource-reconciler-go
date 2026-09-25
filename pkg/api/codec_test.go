package api

import "testing"

func TestDecodePreservesUnknownFields(t *testing.T) {
	// An object carrying a field this build does not know must neither
	// fail decoding nor lose the field on a round trip.
	raw := []byte(`{
		"kind":"Widget","apiVersion":"v1","name":"u",
		"spec":{"replicas":2,"image":"i:1"},
		"futurePolicy":{"enabled":true,"weight":9007199254740993},
		"annotations":{"a":"b"}
	}`)
	r, err := DecodeResource(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, ok := r.Unknown["futurePolicy"]; !ok {
		t.Fatalf("unknown field dropped: %#v", r.Unknown)
	}
	if _, ok := r.Unknown["annotations"]; !ok {
		t.Fatalf("unknown annotations dropped")
	}

	out, err := EncodeResource(r)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	r2, err := DecodeResource(out)
	if err != nil {
		t.Fatalf("round-trip decode: %v", err)
	}
	fp := r2.Unknown["futurePolicy"].(map[string]any)
	if got := fp["weight"].(interface{}); got == nil {
		t.Fatal("weight lost in round trip")
	}
	t.Logf("round-trip preserved unknown fields: %v", r2.Unknown)
}

func TestDecodeToleratesMissingNewFields(t *testing.T) {
	// An old-version object missing newer fields still decodes cleanly.
	raw := []byte(`{"kind":"Widget","name":"old"}`)
	r, err := DecodeResource(raw)
	if err != nil {
		t.Fatalf("decode old object: %v", err)
	}
	if r.Spec.Replicas != 0 || r.Spec.Image != "" {
		t.Fatalf("absent fields should be zero, got %+v", r.Spec)
	}
	if r.Kind != "Widget" || r.Name != "old" {
		t.Fatalf("known fields wrong: %+v", r)
	}
}

func TestDecodeMalformedReturnsError(t *testing.T) {
	if _, err := DecodeResource([]byte(`{not json`)); err == nil {
		t.Fatal("expected decode error for malformed JSON")
	}
}
