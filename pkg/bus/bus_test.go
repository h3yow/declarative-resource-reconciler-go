package bus_test

import (
	"testing"

	"github.com/h3yow/declarative-resource-reconciler-go/pkg/api"
	"github.com/h3yow/declarative-resource-reconciler-go/pkg/bus"
)

func TestEnsureIdempotent(t *testing.T) {
	b := bus.New()
	spec := api.WidgetSpec{Image: "i", Replicas: 1}
	ref1, err := b.Ensure("a", spec)
	if err != nil {
		t.Fatal(err)
	}
	ref2, err := b.Ensure("a", spec)
	if err != nil {
		t.Fatal(err)
	}
	if ref1 != ref2 {
		t.Errorf("references differ: %s vs %s", ref1, ref2)
	}
	if b.EffectCount() != 1 {
		t.Errorf("count=%d want 1", b.EffectCount())
	}
	if b.EnsureCalls("a") != 2 {
		t.Errorf("calls accounting wrong: %d", b.EnsureCalls("a"))
	}
}

func TestEnsureFailureClassification(t *testing.T) {
	b := bus.New()
	b.SetFailRule("a", bus.FailRule{TransientAttempts: 2})
	spec := api.WidgetSpec{Image: "i", Replicas: 1}
	for i := 0; i < 2; i++ {
		if _, err := b.Ensure("a", spec); !api.IsTransient(err) {
			t.Fatalf("call %d: want transient, got %v", i, err)
		}
	}
	if _, err := b.Ensure("a", spec); err != nil {
		t.Fatalf("third call should succeed: %v", err)
	}
	if !b.HasEffect("a") {
		t.Fatal("effect missing after recovery")
	}

	b2 := bus.New()
	b2.SetFailRule("p", bus.FailRule{Permanent: true})
	_, err := b2.Ensure("p", spec)
	if _, ok := api.AsPermanent(err); !ok {
		t.Fatalf("want permanent error, got %v", err)
	}
}

func TestCleanupIdempotentAndTransient(t *testing.T) {
	b := bus.New()
	b.SetFailRule("a", bus.FailRule{CleanupTransientAttempts: 2})
	// Cleanup of a never-existing effect is idempotent nil regardless of
	// any installed rule.
	if err := b.Cleanup("absent"); err != nil {
		t.Errorf("cleanup of absent effect should be nil, got %v", err)
	}
	if _, err := b.Ensure("a", api.WidgetSpec{Image: "i"}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := b.Cleanup("a"); !api.IsTransient(err) {
			t.Fatalf("cleanup %d: want transient, got %v", i, err)
		}
		if !b.HasEffect("a") {
			t.Fatal("effect must survive failed cleanup")
		}
	}
	if err := b.Cleanup("a"); err != nil {
		t.Fatalf("cleanup should succeed: %v", err)
	}
	if b.HasEffect("a") {
		t.Fatal("effect should be gone after successful cleanup")
	}
	if err := b.Cleanup("a"); err != nil {
		t.Errorf("repeat cleanup must be idempotent nil, got %v", err)
	}
}
