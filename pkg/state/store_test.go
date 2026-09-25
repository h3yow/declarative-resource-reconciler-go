package state

import (
	"testing"

	"github.com/h3yow/declarative-resource-reconciler-go/pkg/api"
)

func TestGenerationBumpsOnSpecChange(t *testing.T) {
	s := NewStore()
	r := &api.Resource{Kind: "Widget", Name: "g", Spec: api.Spec{Replicas: 1}}
	s.PutDesired(r)
	got, _ := s.GetDesired("Widget/g")
	if got.Generation != 1 {
		t.Fatalf("initial generation=%d, want 1", got.Generation)
	}
	got.Spec.Replicas = 2
	s.PutDesired(got)
	got2, _ := s.GetDesired("Widget/g")
	if got2.Generation != 2 {
		t.Fatalf("generation after spec change=%d, want 2", got2.Generation)
	}
}

func TestDeepCopyPreventsAliasing(t *testing.T) {
	s := NewStore()
	s.PutDesired(&api.Resource{Kind: "Widget", Name: "z"})
	a, _ := s.GetDesired("Widget/z")
	b, _ := s.GetDesired("Widget/z")
	a.Finalizers = append(a.Finalizers, "x")
	if len(b.Finalizers) != 0 {
		t.Fatal("GetDesired returned aliased finalizers")
	}
}

func TestApplyWriteFailureIsNotCounted(t *testing.T) {
	s := NewStore()
	want := &LiveObject{Replicas: 1}

	s.SetFaults("Widget/f", Faults{TransientTimes: 1})
	if err := s.ApplyWrite("Widget/f", want); api.ClassOf(err) != api.ClassTransient {
		t.Fatalf("err=%v, want transient", err)
	}
	if c := s.Counters(); c.Creates != 0 {
		t.Fatalf("failed write counted as create: %+v", c)
	}
	if err := s.ApplyWrite("Widget/f", want); err != nil {
		t.Fatal(err)
	}
	if c := s.Counters(); c.Creates != 1 {
		t.Fatalf("successful write not counted: %+v", c)
	}

	// An externally-missing object is recreated, not updated.
	s.ExternalMutate("Widget/f", func(o *LiveObject) { o.Missing = true })
	if err := s.ApplyWrite("Widget/f", want); err != nil {
		t.Fatal(err)
	}
	c := s.Counters()
	if c.Creates != 2 || c.Updates != 0 {
		t.Fatalf("recreate counters=%+v, want Creates=2 Updates=0", c)
	}
}

func TestPermanentAndCleanupFaults(t *testing.T) {
	s := NewStore()
	s.PutLive("Widget/p", &LiveObject{})
	s.SetFaults("Widget/p", Faults{Permanent: true})
	if err := s.ApplyWrite("Widget/p", &LiveObject{}); api.ClassOf(err) != api.ClassPermanent {
		t.Fatalf("err=%v, want permanent", err)
	}

	s.SetFaults("Widget/c", Faults{CleanupTimes: 1})
	s.PutLive("Widget/c", &LiveObject{})
	if err := s.Cleanup("Widget/c"); api.ClassOf(err) != api.ClassTransient {
		t.Fatalf("cleanup err=%v, want transient", err)
	}
	if _, ok := s.GetLive("Widget/c"); !ok {
		t.Fatal("live object released before cleanup succeeded")
	}
	if err := s.Cleanup("Widget/c"); err != nil {
		t.Fatalf("second cleanup: %v", err)
	}
	if _, ok := s.GetLive("Widget/c"); ok {
		t.Fatal("live object still present after successful cleanup")
	}
	if c := s.Counters(); c.Deletes != 1 {
		t.Fatalf("Deletes=%d, want 1", c.Deletes)
	}
}
