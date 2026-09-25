package store_test

import (
	"errors"
	"testing"

	"github.com/h3yow/declarative-resource-reconciler-go/pkg/api"
	"github.com/h3yow/declarative-resource-reconciler-go/pkg/store"
)

func TestCreateConflictAndUpdateCAS(t *testing.T) {
	s := store.New()
	r := api.Resource{ID: "a"}
	saved, err := s.Create(r)
	if err != nil {
		t.Fatal(err)
	}
	if saved.ResourceVersion != 1 {
		t.Errorf("initial version=%d want 1", saved.ResourceVersion)
	}
	if _, err := s.Create(r); !errors.Is(err, err) || !api.IsTransient(err) {
		t.Errorf("duplicate create should be transient conflict, got %v", err)
	}

	// Successful CAS bumps version.
	got, err := s.UpdateFunc("a", func(cur *api.Resource) (bool, error) {
		cur.Generation = 5
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Generation != 5 || got.ResourceVersion != 2 {
		t.Errorf("CAS result wrong: gen=%d ver=%d", got.Generation, got.ResourceVersion)
	}

	// save=false must not bump version.
	before, _, _ := s.Get("a")
	if _, err := s.UpdateFunc("a", func(cur *api.Resource) (bool, error) {
		cur.Generation = 99
		return false, nil
	}); err != nil {
		t.Fatal(err)
	}
	after, _, _ := s.Get("a")
	if after.ResourceVersion != before.ResourceVersion || after.Generation != 5 {
		t.Errorf("save=false unexpectedly mutated state: %+v", after)
	}
}

func TestCorruptForTest(t *testing.T) {
	s := store.New()
	if err := s.Put(api.Resource{ID: "a"}); err != nil {
		t.Fatal(err)
	}
	s.CorruptForTest("a")
	if _, _, err := s.Get("a"); err == nil {
		t.Fatal("expected decode error after corruption")
	}
	// Put repairs corruption.
	if err := s.Put(api.Resource{ID: "a", Generation: 9}); err != nil {
		t.Fatal(err)
	}
	r, ok, err := s.Get("a")
	if err != nil || !ok || r.Generation != 9 {
		t.Fatalf("repair failed: %+v %v %v", r, ok, err)
	}
}
