// Package store is a local, concurrency-safe simulation of the managed
// resource store. No external system is involved.
package store

import (
	"encoding/json"
	"sync"

	"github.com/h3yow/declarative-resource-reconciler-go/pkg/api"
)

// Store is an in-memory resource database with optimistic concurrency:
// UpdateFunc only commits if resourceVersion still matches the value read.
//
// Resources are serialized through JSON on every read/write so that
// unknown-field preservation (Resource.Extra) is exercised exactly as it
// would be across a real API boundary.
type Store struct {
	mu   sync.Mutex
	data map[string]json.RawMessage
}

// New creates an empty store.
func New() *Store {
	return &Store{data: map[string]json.RawMessage{}}
}

func (s *Store) decode(id string) (api.Resource, bool, error) {
	raw, ok := s.data[id]
	if !ok {
		return api.Resource{}, false, nil
	}
	var r api.Resource
	if err := json.Unmarshal(raw, &r); err != nil {
		return api.Resource{}, false, err
	}
	return r, true, nil
}

func (s *Store) encodeLocked(r api.Resource) (json.RawMessage, error) {
	r.ResourceVersion++
	b, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(b), nil
}

// Get returns a snapshot copy. The bool is false when absent.
func (s *Store) Get(id string) (api.Resource, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok, err := s.decode(id)
	if err != nil || !ok {
		return r, ok, err
	}
	return r.Snapshot(), true, nil
}

// List returns snapshot copies of all stored resources.
func (s *Store) List() ([]api.Resource, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]api.Resource, 0, len(s.data))
	for id := range s.data {
		r, _, err := s.decode(id)
		if err != nil {
			return nil, err
		}
		out = append(out, r.Snapshot())
	}
	return out, nil
}

// Put creates or replaces a resource unconditionally, preserving the
// resource-version lineage. It is intended for tests and for simulating
// *external* mutations (drift).
func (s *Store) Put(r api.Resource) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cur, ok, err := s.decode(r.ID); err == nil && ok {
		r.ResourceVersion = cur.ResourceVersion
	} else {
		// Absent or corrupt: an unconditional put is also the repair path.
		r.ResourceVersion = 0
	}
	raw, err := s.encodeLocked(r)
	if err != nil {
		return err
	}
	s.data[r.ID] = raw
	return nil
}

// DeleteForTest unconditionally removes a resource (external deletion sim).
func (s *Store) DeleteForTest(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data[id]; !ok {
		return false
	}
	delete(s.data, id)
	return true
}

// Create commits r only if its id does not exist yet.
func (s *Store) Create(r api.Resource) (api.Resource, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data[r.ID]; ok {
		return api.Resource{}, &api.ConflictError{ID: r.ID}
	}
	r.ResourceVersion = 0
	raw, err := s.encodeLocked(r)
	if err != nil {
		return api.Resource{}, err
	}
	s.data[r.ID] = raw
	var saved api.Resource
	if err := json.Unmarshal(raw, &saved); err != nil {
		return api.Resource{}, err
	}
	return saved.Snapshot(), nil
}

// UpdateFunc applies fn to the current resource and commits only when the
// resourceVersion seen inside fn matches at commit time. Returning
// save=false leaves state untouched and returns the current snapshot.
//
// The optimistic check detects external mutations happening between read
// and commit: such races return api.ConflictError (transient) and the
// caller re-runs convergence against fresh observed state.
func (s *Store) UpdateFunc(id string, fn func(cur *api.Resource) (save bool, err error)) (api.Resource, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok, err := s.decode(id)
	if err != nil {
		return api.Resource{}, err
	}
	if !ok {
		return api.Resource{}, &api.ConflictError{ID: id}
	}
	before := r.ResourceVersion
	save, err := fn(&r)
	if err != nil {
		return api.Resource{}, err
	}
	if !save {
		return r.Snapshot(), nil
	}
	cur, ok, err := s.decode(id)
	if err != nil {
		return api.Resource{}, err
	}
	if !ok || cur.ResourceVersion != before {
		return api.Resource{}, &api.ConflictError{ID: id}
	}
	raw, err := s.encodeLocked(r)
	if err != nil {
		return api.Resource{}, err
	}
	s.data[id] = raw
	var saved api.Resource
	if err := json.Unmarshal(raw, &saved); err != nil {
		return api.Resource{}, err
	}
	return saved.Snapshot(), nil
}

// Remove deletes a resource unconditionally. Used by the reconciler after
// cleanup has completed; external-deletion tests may also use it.
func (s *Store) Remove(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data[id]; !ok {
		return false
	}
	delete(s.data, id)
	return true
}

// CorruptForTest replaces the persisted bytes for id with invalid JSON. It
// simulates on-disk/peer corruption so tests can prove decode failures are
// surfaced as retryable errors instead of crashing reconciliation.
func (s *Store) CorruptForTest(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[id] = json.RawMessage("{not-valid-json")
}
