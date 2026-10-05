// Package state provides the local, in-memory simulation of the
// "outside world": desired resources, the live managed objects the
// reconciler creates, an external side-effect ledger, and a small
// fault-injection point used by tests.
package state

import (
	"reflect"
	"sync"

	"github.com/h3yow/declarative-resource-reconciler-go/pkg/api"
)

// LiveObject is the simulated managed resource (observed state).
type LiveObject struct {
	Replicas int
	Image    string
	Token    string
	// Missing simulates the managed object having been deleted externally.
	Missing bool
	// ExternalTag is set by a simulated external mutator; the reconciler
	// never writes it, so its presence proves an out-of-band change.
	ExternalTag string
}

// Faults configures injected failures for a given resource key.
type Faults struct {
	// TransientTimes makes the first N writes fail transiently, then heal.
	TransientTimes int
	// CleanupTimes makes the first N cleanup attempts fail transiently.
	CleanupTimes int
	// Permanent forces every write to fail permanently.
	Permanent bool
}

// Counters records externally observable side effects.
type Counters struct {
	Creates   int
	Updates   int
	Deletes   int
	SideCalls int // external side-effect invocations
}

// Store simulates persistence and the external environment. All access
// is mutex-protected so tests can trigger reconciliation concurrently
// while mutating observed state.
type Store struct {
	mu sync.Mutex

	desired map[string]*api.Resource
	live    map[string]*LiveObject
	faults  map[string]Faults

	counters Counters
}

// NewStore constructs an empty store.
func NewStore() *Store {
	return &Store{
		desired: map[string]*api.Resource{},
		live:    map[string]*LiveObject{},
		faults:  map[string]Faults{},
	}
}

// cloneResource returns a deep copy so callers can mutate slices/maps
// (conditions, finalizers, unknown fields) without aliasing stored state.
func cloneResource(r *api.Resource) *api.Resource {
	cp := *r
	if r.Finalizers != nil {
		cp.Finalizers = append([]string(nil), r.Finalizers...)
	}
	if r.Unknown != nil {
		cp.Unknown = make(map[string]any, len(r.Unknown))
		for k, v := range r.Unknown {
			cp.Unknown[k] = v
		}
	}
	if r.Status.Conditions != nil {
		cp.Status.Conditions = append([]api.Condition(nil), r.Status.Conditions...)
	}
	if r.Status.UnknownFields != nil {
		cp.Status.UnknownFields = make(map[string]string, len(r.Status.UnknownFields))
		for k, v := range r.Status.UnknownFields {
			cp.Status.UnknownFields[k] = v
		}
	}
	return &cp
}

// Desired-side accessors -------------------------------------------------

// PutDesired stores a copy of r as desired state and bumps generation
// when the spec of an existing resource changes.
func (s *Store) PutDesired(r *api.Resource) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if old, ok := s.desired[r.Key()]; ok && !reflect.DeepEqual(old.Spec, r.Spec) {
		r.Generation = old.Generation + 1
	} else if r.Generation == 0 {
		r.Generation = 1
	}
	s.desired[r.Key()] = cloneResource(r)
}

func (s *Store) GetDesired(key string) (*api.Resource, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.desired[key]
	if !ok {
		return nil, false
	}
	return cloneResource(r), true
}

// DeleteDesired removes the desired record outright (only called once
// finalizer cleanup has finished).
func (s *Store) DeleteDesired(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.desired, key)
}

func (s *Store) ListDesired() []*api.Resource {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*api.Resource, 0, len(s.desired))
	for _, r := range s.desired {
		out = append(out, cloneResource(r))
	}
	return out
}

// UpdateDesired persists a mutated desired resource (status/finalizers).
func (s *Store) UpdateDesired(r *api.Resource) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.desired[r.Key()] = cloneResource(r)
}

// Live-object accessors (simulated observed state) ----------------------

func (s *Store) GetLive(key string) (*LiveObject, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	obj, ok := s.live[key]
	if !ok || obj.Missing {
		return nil, false
	}
	cp := *obj
	return &cp, true
}

func (s *Store) PutLive(key string, obj *LiveObject) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *obj
	s.live[key] = &cp
}

func (s *Store) DeleteLive(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.live, key)
}

// ExternalMutate models an out-of-band change to the live object.
func (s *Store) ExternalMutate(key string, mutate func(o *LiveObject)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if obj, ok := s.live[key]; ok {
		mutate(obj)
	}
}

// Side effects / faults --------------------------------------------------

// CallSideEffect invokes the simulated external side effect. It is a
// counted external action: the reconciler is responsible for calling it
// at most once per SideEffectToken value; the store itself never dedups.
func (s *Store) CallSideEffect(key, token string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.counters.SideCalls++
}

// ApplyWrite creates or updates the live object to match want, honoring
// injected faults. It distinguishes create from update for the ledger.
func (s *Store) ApplyWrite(key string, want *LiveObject) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	f := s.faults[key]
	if f.Permanent {
		return api.Permanent(api.ReasonPermanentFailure,
			"injected permanent write failure for "+key)
	}
	if f.TransientTimes > 0 {
		f.TransientTimes--
		s.faults[key] = f
		return api.Transient(api.ReasonTransientFailure,
			"injected transient write failure for "+key)
	}

	// Count the side effect only after fault checks pass: a failed
	// attempt produced no external change and must not be counted.
	// An object flagged Missing (externally deleted but still tracked)
	// is a recreate and counts as a Create, not an Update.
	prev, tracked := s.live[key]
	if !tracked || prev.Missing {
		s.counters.Creates++
	} else {
		s.counters.Updates++
	}
	cp := *want
	cp.ExternalTag = "" // our own writes remove evidence of external drift
	s.live[key] = &cp
	return nil
}

// Cleanup simulates finalizer cleanup (releasing external resources).
// It only succeeds after CleanupTimes transient failures are consumed;
// on success the live object and ledger delete are finalized.
func (s *Store) Cleanup(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	f := s.faults[key]
	if f.CleanupTimes > 0 {
		f.CleanupTimes--
		s.faults[key] = f
		return api.Transient(api.ReasonTransientFailure,
			"injected transient cleanup failure for "+key)
	}
	if _, ok := s.live[key]; ok {
		s.counters.Deletes++
	}
	delete(s.live, key)
	return nil
}

func (s *Store) SetFaults(key string, f Faults) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.faults[key] = f
}

func (s *Store) SnapshotFaults(key string) Faults {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.faults[key]
}

func (s *Store) Counters() Counters {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.counters
}
