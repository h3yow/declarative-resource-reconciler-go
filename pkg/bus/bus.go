// Package bus simulates the external world: side effects (created objects
// in a remote system) and their cleanup. Everything is in-process and
// deterministic; callers inject failures to exercise retry classification.
package bus

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/h3yow/declarative-resource-reconciler-go/pkg/api"
)

// FailRule injects a deterministic failure for a specific resource id.
type FailRule struct {
	// TransientAttempts: number of initial Ensure calls that fail
	// transiently before succeeding.
	TransientAttempts int
	// Permanent: every Ensure call fails permanently; reconciliation must
	// stop retrying and record a stable condition.
	Permanent bool
	// CleanupTransientAttempts: number of initial Cleanup calls that fail
	// transiently before cleanup finally succeeds.
	CleanupTransientAttempts int
}

// Bus is a thread-safe registry of simulated external effects. Even though
// reconciliation serializes per id, counters and the effect set must be
// safe because tests read them concurrently.
type Bus struct {
	mu        sync.Mutex
	effects   map[string]string
	failRules map[string]FailRule
	attempts  map[string]int
	cleanups  map[string]int

	ensureCalls  int64
	cleanupCalls int64
}

// New creates an empty effect bus.
func New() *Bus {
	return &Bus{
		effects:   map[string]string{},
		failRules: map[string]FailRule{},
		attempts:  map[string]int{},
		cleanups:  map[string]int{},
	}
}

// SetFailRule installs a deterministic failure injection rule.
func (b *Bus) SetFailRule(id string, rule FailRule) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failRules[id] = rule
}

// Ensure creates or idempotently adopts the external effect for id and
// returns its stable reference. Repeated calls for an existing effect do
// not create duplicates: this is the external-world guarantee that backs
// reconciler idempotence.
func (b *Bus) Ensure(id string, spec api.WidgetSpec) (string, error) {
	atomic.AddInt64(&b.ensureCalls, 1)
	b.mu.Lock()
	defer b.mu.Unlock()
	n := b.attempts[id]
	b.attempts[id] = n + 1
	rule := b.failRules[id]
	if rule.Permanent {
		return "", &api.PermanentError{
			Op:      "Ensure",
			Reason:  api.ReasonPermanentFailure,
			Message: fmt.Sprintf("effect %s rejected by simulated external policy", id),
		}
	}
	// Failures only occur while the effect does not yet exist; once an
	// earlier call has actually created it, later calls adopt it without
	// error even if a (now stale) rule remains installed.
	if _, exists := b.effects[id]; !exists && n < rule.TransientAttempts {
		return "", &api.TransientError{
			Op:      "Ensure",
			Message: fmt.Sprintf("simulated transient unavailability (attempt %d/%d)", n+1, rule.TransientAttempts),
		}
	}
	if ref, exists := b.effects[id]; exists {
		return ref, nil
	}
	if spec.KeyFieldMissing() {
		return "", &api.PermanentError{
			Op:      "Ensure",
			Reason:  api.ReasonMissingFieldRebuilt,
			Message: "cannot create effect without required field image",
		}
	}
	ref := "effect://" + id
	b.effects[id] = ref
	return ref, nil
}

// Cleanup releases the external effect. It is idempotent: a missing effect
// is treated as already cleaned. Transient failure injection applies only
// while the effect still exists.
func (b *Bus) Cleanup(id string) error {
	atomic.AddInt64(&b.cleanupCalls, 1)
	b.mu.Lock()
	defer b.mu.Unlock()
	n := b.cleanups[id]
	b.cleanups[id] = n + 1
	if _, exists := b.effects[id]; !exists {
		return nil
	}
	rule := b.failRules[id]
	if n < rule.CleanupTransientAttempts {
		return &api.TransientError{
			Op:      "Cleanup",
			Message: fmt.Sprintf("simulated cleanup unavailability (attempt %d/%d)", n+1, rule.CleanupTransientAttempts),
		}
	}
	delete(b.effects, id)
	return nil
}

// HasEffect reports whether an external effect currently exists.
func (b *Bus) HasEffect(id string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, ok := b.effects[id]
	return ok
}

// EffectCount returns the number of live external effects.
func (b *Bus) EffectCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.effects)
}

// EnsureCalls returns the total number of Ensure invocations for id,
// including failed and idempotent re-adopt calls.
func (b *Bus) EnsureCalls(id string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.attempts[id]
}

// CleanupCalls returns the total number of Cleanup invocations for id.
func (b *Bus) CleanupCalls(id string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.cleanups[id]
}

// TotalEnsureCalls / TotalCleanupCalls expose process-wide counters.
func (b *Bus) TotalEnsureCalls() int64  { return atomic.LoadInt64(&b.ensureCalls) }
func (b *Bus) TotalCleanupCalls() int64 { return atomic.LoadInt64(&b.cleanupCalls) }
