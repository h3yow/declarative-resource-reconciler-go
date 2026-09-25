package reconcile

import (
	"sync"
	"testing"
	"time"
)

// TestQueueCoalescesDuplicateTriggers: repeated triggers while queued
// merge into one item, but a trigger arriving during processing forces
// exactly one additional pass (no lost request).
func TestQueueCoalescesDuplicateTriggers(t *testing.T) {
	q := NewKeyedQueue()

	for i := 0; i < 100; i++ {
		q.Trigger("k")
	}
	key, attempts, ok := q.Get()
	if !ok || key != "k" || attempts != 0 {
		t.Fatalf("Get = %q,%d,%v, want k,0,true", key, attempts, ok)
	}
	q.mu.Lock()
	if len(q.queue) != 0 {
		q.mu.Unlock()
		t.Fatalf("expected duplicate triggers to coalesce, queue=%v", q.queue)
	}
	q.mu.Unlock()

	// Trigger while in-flight: must not appear in queue yet.
	q.Trigger("k")
	q.mu.Lock()
	if len(q.queue) != 0 {
		q.mu.Unlock()
		t.Fatalf("in-flight trigger must not be deliverable concurrently, queue=%v", q.queue)
	}
	q.mu.Unlock()

	q.Done("k", 0, false)
	key, attempts, ok = q.Get()
	if !ok || key != "k" {
		t.Fatalf("in-flight trigger lost: Get=%q,%v", key, ok)
	}
	if attempts != 0 {
		t.Fatalf("re-pass attempts=%d, want 0", attempts)
	}
	q.Done("k", 0, false)
}

// TestQueuePerKeySerialization: a key cannot be handed to two consumers
// until Done, while a different key proceeds independently.
func TestQueuePerKeySerialization(t *testing.T) {
	q := NewKeyedQueue()
	q.Trigger("a")
	q.Trigger("b")

	key1, _, _ := q.Get()
	// Both a and b are queued; whichever was taken, re-triggering the
	// in-flight key must not make it Get-able again.
	q.Trigger(key1)
	q.Trigger(key1)
	got := ""
	if k2, _, ok := q.Get(); ok {
		got = k2
	}
	if got == key1 {
		t.Fatalf("in-flight key %q was handed out concurrently", key1)
	}
	if got == "" {
		t.Fatalf("independent key was blocked behind in-flight key")
	}
	q.Done(got, 0, false)
	q.Done(key1, 0, false)

	// The in-flight triggers should yield exactly one extra pass.
	k3, _, ok := q.Get()
	if !ok || k3 != key1 {
		t.Fatalf("expected one re-pass of %q, got %q,%v", key1, k3, ok)
	}
	q.Done(key1, 0, false)
}

// TestQueueRetriesCarryAttempts: requeue carries an incrementing attempt
// counter until the caller settles with a non-requeue Done.
func TestQueueRetriesCarryAttempts(t *testing.T) {
	q := NewKeyedQueue()
	q.Trigger("r")
	_, a, _ := q.Get()
	for want := 1; want <= 3; want++ {
		q.Done("r", a, true)
		_, a2, ok := q.Get()
		if !ok || a2 != want {
			t.Fatalf("retry %d: attempts=%d ok=%v", want, a2, ok)
		}
		a = a2
	}
	q.Done("r", a, false)
	// Settled: attempts reset on a fresh trigger.
	q.Trigger("r")
	_, fresh, _ := q.Get()
	if fresh != 0 {
		t.Fatalf("attempts after settle=%d, want 0", fresh)
	}
	q.Done("r", 0, false)
}

// TestQueueConcurrentTriggersNoLoss fires many triggers from many
// goroutines and asserts they coalesce into delivery (no loss), and that
// triggers landing while the item is in-flight force one more pass.
func TestQueueConcurrentTriggersNoLoss(t *testing.T) {
	q := NewKeyedQueue()
	const goroutines, perG = 16, 50
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				q.Trigger("shared")
			}
		}()
	}
	wg.Wait()

	key, _, ok := q.Get()
	if !ok || key != "shared" {
		t.Fatalf("first Get = %q,%v, want shared,true", key, ok)
	}

	// Triggers during processing: all must merge into a single re-pass.
	for g := 0; g < goroutines; g++ {
		q.Trigger("shared")
	}
	q.Done(key, 0, false)

	key2, a, ok := q.Get()
	if !ok || key2 != "shared" || a != 0 {
		t.Fatalf("in-flight triggers lost: Get=%q,%d,%v", key2, a, ok)
	}
	q.Done(key2, 0, false)

	// Nothing left: queue must stay empty (no phantom deliveries).
	time.Sleep(20 * time.Millisecond)
	if leftover := q.Len(); leftover != 0 {
		t.Fatalf("queue not empty after settling, leftover=%d", leftover)
	}
	t.Logf("trigger inputs=%d while queued + %d while in-flight; exactly 2 passes, no loss",
		goroutines*perG, goroutines)
}

// TestBackoff verifies exponential growth capped at MaxBackoff.
func TestBackoff(t *testing.T) {
	cfg := Config{BaseBackoff: 10, MaxBackoff: 100}
	want := []int64{10, 20, 40, 80, 100, 100}
	for i, w := range want {
		if got := int64(backoff(cfg, i+1)); got != w {
			t.Fatalf("backoff(%d)=%d, want %d", i+1, got, w)
		}
	}
}
