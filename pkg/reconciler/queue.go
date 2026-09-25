package reconciler

import (
	"context"
	"sync"
	"time"
)

// keyQueue is the single-consumer, per-resource serialization primitive.
//
// It combines:
//   - a pending counter so concurrent triggers are never lost;
//   - a capacity-1 wake channel so every 0->1 transition wakes the one
//     worker exactly (additional triggers coalesce while one is pending).
//
// A stale (already-consumed) token can at worst cause one extra harmless
// reconcile pass; it can never cause a lost wakeup.
type keyQueue struct {
	mu      sync.Mutex
	pending int
	wake    chan struct{}
}

func newKeyQueue() *keyQueue {
	return &keyQueue{wake: make(chan struct{}, 1)}
}

// enqueue accounts for one trigger and wakes the worker when this is the
// first unprocessed trigger.
func (q *keyQueue) enqueue() {
	q.mu.Lock()
	first := q.pending == 0
	q.pending++
	q.mu.Unlock()
	if first {
		select {
		case q.wake <- struct{}{}:
		default:
			// A token is already buffered or about to be consumed; the
			// counter keeps this trigger from being lost.
		}
	}
}

// drainOne blocks until a trigger can be consumed or ctx ends. It must be
// called only when pending is known (or suspected) to be zero; the single
// worker calls it serially.
func (q *keyQueue) wait(ctx context.Context) bool {
	q.mu.Lock()
	ch := q.wake
	q.mu.Unlock()
	select {
	case <-ch:
		return true
	case <-ctx.Done():
		return false
	}
}

// waitWithBackoff blocks up to delay (retry pacing), returns early when a
// fresh trigger wakes the worker, and returns false when ctx ended.
func (q *keyQueue) waitWithBackoff(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	q.mu.Lock()
	ch := q.wake
	q.mu.Unlock()
	select {
	case <-ch:
		return true
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// pendingCount returns the number of triggers waiting to be processed.
func (q *keyQueue) pendingCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.pending
}

// consume decrements the pending count after a wakeup.
func (q *keyQueue) consume() {
	q.mu.Lock()
	if q.pending > 0 {
		q.pending--
	}
	q.mu.Unlock()
}
