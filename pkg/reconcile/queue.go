// Package reconcile implements a keyed, per-resource serialized
// reconciliation loop with at-least-once delivery and exponential
// backoff retries.
package reconcile

import "sync"

// queuedItem pairs a key with its current retry attempt.
type queuedItem struct {
	key      string
	attempts int
}

// KeyedQueue guarantees:
//   - every Trigger(key) is eventually processed (no lost requests),
//   - a given key is processed by at most one worker at a time,
//   - a Trigger while the key is queued merges; a Trigger while the key
//     is being processed marks it dirty so the key is reprocessed once
//     after the in-flight pass finishes (classic dirty/working set).
type KeyedQueue struct {
	mu   sync.Mutex
	cond *sync.Cond

	queue []queuedItem
	dirty map[string]struct{} // enqueued and not yet taken by a worker
	work  map[string]struct{} // currently being processed
	// attempts holds the current retry count carried with a queued key.
	attempts map[string]int

	closed bool
}

// NewKeyedQueue constructs an empty queue.
func NewKeyedQueue() *KeyedQueue {
	q := &KeyedQueue{
		dirty:    map[string]struct{}{},
		work:     map[string]struct{}{},
		attempts: map[string]int{},
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// Trigger adds key to the queue. Never blocks; safe for concurrent use.
func (q *KeyedQueue) Trigger(key string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	if _, ok := q.dirty[key]; ok {
		return // already queued: coalesce
	}
	q.dirty[key] = struct{}{}
	if _, processing := q.work[key]; processing {
		// Don't append now; Done will requeue so the in-flight pass
		// cannot swallow this trigger.
		return
	}
	q.queue = append(q.queue, queuedItem{key: key, attempts: q.attempts[key]})
	q.cond.Signal()
}

// Get blocks until an item is available, then marks it in-flight.
// The boolean is false after Close with no remaining items.
func (q *KeyedQueue) Get() (string, int, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.queue) == 0 && !q.closed {
		q.cond.Wait()
	}
	if len(q.queue) == 0 {
		return "", 0, false
	}
	item := q.queue[0]
	q.queue = q.queue[1:]
	delete(q.dirty, item.key)
	q.work[item.key] = struct{}{}
	return item.key, item.attempts, true
}

// Done must be called after processing every item taken via Get.
// When requeue is true the key is re-enqueued carrying attempts+1
// (transient-failure retry); when false the retry counter is reset and
// any trigger that arrived during processing is released for one more
// pass.
func (q *KeyedQueue) Done(key string, attempts int, requeue bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.work, key)

	switch {
	case requeue:
		next := attempts + 1
		q.attempts[key] = next
		// Consume any dirty trigger into this single re-enqueue.
		delete(q.dirty, key)
		q.dirty[key] = struct{}{}
		q.queue = append(q.queue, queuedItem{key: key, attempts: next})
		q.cond.Signal()
	default:
		delete(q.attempts, key)
		if _, dirty := q.dirty[key]; dirty {
			// A concurrent Trigger arrived mid-pass: process once more
			// so it is never lost.
			q.queue = append(q.queue, queuedItem{key: key, attempts: 0})
			q.cond.Signal()
		}
	}
}

// Close wakes all workers; queued items are drained first.
func (q *KeyedQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.cond.Broadcast()
}

// Len returns the number of items waiting to be taken.
func (q *KeyedQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.queue)
}
