package reconciler_test

import (
	"testing"
	"time"

	"github.com/h3yow/declarative-resource-reconciler-go/pkg/api"
	"github.com/h3yow/declarative-resource-reconciler-go/pkg/bus"
)

// TestDeleteWaitsForCleanup asserts the resource is retained while cleanup
// is transiently failing, retried with backoff, and removed only after the
// external effect is actually gone.
func TestDeleteWaitsForCleanup(t *testing.T) {
	h := newHarness(t)
	const id = "w-del"
	h.bus.SetFailRule(id, bus.FailRule{CleanupTransientAttempts: 3})

	h.rec.Trigger(api.DesiredState{
		ID: id, Generation: 1,
		Spec: api.WidgetSpec{Image: "img", Replicas: 1},
	})
	h.eventuallyReady(id, 1)

	h.rec.Delete(api.DeleteRequest{ID: id, Generation: 2})

	// During cleanup retries the resource must remain present and the
	// deleting condition must be distinguishable from create-side retries.
	sawDeletingRetry := false
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		r, exists, err := h.st.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		if exists {
			// Once any Deleting condition is observable, the deletion
			// marker must have been committed in the same write.
			if _, hasDeleting := r.Conditions[api.ConditionDeleting]; hasDeleting {
				if r.DeletionTimestamp == nil {
					t.Errorf("deleting condition present but deletion timestamp not set")
				}
			}
			if c := r.Conditions[api.ConditionDeleting]; c.Reason == api.ReasonCleanupTransient {
				sawDeletingRetry = true
			}
		}
		_, stillExists, _ := h.st.Get(id)
		if !stillExists {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	h.eventuallyAbsent(id)
	if !sawDeletingRetry {
		t.Errorf("never observed Deleting=False/%s condition while cleanup was failing", api.ReasonCleanupTransient)
	}
	if h.bus.HasEffect(id) {
		t.Errorf("resource removed but external effect still exists")
	}
	if calls := h.bus.CleanupCalls(id); calls != 4 {
		t.Errorf("CleanupCalls = %d, want 4 (3 transient + 1 success)", calls)
	}
	for _, d := range h.rec.Decisions(id) {
		if d.Action == api.ActionDelete {
			if d.Reason != api.ReasonDeleted {
				t.Errorf("delete decision reason = %s, want %s", d.Reason, api.ReasonDeleted)
			}
		}
	}
	t.Logf("trigger inputs: create gen1, delete gen2; cleanup transient failures=3; cleanupCalls=%d; resource released only after effect gone; decisions:",
		h.bus.CleanupCalls(id))
	h.rec.DecisionsText(&logWriter{t}, id)
}

// TestDeleteNeverReadyResource removes a resource that never acquired its
// effect without calling cleanup, using a distinguishable reason.
func TestDeleteNeverReadyResource(t *testing.T) {
	h := newHarness(t)
	const id = "w-del-missing"
	// Missing key field -> resource exists but no effect/finalizer.
	h.rec.Trigger(api.DesiredState{
		ID: id, Generation: 1,
		Spec: api.WidgetSpec{Image: "", Replicas: 1},
	})
	waitFor(2*time.Second, func() bool {
		r, ok, err := h.st.Get(id)
		return err == nil && ok &&
			r.Conditions[api.ConditionReady].Reason == api.ReasonMissingFieldRebuilt
	})

	h.rec.Delete(api.DeleteRequest{ID: id, Generation: 2})
	h.eventuallyAbsent(id)

	if calls := h.bus.CleanupCalls(id); calls != 0 {
		t.Errorf("CleanupCalls = %d, want 0 (no effect was ever created)", calls)
	}
	found := false
	for _, d := range h.rec.Decisions(id) {
		if d.Reason == api.ReasonDeletedBeforeReady {
			found = true
		}
	}
	if !found {
		t.Errorf("expected a %s decision", api.ReasonDeletedBeforeReady)
	}
	t.Logf("trigger inputs: create-with-missing-field gen1, delete gen2; cleanupCalls=0; reason=%s",
		api.ReasonDeletedBeforeReady)
}

// TestDeleteCancelledByNewTrigger verifies a fresh desired-state trigger
// cancels an in-flight deletion and reconverges the resource.
func TestDeleteCancelledByNewTrigger(t *testing.T) {
	h := newHarness(t)
	const id = "w-del-cancel"
	h.rec.Trigger(api.DesiredState{
		ID: id, Generation: 1,
		Spec: api.WidgetSpec{Image: "img", Replicas: 1},
	})
	h.eventuallyReady(id, 1)

	h.rec.Delete(api.DeleteRequest{ID: id, Generation: 2})
	// Immediately resubmit desired state at a higher generation.
	h.rec.Trigger(api.DesiredState{
		ID: id, Generation: 3,
		Spec: api.WidgetSpec{Image: "img", Replicas: 2},
	})

	// Either the delete completed (in which case it must be recreated) or it
	// was cancelled: in both cases the end state must converge to gen3.
	h.eventuallyReady(id, 2)
	r := h.getResource(t, id)
	if r.Generation != 3 {
		t.Errorf("generation = %d, want 3 after delete cancelled/overridden", r.Generation)
	}
	if !h.bus.HasEffect(id) {
		t.Errorf("effect should exist after reconvergence")
	}
	t.Logf("trigger inputs: create gen1, delete gen2, recreate gen3; finalGeneration=%d effectExists=true",
		r.Generation)
}
