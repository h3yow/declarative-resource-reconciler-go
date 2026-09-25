package reconciler_test

import (
	"bytes"
	"context"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/h3yow/declarative-resource-reconciler-go/pkg/api"
	"github.com/h3yow/declarative-resource-reconciler-go/pkg/bus"
	"github.com/h3yow/declarative-resource-reconciler-go/pkg/reconciler"
	"github.com/h3yow/declarative-resource-reconciler-go/pkg/store"
)

// fastBackoff keeps deterministic transient-retry tests quick.
func fastBackoff() api.BackoffConfig {
	return api.BackoffConfig{Initial: 2 * time.Millisecond, Max: 20 * time.Millisecond, Multiplier: 2}
}

// safeBuffer is a mutex-protected buffer: workers log while tests read.
type safeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *safeBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *safeBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

type harness struct {
	t      *testing.T
	st     *store.Store
	bus    *bus.Bus
	logBuf *safeBuffer
	rec    *reconciler.Reconciler
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	st := store.New()
	eb := bus.New()
	buf := &safeBuffer{}
	logger := log.New(buf, "", 0)
	rec := reconciler.New(st, eb, logger, fastBackoff())
	rec.Start(context.Background())
	h := &harness{t: t, st: st, bus: eb, logBuf: buf, rec: rec}
	t.Cleanup(func() {
		rec.Stop()
		t.Logf("--- reconciler log ---\n%s", buf.String())
	})
	return h
}

// waitFor polls cond until it returns true or the deadline expires.
func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return cond()
}

func (h *harness) getResource(t *testing.T, id string) api.Resource {
	t.Helper()
	r, ok, err := h.st.Get(id)
	if err != nil {
		t.Fatalf("get %s: %v", id, err)
	}
	if !ok {
		t.Fatalf("resource %s not found", id)
	}
	return r
}

func (h *harness) eventuallyReady(id string, wantReplicas int) {
	h.t.Helper()
	ok := waitFor(3*time.Second, func() bool {
		r, exists, err := h.st.Get(id)
		if err != nil || !exists {
			return false
		}
		c := r.Conditions[api.ConditionReady]
		return c.Status == api.StatusTrue && r.Spec.Replicas == wantReplicas
	})
	if !ok {
		h.t.Fatalf("resource %s never became Ready with replicas=%d", id, wantReplicas)
	}
}

func (h *harness) eventuallyAbsent(id string) {
	h.t.Helper()
	ok := waitFor(3*time.Second, func() bool {
		_, exists, err := h.st.Get(id)
		return err == nil && !exists
	})
	if !ok {
		h.t.Fatalf("resource %s was not removed", id)
	}
}
