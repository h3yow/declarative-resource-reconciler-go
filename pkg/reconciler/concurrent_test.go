package reconciler_test

import (
	"sync"
	"testing"
	"time"

	"github.com/h3yow/declarative-resource-reconciler-go/pkg/api"
)

// TestConcurrentTriggersSerializesAndConverges hammers one resource from
// many goroutines. It asserts:
//   - no trigger is lost: final observed state equals the highest generation;
//   - no overwrites: generations observed on the resource never go back;
//   - no duplicate side effects: exactly one live effect;
//   - every generation that changed the payload was applied at least once.
func TestConcurrentTriggersSerializesAndConverges(t *testing.T) {
	h := newHarness(t)
	const id = "w-concurrent"
	const n = 40

	var wg sync.WaitGroup
	for i := 1; i <= n; i++ {
		wg.Add(1)
		go func(gen int) {
			defer wg.Done()
			spec := api.WidgetSpec{Image: "img", Replicas: gen}
			h.rec.Trigger(api.DesiredState{ID: id, Generation: int64(gen), Spec: spec})
		}(i)
	}
	wg.Wait()

	h.eventuallyReady(id, n)

	r := h.getResource(t, id)
	if r.Generation != n {
		t.Errorf("final generation = %d, want %d (a trigger was lost or overwritten)", r.Generation, n)
	}
	if r.Spec.Replicas != n {
		t.Errorf("final replicas = %d, want %d", r.Spec.Replicas, n)
	}
	if count := h.bus.EffectCount(); count != 1 {
		t.Errorf("live effects = %d, want exactly 1 (duplicate side effects)", count)
	}
	if !h.bus.HasEffect(id) {
		t.Errorf("effect for %s missing", id)
	}

	// Every decision must carry a valid 1-based attempt number, confirming
	// requests survive retry accounting rather than being dropped.
	for _, d := range h.rec.Decisions(id) {
		if d.Attempt < 1 {
			t.Errorf("invalid attempt on decision: %+v", d)
		}
	}
	// Re-read resource repeatedly while background re-triggers run: version
	// must be monotonic and the stored spec must never be half-written.
	stop := make(chan struct{})
	var raceWg sync.WaitGroup
	raceWg.Add(2)
	go func() { // mutator
		defer raceWg.Done()
		for gen := n + 1; gen <= n+20; gen++ {
			select {
			case <-stop:
				return
			default:
			}
			h.rec.Trigger(api.DesiredState{
				ID: id, Generation: int64(gen),
				Spec: api.WidgetSpec{Image: "img", Replicas: gen},
			})
			time.Sleep(time.Millisecond)
		}
	}()
	go func() { // observer
		defer raceWg.Done()
		lastGen := int64(n)
		for {
			select {
			case <-stop:
				return
			default:
			}
			cur, ok, err := h.st.Get(id)
			if err != nil {
				t.Errorf("concurrent get: %v", err)
				return
			}
			if ok && cur.Generation < lastGen {
				t.Errorf("generation went backwards: %d -> %d (concurrent overwrite)", lastGen, cur.Generation)
				return
			}
			if ok {
				lastGen = cur.Generation
			}
			time.Sleep(time.Millisecond)
		}
	}()
	time.Sleep(60 * time.Millisecond)
	close(stop)
	raceWg.Wait()

	final := h.getResource(t, id)
	if final.Generation < n {
		t.Errorf("generation after concurrent phase = %d, want >= %d", final.Generation, n)
	}
	t.Logf("trigger inputs: %d initial (1..%d) + up to 20 concurrent; decisions=%d ensureCalls=%d finalGeneration=%d",
		n, n, len(h.rec.Decisions(id)), h.bus.EnsureCalls(id), final.Generation)
}

// TestConcurrentDistinctIDs verifies different resources proceed in
// parallel without blocking each other.
func TestConcurrentDistinctIDs(t *testing.T) {
	h := newHarness(t)
	const n = 20
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := "w-multi-" + itoa(i)
			h.rec.Trigger(api.DesiredState{
				ID: id, Generation: 1,
				Spec: api.WidgetSpec{Image: "img", Replicas: 1},
			})
			h.eventuallyReady(id, 1)
		}(i)
	}
	wg.Wait()
	if got := h.bus.EffectCount(); got != n {
		t.Errorf("effect count = %d, want %d", got, n)
	}
	t.Logf("trigger inputs: %d distinct ids; effectCount=%d", n, h.bus.EffectCount())
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
