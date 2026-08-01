package registry

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestRegistry_ConcurrentRegisterAndHeartbeat(t *testing.T) {
	r := New()
	const n = 50

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("worker-%d", i)
			r.Register(id, "127.0.0.1:0", time.Now())
			r.Heartbeat(id, time.Now())
		}(i)
	}
	wg.Wait()

	if got := len(r.Alive()); got != n {
		t.Fatalf("len(Alive()) = %d, want %d", got, n)
	}
}

func TestRegistry_HeartbeatUnknownWorkerReturnsFalse(t *testing.T) {
	r := New()
	if r.Heartbeat("ghost", time.Now()) {
		t.Fatal("expected false for unregistered worker")
	}
}

func TestRegistry_MarkDeadExcludesFromAlive(t *testing.T) {
	r := New()
	r.Register("w1", "127.0.0.1:9001", time.Now())
	r.Register("w2", "127.0.0.1:9002", time.Now())

	r.MarkDead("w1")

	alive := r.Alive()
	if len(alive) != 1 || alive[0].ID != "w2" {
		t.Fatalf("got %+v, want only w2", alive)
	}
}

func TestRegistry_HeartbeatRevivesDeadWorker(t *testing.T) {
	r := New()
	r.Register("w1", "127.0.0.1:9001", time.Now())
	r.MarkDead("w1")

	if !r.Heartbeat("w1", time.Now()) {
		t.Fatal("expected heartbeat to succeed for a known worker")
	}

	w, ok := r.Get("w1")
	if !ok || w.Status != StatusAlive {
		t.Fatalf("got %+v, want ALIVE", w)
	}
}
