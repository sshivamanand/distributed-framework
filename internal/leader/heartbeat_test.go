package leader

import (
	"context"
	"testing"
	"time"

	"github.com/sshivamanand/distributed-task-framework/internal/registry"
	"github.com/sshivamanand/distributed-task-framework/internal/task"
)

// TestReapStaleWorkers_RequeuesInFlightTask exercises the slow-path
// failure detector directly (package-internal, so it can poke at
// inFlight/conns state without a real TCP connection): a worker with a
// task in flight goes quiet past HeartbeatTimeout, and the task should
// come back off the shared queue for redispatch.
func TestReapStaleWorkers_RequeuesInFlightTask(t *testing.T) {
	queue := task.NewQueue(4)
	results := task.NewResultStore()
	s := NewServer(queue, results)
	s.HeartbeatTimeout = 30 * time.Millisecond

	staleTime := time.Now().Add(-time.Hour) // already older than any timeout
	s.Registry.Register("w1", "127.0.0.1:9001", staleTime)

	t1 := task.Task{ID: "t1", Command: "echo", Args: []string{"hi"}}
	s.mu.Lock()
	s.inFlight["t1"] = inFlightAssignment{Task: t1, WorkerID: "w1"}
	s.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.reapStaleWorkers(ctx)

	w, ok := s.Registry.Get("w1")
	if !ok || w.Status != registry.StatusDead {
		t.Fatalf("worker status = %+v (ok=%v), want DEAD", w, ok)
	}

	select {
	case got := <-queue.Tasks():
		if got.ID != "t1" {
			t.Errorf("requeued task ID = %s, want t1", got.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for stranded task to be requeued")
	}

	s.mu.Lock()
	_, stillInFlight := s.inFlight["t1"]
	s.mu.Unlock()
	if stillInFlight {
		t.Error("task should have been removed from inFlight once requeued")
	}
}

// TestReapStaleWorkers_LeavesFreshWorkersAlone makes sure the monitor
// only acts on workers that have actually gone quiet.
func TestReapStaleWorkers_LeavesFreshWorkersAlone(t *testing.T) {
	queue := task.NewQueue(4)
	results := task.NewResultStore()
	s := NewServer(queue, results)
	s.HeartbeatTimeout = time.Hour

	s.Registry.Register("w1", "127.0.0.1:9001", time.Now())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.reapStaleWorkers(ctx)

	w, ok := s.Registry.Get("w1")
	if !ok || w.Status != registry.StatusAlive {
		t.Fatalf("worker status = %+v (ok=%v), want ALIVE", w, ok)
	}
}

// TestHeartbeatMonitor_RunsUntilContextCancelled checks the monitor
// goroutine itself starts, ticks, and shuts down cleanly.
func TestHeartbeatMonitor_RunsUntilContextCancelled(t *testing.T) {
	queue := task.NewQueue(1)
	results := task.NewResultStore()
	s := NewServer(queue, results)
	s.HeartbeatTimeout = 20 * time.Millisecond
	s.HeartbeatCheckInterval = 5 * time.Millisecond

	s.Registry.Register("w1", "127.0.0.1:9001", time.Now())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.heartbeatMonitor(ctx)
		close(done)
	}()

	// Let it tick a few times without a task in flight; nothing should
	// panic or block, and cancellation should still shut it down promptly.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("heartbeatMonitor did not return after context cancellation")
	}
}
