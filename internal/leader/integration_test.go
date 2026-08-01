package leader_test

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/sshivamanand/distributed-task-framework/internal/leader"
	"github.com/sshivamanand/distributed-task-framework/internal/task"
	"github.com/sshivamanand/distributed-task-framework/internal/worker"
)

// TestLeaderWorker_EndToEndTaskExecution exercises the full phase-2 loop
// over a real loopback TCP connection: a worker registers with a leader,
// the leader dispatches a queued task to it, the worker executes the task
// with its local (phase-1) worker pool, and reports the result back.
func TestLeaderWorker_EndToEndTaskExecution(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	queue := task.NewQueue(4)
	results := task.NewResultStore()
	srv := leader.NewServer(queue, results)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serveDone := make(chan error, 1)
	go func() { serveDone <- srv.Serve(ctx, ln) }()

	w := &worker.Client{ID: "w1", LeaderAddr: ln.Addr().String(), Concurrency: 2}
	workerDone := make(chan error, 1)
	go func() { workerDone <- w.Run(ctx) }()

	waitUntil(t, 2*time.Second, func() bool {
		return len(srv.Registry.Alive()) == 1
	})

	if err := queue.Submit(ctx, task.Task{ID: "t1", Command: "echo", Args: []string{"hello-from-worker"}}); err != nil {
		t.Fatalf("submit: %v", err)
	}

	var res task.Result
	waitUntil(t, 2*time.Second, func() bool {
		var ok bool
		res, ok = results.Get("t1")
		return ok
	})

	if res.Status != task.StatusCompleted {
		t.Fatalf("status = %s, want %s", res.Status, task.StatusCompleted)
	}
	if !strings.Contains(res.Output, "hello-from-worker") {
		t.Fatalf("output = %q, want to contain %q", res.Output, "hello-from-worker")
	}

	cancel()
	<-serveDone
	<-workerDone
}

// TestLeaderWorker_ReassignsTaskWhenWorkerConnectionDrops exercises the
// fast failure-detection path end to end: a worker registers, is handed
// a long-running task, and then has its connection to the leader cut
// (simulating a crash) before it can report completion. The leader
// should notice the closed connection immediately, mark the worker DEAD,
// and requeue the stranded task for the next worker to pick up — no
// waiting for the heartbeat timeout.
func TestLeaderWorker_ReassignsTaskWhenWorkerConnectionDrops(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	queue := task.NewQueue(4)
	results := task.NewResultStore()
	srv := leader.NewServer(queue, results)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serveDone := make(chan error, 1)
	go func() { serveDone <- srv.Serve(ctx, ln) }()

	// w1 is the only worker at first, so it is guaranteed to get the
	// task below. Its context is independent of the test's so it can be
	// killed on its own, without tearing down the leader or w2.
	w1Ctx, cancelW1 := context.WithCancel(context.Background())
	w1 := &worker.Client{ID: "w1", LeaderAddr: ln.Addr().String(), Concurrency: 1}
	w1Done := make(chan error, 1)
	go func() { w1Done <- w1.Run(w1Ctx) }()

	waitUntil(t, 2*time.Second, func() bool {
		return len(srv.Registry.Alive()) == 1
	})

	// Long enough that it is still running (not yet completed/reported)
	// when w1's connection is cut a moment from now.
	if err := queue.Submit(ctx, task.Task{ID: "t1", Command: "sleep", Args: []string{"1"}}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	time.Sleep(100 * time.Millisecond) // give dispatchLoop time to hand t1 to w1

	cancelW1() // simulate w1 crashing mid-task: its connection drops
	select {
	case <-w1Done:
	case <-time.After(2 * time.Second):
		t.Fatal("w1 did not shut down after its context was cancelled")
	}

	w2 := &worker.Client{ID: "w2", LeaderAddr: ln.Addr().String(), Concurrency: 1}
	w2Done := make(chan error, 1)
	go func() { w2Done <- w2.Run(ctx) }()

	var res task.Result
	waitUntil(t, 3*time.Second, func() bool {
		var ok bool
		res, ok = results.Get("t1")
		return ok
	})
	if res.Status != task.StatusCompleted {
		t.Fatalf("status = %s, want %s", res.Status, task.StatusCompleted)
	}

	cancel()
	<-serveDone
	<-w2Done
}

func waitUntil(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		if cond() {
			return
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for condition")
		case <-time.After(10 * time.Millisecond):
		}
	}
}
