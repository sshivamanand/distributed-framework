package client_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/sshivamanand/distributed-task-framework/internal/client"
	"github.com/sshivamanand/distributed-task-framework/internal/leader"
	"github.com/sshivamanand/distributed-task-framework/internal/raft"
	"github.com/sshivamanand/distributed-task-framework/internal/task"
	"github.com/sshivamanand/distributed-task-framework/internal/worker"
)

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

// TestClient_SubmitAndQueryResult_EndToEnd exercises the whole new
// client-facing path: a client submits a task to the leader, a worker
// executes it, and the client queries the leader for the result.
func TestClient_SubmitAndQueryResult_EndToEnd(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := leader.NewServer(task.NewQueue(4), task.NewResultStore(), raft.NewNode("solo", nil))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Raft.Run(ctx)
	go srv.Serve(ctx, ln)

	w := &worker.Client{ID: "w1", LeaderAddrs: []string{ln.Addr().String()}, Concurrency: 1}
	go w.Run(ctx)

	waitUntil(t, 2*time.Second, func() bool { return len(srv.Registry.Alive()) == 1 })

	c := &client.Client{LeaderAddrs: []string{ln.Addr().String()}}
	if err := c.Submit(task.Task{ID: "t1", Command: "echo", Args: []string{"hi-from-client"}}); err != nil {
		t.Fatalf("submit: %v", err)
	}

	var res task.Result
	var found bool
	waitUntil(t, 2*time.Second, func() bool {
		var err error
		res, found, err = c.QueryResult("t1")
		return err == nil && found
	})
	if res.Status != task.StatusCompleted {
		t.Fatalf("status = %s, want %s", res.Status, task.StatusCompleted)
	}
}

func TestClient_QueryResult_UnknownTaskIsNotFound(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := leader.NewServer(task.NewQueue(4), task.NewResultStore(), raft.NewNode("solo", nil))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Raft.Run(ctx)
	go srv.Serve(ctx, ln)

	waitUntil(t, time.Second, srv.Raft.IsLeader)

	c := &client.Client{LeaderAddrs: []string{ln.Addr().String()}}
	_, found, err := c.QueryResult("ghost")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if found {
		t.Fatal("expected an unknown task ID to be reported not found")
	}
}

// TestClient_Submit_SkipsNonLeaderAddress proves the discovery loop:
// given a follower's address before the real leader's, Submit should
// silently skip the rejection and succeed against the leader.
func TestClient_Submit_SkipsNonLeaderAddress(t *testing.T) {
	lnFollower, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	// This server's raft node is deliberately never Run(), so it never
	// becomes leader no matter how long the test waits.
	followerSrv := leader.NewServer(task.NewQueue(4), task.NewResultStore(), raft.NewNode("never-leader", nil))

	lnLeader, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	leaderSrv := leader.NewServer(task.NewQueue(4), task.NewResultStore(), raft.NewNode("solo", nil))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go followerSrv.Serve(ctx, lnFollower)
	go leaderSrv.Raft.Run(ctx)
	go leaderSrv.Serve(ctx, lnLeader)

	waitUntil(t, time.Second, leaderSrv.Raft.IsLeader)

	c := &client.Client{LeaderAddrs: []string{lnFollower.Addr().String(), lnLeader.Addr().String()}}
	if err := c.Submit(task.Task{ID: "t2", Command: "echo", Args: []string{"hi"}}); err != nil {
		t.Fatalf("submit should have succeeded by skipping the non-leader address: %v", err)
	}
}
