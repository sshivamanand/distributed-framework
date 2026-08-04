package leader_test

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/sshivamanand/distributed-task-framework/internal/leader"
	"github.com/sshivamanand/distributed-task-framework/internal/raft"
	"github.com/sshivamanand/distributed-task-framework/internal/task"
	"github.com/sshivamanand/distributed-task-framework/internal/worker"
)

func waitForSingleLeaderServer(t *testing.T, servers []*leader.Server, timeout time.Duration) int {
	t.Helper()
	deadline := time.After(timeout)
	for {
		leaders, idx := 0, -1
		for i, s := range servers {
			if s.Raft.IsLeader() {
				leaders++
				idx = i
			}
		}
		if leaders == 1 {
			return idx
		}
		if leaders > 1 {
			t.Fatalf("split brain: %d servers simultaneously think they are leader", leaders)
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for a single leader server (currently %d)", leaders)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// TestCluster_WorkersReconnectAndNewLeaderServesTasksAfterFailover is the
// full-stack rehearsal of the project's headline demo: a 3-node
// leader-eligible cluster elects a leader, workers register with it,
// the leader is killed mid-run, a new leader is elected among the
// survivors, and the already-running workers detect the dropped
// connection and rediscover the new leader on their own — which can
// then take on and complete a fresh task.
//
// This deliberately does not submit a task before the kill and check it
// survives: by design (see information.md) there is no replication of
// queue/registry state across an election, so in-flight work on a
// crashed leader is genuinely lost. What this test proves is the
// cluster's recovery, not durability it was never meant to have.
func TestCluster_WorkersReconnectAndNewLeaderServesTasksAfterFailover(t *testing.T) {
	const n = 3
	addrs := make([]string, n)
	listeners := make([]net.Listener, n)
	for i := 0; i < n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		listeners[i] = ln
		addrs[i] = ln.Addr().String()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	servers := make([]*leader.Server, n)
	nodeCancels := make([]context.CancelFunc, n)
	for i := 0; i < n; i++ {
		var peers []string
		for j, a := range addrs {
			if j != i {
				peers = append(peers, a)
			}
		}
		node := raft.NewNode(fmt.Sprintf("node-%d", i), peers)
		node.ElectionTimeoutMin = 60 * time.Millisecond
		node.ElectionTimeoutMax = 120 * time.Millisecond
		node.HeartbeatInterval = 20 * time.Millisecond
		node.DialTimeout = 50 * time.Millisecond

		srv := leader.NewServer(task.NewQueue(8), task.NewResultStore(), node)
		servers[i] = srv

		nodeCtx, nodeCancel := context.WithCancel(ctx)
		nodeCancels[i] = nodeCancel
		go node.Run(nodeCtx)
		go srv.Serve(nodeCtx, listeners[i])
	}

	firstIdx := waitForSingleLeaderServer(t, servers, 3*time.Second)
	first := servers[firstIdx]

	w1 := &worker.Client{ID: "w1", LeaderAddrs: addrs, Concurrency: 1}
	w2 := &worker.Client{ID: "w2", LeaderAddrs: addrs, Concurrency: 1}
	go w1.Run(ctx)
	go w2.Run(ctx)

	waitUntil(t, 2*time.Second, func() bool {
		return len(first.Registry.Alive()) == 2
	})

	// Kill the current leader: stop its raft node and close its listener,
	// simulating a crash.
	nodeCancels[firstIdx]()
	listeners[firstIdx].Close()

	var remaining []*leader.Server
	for i, s := range servers {
		if i != firstIdx {
			remaining = append(remaining, s)
		}
	}
	secondIdx := waitForSingleLeaderServer(t, remaining, 3*time.Second)
	second := remaining[secondIdx]

	// The workers were never told about the new leader; they have to
	// notice their connection dropped and rediscover it themselves.
	waitUntil(t, 3*time.Second, func() bool {
		return len(second.Registry.Alive()) == 2
	})

	if err := second.Queue.Submit(ctx, task.Task{ID: "t-after-failover", Command: "echo", Args: []string{"still-alive"}}); err != nil {
		t.Fatalf("submit to new leader: %v", err)
	}

	waitUntil(t, 2*time.Second, func() bool {
		res, ok := second.Results.Get("t-after-failover")
		return ok && res.Status == task.StatusCompleted
	})
}
