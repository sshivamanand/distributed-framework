package raft_test

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/sshivamanand/distributed-task-framework/internal/protocol"
	"github.com/sshivamanand/distributed-task-framework/internal/raft"
)

// newTestCluster builds n raft.Nodes, each configured with the addresses
// of every other node as peers, plus a listener for each. Timeouts are
// shrunk well below the package defaults so these tests run in
// milliseconds instead of the several real seconds a production cluster
// would use.
func newTestCluster(t *testing.T, n int) ([]*raft.Node, []net.Listener) {
	t.Helper()

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

	nodes := make([]*raft.Node, n)
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
		nodes[i] = node
	}
	return nodes, listeners
}

// serveRaftNode is a minimal stand-in for the dispatch that leader.Server
// does on its shared listener in production: read the first message off
// each connection and route RequestVote/AppendEntries to the node. (The
// production listener also handles RegisterWorker, which doesn't apply
// to a bare raft.Node under test.)
func serveRaftNode(ctx context.Context, ln net.Listener, n *raft.Node) {
	go func() {
		<-ctx.Done()
		ln.Close()
	}()
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go func(conn net.Conn) {
			defer conn.Close()
			env, err := protocol.ReadMessage(bufio.NewReader(conn))
			if err != nil {
				return
			}
			switch env.Type {
			case protocol.TypeRequestVote:
				n.HandleRequestVoteConn(conn, env)
			case protocol.TypeAppendEntries:
				n.HandleAppendEntriesConn(conn, env)
			}
		}(conn)
	}
}

func waitForSingleLeader(t *testing.T, nodes []*raft.Node, timeout time.Duration) *raft.Node {
	t.Helper()
	deadline := time.After(timeout)
	for {
		var leaders []*raft.Node
		for _, n := range nodes {
			if n.IsLeader() {
				leaders = append(leaders, n)
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		if len(leaders) > 1 {
			t.Fatalf("split brain: %d nodes simultaneously think they are leader", len(leaders))
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for a single leader to emerge (currently %d)", len(leaders))
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// TestCluster_ElectsExactlyOneLeader is the core safety+liveness proof:
// a healthy 3-node cluster converges on exactly one leader.
func TestCluster_ElectsExactlyOneLeader(t *testing.T) {
	nodes, listeners := newTestCluster(t, 3)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i, n := range nodes {
		go n.Run(ctx)
		go serveRaftNode(ctx, listeners[i], n)
	}

	waitForSingleLeader(t, nodes, 3*time.Second)
}

// TestCluster_ElectsNewLeaderAfterOldLeaderStops is the failover proof:
// killing the current leader still leaves the remaining majority able to
// elect a different one — the property the docker-compose demo (kill
// the leader, watch the cluster recover) rests on.
func TestCluster_ElectsNewLeaderAfterOldLeaderStops(t *testing.T) {
	nodes, listeners := newTestCluster(t, 3)
	ctx := context.Background()

	nodeCtxs := make([]context.Context, len(nodes))
	nodeCancels := make([]context.CancelFunc, len(nodes))
	for i, n := range nodes {
		nodeCtxs[i], nodeCancels[i] = context.WithCancel(ctx)
		go n.Run(nodeCtxs[i])
		go serveRaftNode(nodeCtxs[i], listeners[i], n)
	}
	defer func() {
		for _, cancel := range nodeCancels {
			cancel()
		}
	}()

	first := waitForSingleLeader(t, nodes, 3*time.Second)

	idx := -1
	for i, n := range nodes {
		if n == first {
			idx = i
		}
	}
	nodeCancels[idx]()

	var remaining []*raft.Node
	for i, n := range nodes {
		if i != idx {
			remaining = append(remaining, n)
		}
	}

	second := waitForSingleLeader(t, remaining, 3*time.Second)
	if second == first {
		t.Fatal("expected a different node to become the new leader")
	}
}

// TestCluster_NoLeaderWithoutQuorum proves the safety half of the
// tradeoff: with only 1 of 3 nodes reachable, quorum (computed against
// the configured cluster size, not the currently-reachable set) is
// unattainable, so the system correctly refuses to elect anyone rather
// than risk a minority declaring itself leader.
func TestCluster_NoLeaderWithoutQuorum(t *testing.T) {
	nodes, listeners := newTestCluster(t, 3)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Only start node 0; nodes 1 and 2 never run, simulating them being
	// down from the start.
	go nodes[0].Run(ctx)
	go serveRaftNode(ctx, listeners[0], nodes[0])

	// Long enough for several election-timeout cycles to have elapsed.
	deadline := time.After(500 * time.Millisecond)
	for {
		if nodes[0].IsLeader() {
			t.Fatal("a lone node should never win an election without a majority of the cluster")
		}
		select {
		case <-deadline:
			return // no leader ever emerged, as expected
		case <-time.After(10 * time.Millisecond):
		}
	}
}
