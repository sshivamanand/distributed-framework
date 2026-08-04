package raft

import (
	"context"
	"testing"
	"time"
)

func TestHandleRequestVote_GrantsOncePerTerm(t *testing.T) {
	n := NewNode("n1", nil)

	granted, term := n.HandleRequestVote(1, "A")
	if !granted || term != 1 {
		t.Fatalf("first vote in term 1: granted=%v term=%d, want true/1", granted, term)
	}

	granted, term = n.HandleRequestVote(1, "B")
	if granted {
		t.Fatal("expected second vote request in the same term to be refused")
	}
	if term != 1 {
		t.Fatalf("term = %d, want 1", term)
	}
}

func TestHandleRequestVote_RejectsStaleTerm(t *testing.T) {
	n := NewNode("n1", nil)
	n.HandleRequestVote(5, "A") // establish currentTerm = 5

	granted, term := n.HandleRequestVote(3, "B")
	if granted {
		t.Fatal("expected a vote request for a stale term to be refused")
	}
	if term != 5 {
		t.Fatalf("term = %d, want 5 (our own current term)", term)
	}
}

func TestHandleRequestVote_HigherTermStepsDownAndGrantsVote(t *testing.T) {
	n := NewNode("n1", nil)
	n.mu.Lock()
	n.state = Leader
	n.currentTerm = 2
	n.mu.Unlock()

	granted, term := n.HandleRequestVote(5, "C")
	if !granted {
		t.Fatal("expected vote to be granted after adopting the higher term")
	}
	if term != 5 {
		t.Fatalf("term = %d, want 5", term)
	}
	if got := n.State(); got != Follower {
		t.Fatalf("state = %s, want %s", got, Follower)
	}
}

func TestHandleAppendEntries_RejectsStaleTerm(t *testing.T) {
	n := NewNode("n1", nil)
	n.HandleRequestVote(5, "A") // establish currentTerm = 5

	success, term := n.HandleAppendEntries(3, "L")
	if success {
		t.Fatal("expected a stale-term heartbeat to be rejected")
	}
	if term != 5 {
		t.Fatalf("term = %d, want 5", term)
	}
}

func TestHandleAppendEntries_AdvancesTermAndBecomesFollower(t *testing.T) {
	n := NewNode("n1", nil)
	n.mu.Lock()
	n.state = Candidate
	n.currentTerm = 1
	n.votedFor = n.ID
	n.mu.Unlock()

	success, term := n.HandleAppendEntries(2, "L")
	if !success {
		t.Fatal("expected a higher-term heartbeat to be accepted")
	}
	if term != 2 {
		t.Fatalf("term = %d, want 2", term)
	}
	if got := n.State(); got != Follower {
		t.Fatalf("state = %s, want %s", got, Follower)
	}

	// The new term means we haven't voted in it yet.
	granted, _ := n.HandleRequestVote(2, "other-candidate")
	if !granted {
		t.Fatal("expected votedFor to have been cleared for the new term")
	}
}

func TestHandleAppendEntries_FiresOnBecomeFollowerOnlyWhenLeaderSteppedDown(t *testing.T) {
	n := NewNode("n1", nil)
	calls := 0
	n.OnBecomeFollower = func() { calls++ }

	// Not leader yet: a heartbeat shouldn't fire the callback.
	n.HandleAppendEntries(1, "L")
	if calls != 0 {
		t.Fatalf("calls = %d, want 0 (was never leader)", calls)
	}

	n.mu.Lock()
	n.state = Leader
	n.mu.Unlock()

	n.HandleAppendEntries(2, "L2")
	if calls != 1 {
		t.Fatalf("calls = %d, want 1 (stepped down from leader)", calls)
	}
}

func TestNode_NoPeersIsTriviallyLeaderOnRun(t *testing.T) {
	n := NewNode("solo", nil)
	if n.IsLeader() {
		t.Fatal("should not be leader before Run")
	}

	done := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		n.Run(ctx)
		close(done)
	}()

	waitUntil(t, time.Second, n.IsLeader)

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
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
		case <-time.After(5 * time.Millisecond):
		}
	}
}
