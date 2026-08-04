// Package raft implements Raft-inspired leader election: follower,
// candidate, and leader states; randomized election timeouts; term-based
// voting; and a majority quorum to win. It deliberately does not
// replicate a command log across nodes — see information.md for why —
// so a Node only ever tracks currentTerm/votedFor/state in memory, never
// a log. It has zero knowledge of tasks or workers; leader.Server uses
// IsLeader to decide whether to accept work.
package raft

import (
	"bufio"
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/sshivamanand/distributed-task-framework/internal/protocol"
)

type State string

const (
	Follower  State = "FOLLOWER"
	Candidate State = "CANDIDATE"
	Leader    State = "LEADER"
)

const (
	// Election timeouts are randomized within [Min, Max) so peers don't
	// restart elections in lockstep — the mechanism that keeps a split
	// vote from repeating forever (each retry draws a fresh, independent
	// timeout, so ties decorrelate rather than staying synchronized).
	DefaultElectionTimeoutMin = 1500 * time.Millisecond
	DefaultElectionTimeoutMax = 3000 * time.Millisecond
	// DefaultHeartbeatInterval must be well below the election timeout
	// range so a healthy leader's heartbeats reliably beat every
	// follower's timeout.
	DefaultHeartbeatInterval = 400 * time.Millisecond
	DefaultDialTimeout       = 300 * time.Millisecond
)

// Node runs Raft-inspired election among a fixed set of peers.
type Node struct {
	ID    string
	Peers []string // addresses of every OTHER node in the cluster; never mutated after construction

	// OnBecomeFollower fires when a node that was Leader steps down —
	// leader.Server uses this to drop its currently-registered workers,
	// since they need to rediscover whoever the new leader is.
	OnBecomeFollower func()

	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
	DialTimeout        time.Duration

	mu          sync.Mutex
	state       State
	currentTerm int
	votedFor    string
	resetTimer  chan struct{}
}

func NewNode(id string, peers []string) *Node {
	return &Node{
		ID:                 id,
		Peers:              peers,
		state:              Follower,
		resetTimer:         make(chan struct{}, 1),
		ElectionTimeoutMin: DefaultElectionTimeoutMin,
		ElectionTimeoutMax: DefaultElectionTimeoutMax,
		HeartbeatInterval:  DefaultHeartbeatInterval,
		DialTimeout:        DefaultDialTimeout,
	}
}

func (n *Node) State() State {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state
}

func (n *Node) IsLeader() bool {
	return n.State() == Leader
}

// Run drives the node's election timer (as Follower/Candidate) or
// heartbeat loop (as Leader) until ctx is done.
func (n *Node) Run(ctx context.Context) {
	if len(n.Peers) == 0 {
		// A 1-node "cluster" is trivially its own majority: waiting out
		// an election timeout to elect the only possible leader would
		// just be a pointless delay.
		n.mu.Lock()
		n.state = Leader
		n.mu.Unlock()
		log.Printf("raft[%s]: no peers configured, trivially leader of a 1-node cluster", n.ID)
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if n.IsLeader() {
			n.runLeader(ctx)
		} else {
			n.runElectionTimer(ctx)
		}
	}
}

func (n *Node) runElectionTimer(ctx context.Context) {
	timeout := n.randomElectionTimeout()
	select {
	case <-ctx.Done():
	case <-n.resetTimer:
		// A valid heartbeat arrived, or we just granted a vote; let Run
		// loop back around and start a fresh, freshly-randomized wait.
	case <-time.After(timeout):
		n.startElection(ctx)
	}
}

func (n *Node) randomElectionTimeout() time.Duration {
	span := n.ElectionTimeoutMax - n.ElectionTimeoutMin
	if span <= 0 {
		return n.ElectionTimeoutMin
	}
	return n.ElectionTimeoutMin + time.Duration(rand.Int63n(int64(span)))
}

func (n *Node) runLeader(ctx context.Context) {
	n.broadcastHeartbeat(ctx)
	ticker := time.NewTicker(n.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !n.IsLeader() {
				return
			}
			n.broadcastHeartbeat(ctx)
		}
	}
}

// startElection runs one candidacy: increment the term, vote for
// ourselves, request votes from every peer in parallel, and become
// leader if a strict majority of the whole cluster (self included)
// grants a vote. If we don't win, we simply return — Run's loop takes us
// back through runElectionTimer with a fresh randomized timeout, exactly
// as a real Raft candidate retries after a split vote.
func (n *Node) startElection(ctx context.Context) {
	n.mu.Lock()
	n.state = Candidate
	n.currentTerm++
	term := n.currentTerm
	n.votedFor = n.ID
	n.mu.Unlock()

	log.Printf("raft[%s]: election timeout, starting election for term %d", n.ID, term)

	var (
		mu    sync.Mutex
		votes = 1 // vote for self
		wg    sync.WaitGroup
	)
	for _, peer := range n.Peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			granted, peerTerm := n.sendRequestVote(ctx, peer, term)
			if peerTerm > term {
				n.becomeFollower(peerTerm)
				return
			}
			if granted {
				mu.Lock()
				votes++
				mu.Unlock()
			}
		}(peer)
	}
	wg.Wait()

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.state != Candidate || n.currentTerm != term {
		return // stepped down or moved to a new term while votes were in flight
	}
	if votes*2 > len(n.Peers)+1 {
		n.state = Leader
		log.Printf("raft[%s]: won election for term %d with %d/%d votes", n.ID, term, votes, len(n.Peers)+1)
	}
}

// HandleRequestVote is the RequestVote RPC's core logic, decided on term
// number alone (there is no replicated log to compare). Exported
// separately from the network adapter below so it's unit-testable
// without any TCP involved.
func (n *Node) HandleRequestVote(candidateTerm int, candidateID string) (granted bool, currentTerm int) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if candidateTerm > n.currentTerm {
		n.stepDownLocked(candidateTerm)
	}
	if candidateTerm < n.currentTerm {
		return false, n.currentTerm
	}
	if n.votedFor == "" || n.votedFor == candidateID {
		n.votedFor = candidateID
		n.resetElectionTimerLocked()
		return true, n.currentTerm
	}
	return false, n.currentTerm
}

// HandleAppendEntries is the heartbeat RPC's core logic: a valid
// (term >= ours) heartbeat always keeps/makes the receiver a follower of
// that term and resets its election timer.
func (n *Node) HandleAppendEntries(leaderTerm int, leaderID string) (success bool, currentTerm int) {
	n.mu.Lock()
	if leaderTerm < n.currentTerm {
		term := n.currentTerm
		n.mu.Unlock()
		return false, term
	}
	wasLeader := n.state == Leader
	if leaderTerm > n.currentTerm {
		n.currentTerm = leaderTerm
		n.votedFor = "" // a new term: we haven't cast a vote in it yet
	}
	n.state = Follower
	n.resetElectionTimerLocked()
	term := n.currentTerm
	n.mu.Unlock()

	if wasLeader && n.OnBecomeFollower != nil {
		n.OnBecomeFollower()
	}
	return true, term
}

func (n *Node) becomeFollower(term int) {
	n.mu.Lock()
	wasLeader := n.state == Leader
	n.stepDownLocked(term)
	n.mu.Unlock()
	if wasLeader && n.OnBecomeFollower != nil {
		n.OnBecomeFollower()
	}
}

// stepDownLocked must be called with mu held.
func (n *Node) stepDownLocked(term int) {
	if n.state != Follower || term != n.currentTerm {
		log.Printf("raft[%s]: stepping down to FOLLOWER, adopting term %d", n.ID, term)
	}
	n.state = Follower
	n.currentTerm = term
	n.votedFor = ""
	n.resetElectionTimerLocked()
}

// resetElectionTimerLocked must be called with mu held; it never blocks.
func (n *Node) resetElectionTimerLocked() {
	select {
	case n.resetTimer <- struct{}{}:
	default:
	}
}

// broadcastHeartbeat sends AppendEntries to every peer concurrently. A
// peer reporting a higher term means someone else has already moved the
// cluster on without us, so we step down immediately.
func (n *Node) broadcastHeartbeat(ctx context.Context) {
	n.mu.Lock()
	term := n.currentTerm
	n.mu.Unlock()

	for _, peer := range n.Peers {
		go func(peer string) {
			peerTerm, ok := n.sendAppendEntries(peer, term)
			if ok && peerTerm > term {
				n.becomeFollower(peerTerm)
			}
		}(peer)
	}
}

// sendRequestVote and sendAppendEntries below are the RPC client side:
// each is a fresh, short-lived TCP connection per call rather than a
// pooled/persistent one. That trades connection-reuse performance for
// simplicity — correctness of the election protocol was the point, not
// throughput, and peers only exchange one request/response per interval
// anyway.
//
// A dial/write/read failure is treated as "no response" and never counts
// as a granted vote or a successful heartbeat. Term 0 doubles as the
// "no response" sentinel for peerTerm: every real response carries a
// term of at least 1, since a candidate always increments its term to at
// least 1 before requesting votes.

func (n *Node) sendRequestVote(ctx context.Context, peer string, term int) (granted bool, peerTerm int) {
	conn, err := net.DialTimeout("tcp", peer, n.DialTimeout)
	if err != nil {
		return false, 0
	}
	defer conn.Close()

	if err := protocol.WriteMessage(conn, protocol.TypeRequestVote, protocol.RequestVote{Term: term, CandidateID: n.ID}); err != nil {
		return false, 0
	}
	conn.SetReadDeadline(time.Now().Add(n.DialTimeout))
	env, err := protocol.ReadMessage(bufio.NewReader(conn))
	if err != nil {
		return false, 0
	}
	var resp protocol.VoteResponse
	if err := json.Unmarshal(env.Payload, &resp); err != nil {
		return false, 0
	}
	return resp.VoteGranted, resp.Term
}

func (n *Node) sendAppendEntries(peer string, term int) (peerTerm int, ok bool) {
	conn, err := net.DialTimeout("tcp", peer, n.DialTimeout)
	if err != nil {
		return 0, false
	}
	defer conn.Close()

	if err := protocol.WriteMessage(conn, protocol.TypeAppendEntries, protocol.AppendEntries{Term: term, LeaderID: n.ID}); err != nil {
		return 0, false
	}
	conn.SetReadDeadline(time.Now().Add(n.DialTimeout))
	env, err := protocol.ReadMessage(bufio.NewReader(conn))
	if err != nil {
		return 0, false
	}
	var resp protocol.AppendEntriesResponse
	if err := json.Unmarshal(env.Payload, &resp); err != nil {
		return 0, false
	}
	return resp.Term, true
}

// HandleRequestVoteConn and HandleAppendEntriesConn are the RPC server
// side: thin network adapters around the Handle* logic above, for a
// caller that has already read env as the first message off conn (the
// shared leader/worker listener dispatches on env.Type before routing
// here).
func (n *Node) HandleRequestVoteConn(conn net.Conn, env protocol.Envelope) {
	var req protocol.RequestVote
	if err := json.Unmarshal(env.Payload, &req); err != nil {
		return
	}
	granted, term := n.HandleRequestVote(req.Term, req.CandidateID)
	protocol.WriteMessage(conn, protocol.TypeVoteResponse, protocol.VoteResponse{Term: term, VoteGranted: granted})
}

func (n *Node) HandleAppendEntriesConn(conn net.Conn, env protocol.Envelope) {
	var req protocol.AppendEntries
	if err := json.Unmarshal(env.Payload, &req); err != nil {
		return
	}
	success, term := n.HandleAppendEntries(req.Term, req.LeaderID)
	protocol.WriteMessage(conn, protocol.TypeAppendEntriesResponse, protocol.AppendEntriesResponse{Term: term, Success: success})
}
