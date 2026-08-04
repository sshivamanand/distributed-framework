package protocol

import (
	"bufio"
	"encoding/json"
	"io"

	"github.com/sshivamanand/distributed-task-framework/internal/task"
)

// MessageType identifies which payload an Envelope carries.
type MessageType string

const (
	TypeRegisterWorker MessageType = "REGISTER_WORKER"
	TypeHeartbeat      MessageType = "HEARTBEAT"
	TypeTaskAssignment MessageType = "TASK_ASSIGNMENT"
	TypeTaskCompleted  MessageType = "TASK_COMPLETED"
	TypeAck            MessageType = "ACK"

	// Raft-inspired leader election RPCs, phase 4. They share this same
	// newline-delimited-JSON envelope and, in the leader binary, the same
	// TCP listener as worker traffic — the first message on a connection
	// tells the accept loop which kind of peer it's talking to.
	TypeRequestVote           MessageType = "REQUEST_VOTE"
	TypeVoteResponse          MessageType = "VOTE_RESPONSE"
	TypeAppendEntries         MessageType = "APPEND_ENTRIES"
	TypeAppendEntriesResponse MessageType = "APPEND_ENTRIES_RESPONSE"

	// Client-facing RPCs, phase 5. Like everything else, only the
	// currently-elected leader accepts these — a follower responds with
	// the same Ack{OK:false, Error:"not leader"} a worker gets, so
	// internal/client's discovery loop tries the next address.
	TypeSubmitTask   MessageType = "SUBMIT_TASK"
	TypeQueryResult  MessageType = "QUERY_RESULT"
	TypeResultStatus MessageType = "RESULT_STATUS"
)

// Envelope is the outer frame every message is wrapped in: a type tag plus
// the payload as raw JSON, so a reader can dispatch on Type before
// decoding the payload into the concrete struct it expects.
type Envelope struct {
	Type    MessageType     `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type RegisterWorker struct {
	WorkerID string `json:"worker_id"`
}

type Heartbeat struct {
	WorkerID string `json:"worker_id"`
}

type TaskAssignment struct {
	Task task.Task `json:"task"`
}

type TaskCompleted struct {
	Result task.Result `json:"result"`
}

type Ack struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

// RequestVote is a candidate asking a peer for its vote in an election
// for Term. There is no last-log-index/term to compare (no replicated
// log in this project — see information.md), so vote granting is
// decided on term number alone.
type RequestVote struct {
	Term        int    `json:"term"`
	CandidateID string `json:"candidate_id"`
}

type VoteResponse struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"vote_granted"`
}

// AppendEntries is heartbeat-only in this project: a leader's periodic
// "I'm still here" to its peers. Real Raft also carries log entries;
// this project deliberately doesn't replicate a log, so there are none.
type AppendEntries struct {
	Term     int    `json:"term"`
	LeaderID string `json:"leader_id"`
}

type AppendEntriesResponse struct {
	Term    int  `json:"term"`
	Success bool `json:"success"`
}

// SubmitTask is a client asking the leader to enqueue t. The client
// picks the ID itself (rather than the leader generating one and
// returning it) so it already knows what to ask QueryResult for.
type SubmitTask struct {
	Task task.Task `json:"task"`
}

type QueryResult struct {
	TaskID string `json:"task_id"`
}

// ResultStatus answers QueryResult. Found is false both for a task that
// is still queued/in-flight and for one this leader simply never heard
// of — including a task that completed on a *previous* leader before a
// failover, since results aren't replicated (see information.md). The
// client can't tell those cases apart, and says so.
type ResultStatus struct {
	Found  bool        `json:"found"`
	Result task.Result `json:"result"`
}

// WriteMessage marshals payload, wraps it in an Envelope, and writes it as
// one line of JSON terminated by '\n'. The wire format is literally
// newline-delimited JSON: framing is just "read until '\n'".
func WriteMessage(w io.Writer, msgType MessageType, payload any) error {
	p, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	line, err := json.Marshal(Envelope{Type: msgType, Payload: p})
	if err != nil {
		return err
	}
	line = append(line, '\n')
	_, err = w.Write(line)
	return err
}

// ReadMessage reads one newline-delimited JSON message from r.
func ReadMessage(r *bufio.Reader) (Envelope, error) {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return Envelope{}, err
	}
	var env Envelope
	if err := json.Unmarshal(line, &env); err != nil {
		return Envelope{}, err
	}
	return env, nil
}
