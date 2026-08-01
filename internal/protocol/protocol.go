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
