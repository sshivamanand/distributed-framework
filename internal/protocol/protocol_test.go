package protocol

import (
	"bufio"
	"bytes"
	"encoding/json"
	"testing"

	"github.com/sshivamanand/distributed-task-framework/internal/task"
)

func TestWriteReadMessage_RoundTrip(t *testing.T) {
	var buf bytes.Buffer

	if err := WriteMessage(&buf, TypeRegisterWorker, RegisterWorker{WorkerID: "w1"}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := WriteMessage(&buf, TypeTaskAssignment, TaskAssignment{
		Task: task.Task{ID: "t1", Command: "echo", Args: []string{"hi"}},
	}); err != nil {
		t.Fatalf("write: %v", err)
	}

	r := bufio.NewReader(&buf)

	env, err := ReadMessage(r)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if env.Type != TypeRegisterWorker {
		t.Fatalf("type = %s, want %s", env.Type, TypeRegisterWorker)
	}
	var reg RegisterWorker
	if err := json.Unmarshal(env.Payload, &reg); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if reg.WorkerID != "w1" {
		t.Fatalf("got %+v", reg)
	}

	env, err = ReadMessage(r)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if env.Type != TypeTaskAssignment {
		t.Fatalf("type = %s, want %s", env.Type, TypeTaskAssignment)
	}
	var ta TaskAssignment
	if err := json.Unmarshal(env.Payload, &ta); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if ta.Task.ID != "t1" || ta.Task.Command != "echo" {
		t.Fatalf("got %+v", ta.Task)
	}
}

func TestReadMessage_EmptyInputIsError(t *testing.T) {
	r := bufio.NewReader(bytes.NewReader(nil))
	if _, err := ReadMessage(r); err == nil {
		t.Fatal("expected error on empty input")
	}
}
