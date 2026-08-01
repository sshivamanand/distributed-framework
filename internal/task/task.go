package task

import "time"

type Status string

const (
	StatusPending   Status = "PENDING"
	StatusRunning   Status = "RUNNING"
	StatusCompleted Status = "COMPLETED"
	StatusFailed    Status = "FAILED"
)

// Task is a unit of work: a shell command and its arguments. Keeping the
// payload to a command+args pair (rather than arbitrary code) means it is
// trivially JSON-serializable once it crosses the wire in phase 2.
type Task struct {
	ID      string
	Command string
	Args    []string
}

type Result struct {
	TaskID    string
	Status    Status
	Output    string
	Error     string
	StartedAt time.Time
	EndedAt   time.Time
}
