package task

import (
	"context"
	"testing"
	"time"
)

func TestQueue_SubmitRespectsContextWhenFull(t *testing.T) {
	q := NewQueue(1)
	if err := q.Submit(context.Background(), Task{ID: "1"}); err != nil {
		t.Fatalf("submit: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	if err := q.Submit(ctx, Task{ID: "2"}); err == nil {
		t.Fatal("expected error when queue is full and context times out")
	}
}
