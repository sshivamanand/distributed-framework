package task

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func waitForResult(t *testing.T, store *ResultStore, id string) Result {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		if res, ok := store.Get(id); ok {
			return res
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for result of %s", id)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestWorkerPool_ExecutesConcurrently(t *testing.T) {
	queue := NewQueue(10)
	store := NewResultStore()
	ctx, cancel := context.WithCancel(context.Background())

	var poolWg sync.WaitGroup
	poolWg.Add(1)
	go func() {
		defer poolWg.Done()
		RunWorkerPool(ctx, 4, queue, store, nil)
	}()

	const n = 20
	var submitWg sync.WaitGroup
	submitWg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer submitWg.Done()
			id := fmt.Sprintf("task-%d", i)
			if err := queue.Submit(ctx, Task{ID: id, Command: "echo", Args: []string{id}}); err != nil {
				t.Errorf("submit %s: %v", id, err)
			}
		}(i)
	}
	submitWg.Wait()

	for i := 0; i < n; i++ {
		id := fmt.Sprintf("task-%d", i)
		res := waitForResult(t, store, id)
		if res.Status != StatusCompleted {
			t.Errorf("task %s: status = %s, want %s", id, res.Status, StatusCompleted)
		}
		if !strings.Contains(res.Output, id) {
			t.Errorf("task %s: output = %q, want contains %q", id, res.Output, id)
		}
	}

	cancel()
	poolWg.Wait()
}

func TestWorkerPool_FailedCommandRecordsError(t *testing.T) {
	queue := NewQueue(1)
	store := NewResultStore()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		RunWorkerPool(ctx, 1, queue, store, nil)
		close(done)
	}()

	if err := queue.Submit(ctx, Task{ID: "bad", Command: "definitely-not-a-real-command"}); err != nil {
		t.Fatalf("submit: %v", err)
	}

	res := waitForResult(t, store, "bad")
	if res.Status != StatusFailed {
		t.Errorf("status = %s, want %s", res.Status, StatusFailed)
	}
	if res.Error == "" {
		t.Error("expected non-empty error message")
	}

	cancel()
	<-done
}

func TestWorkerPool_ShutsDownOnContextCancel(t *testing.T) {
	queue := NewQueue(1)
	store := NewResultStore()
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		RunWorkerPool(ctx, 2, queue, store, nil)
		close(done)
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunWorkerPool did not return after context cancellation")
	}
}
