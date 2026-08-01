package task

import "context"

// Queue is a channel-backed FIFO of pending tasks. It is the pipeline
// between producers (submitters) and the worker pool, so it stays a
// channel rather than a mutex-guarded slice.
type Queue struct {
	ch chan Task
}

func NewQueue(capacity int) *Queue {
	return &Queue{ch: make(chan Task, capacity)}
}

// Submit enqueues t, blocking if the queue is full until either there is
// room or ctx is done.
func (q *Queue) Submit(ctx context.Context, t Task) error {
	select {
	case q.ch <- t:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Tasks exposes the receive-only side for workers to range/select over.
func (q *Queue) Tasks() <-chan Task {
	return q.ch
}

// Close stops the queue from accepting further sends. Workers still
// draining Tasks() will observe a closed channel once empty.
func (q *Queue) Close() {
	close(q.ch)
}
