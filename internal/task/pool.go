package task

import (
	"context"
	"os/exec"
	"sync"
	"time"
)

// RunWorkerPool starts n worker goroutines pulling from queue and writing
// results to store. onResult, if non-nil, is called after each result is
// stored — phase 2 uses this to report completion back to the leader over
// the network. It blocks until ctx is cancelled and every worker has
// returned, so callers typically run it in its own goroutine.
func RunWorkerPool(ctx context.Context, n int, queue *Queue, store *ResultStore, onResult func(Result)) {
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			worker(ctx, queue, store, onResult)
		}()
	}
	wg.Wait()
}

func worker(ctx context.Context, queue *Queue, store *ResultStore, onResult func(Result)) {
	for {
		select {
		case t, ok := <-queue.Tasks():
			if !ok {
				return
			}
			res := execute(ctx, t)
			store.Set(res)
			if onResult != nil {
				onResult(res)
			}
		case <-ctx.Done():
			return
		}
	}
}

func execute(ctx context.Context, t Task) Result {
	start := time.Now()
	cmd := exec.CommandContext(ctx, t.Command, t.Args...)
	out, err := cmd.CombinedOutput()
	end := time.Now()

	res := Result{
		TaskID:    t.ID,
		Output:    string(out),
		StartedAt: start,
		EndedAt:   end,
	}
	if err != nil {
		res.Status = StatusFailed
		res.Error = err.Error()
	} else {
		res.Status = StatusCompleted
	}
	return res
}
