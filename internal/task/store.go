package task

import "sync"

// ResultStore is shared, randomly-accessed state (keyed lookups by task
// ID from many goroutines), so it is a mutex-guarded map rather than a
// channel.
type ResultStore struct {
	mu      sync.Mutex
	results map[string]Result
}

func NewResultStore() *ResultStore {
	return &ResultStore{results: make(map[string]Result)}
}

func (s *ResultStore) Set(r Result) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.results[r.TaskID] = r
}

func (s *ResultStore) Get(taskID string) (Result, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.results[taskID]
	return r, ok
}
