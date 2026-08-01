package registry

import (
	"sync"
	"time"
)

type Status string

const (
	StatusAlive Status = "ALIVE"
	StatusDead  Status = "DEAD"
)

// Worker is a snapshot of one worker's known state. Registry hands out
// copies, never the live entry, so callers can't mutate state outside
// the registry's lock.
type Worker struct {
	ID            string
	Address       string
	Status        Status
	LastHeartbeat time.Time
}

// Registry tracks known workers. It is looked up and updated by ID from
// many goroutines (one per connection, plus the future heartbeat
// monitor), so it is a mutex-guarded map, not a channel.
type Registry struct {
	mu      sync.Mutex
	workers map[string]Worker
}

func New() *Registry {
	return &Registry{workers: make(map[string]Worker)}
}

func (r *Registry) Register(id, address string, now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.workers[id] = Worker{ID: id, Address: address, Status: StatusAlive, LastHeartbeat: now}
}

// Heartbeat records a heartbeat for id, reviving it to ALIVE if it had
// been marked DEAD. Returns false if id was never registered.
func (r *Registry) Heartbeat(id string, now time.Time) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	w, ok := r.workers[id]
	if !ok {
		return false
	}
	w.LastHeartbeat = now
	w.Status = StatusAlive
	r.workers[id] = w
	return true
}

func (r *Registry) MarkDead(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	w, ok := r.workers[id]
	if !ok {
		return
	}
	w.Status = StatusDead
	r.workers[id] = w
}

func (r *Registry) Get(id string) (Worker, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	w, ok := r.workers[id]
	return w, ok
}

// Alive returns a snapshot of every worker currently marked ALIVE.
func (r *Registry) Alive() []Worker {
	r.mu.Lock()
	defer r.mu.Unlock()
	var alive []Worker
	for _, w := range r.workers {
		if w.Status == StatusAlive {
			alive = append(alive, w)
		}
	}
	return alive
}
