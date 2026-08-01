package leader

import (
	"bufio"
	"context"
	"encoding/json"
	"log"
	"net"
	"sync"
	"time"

	"github.com/sshivamanand/distributed-task-framework/internal/protocol"
	"github.com/sshivamanand/distributed-task-framework/internal/registry"
	"github.com/sshivamanand/distributed-task-framework/internal/task"
)

const (
	// DefaultHeartbeatTimeout is how long a worker can go without a
	// heartbeat before the monitor marks it DEAD. It must be comfortably
	// larger than the worker's heartbeat-send interval so a couple of
	// missed/delayed beats don't cause a false positive.
	DefaultHeartbeatTimeout = 9 * time.Second
	// DefaultHeartbeatCheckInterval is how often the monitor scans for
	// stale workers.
	DefaultHeartbeatCheckInterval = 1 * time.Second
)

// Server is the leader node: it accepts worker connections, dispatches
// queued tasks to registered workers round-robin, records completed
// results, and reassigns tasks stranded by a dead worker.
type Server struct {
	Registry *registry.Registry
	Queue    *task.Queue
	Results  *task.ResultStore

	// HeartbeatTimeout and HeartbeatCheckInterval configure the failure
	// detector; NewServer sets sane defaults, tests may shrink them.
	HeartbeatTimeout       time.Duration
	HeartbeatCheckInterval time.Duration

	mu       sync.Mutex
	conns    map[string]chan task.Task     // workerID -> its pending-assignment channel
	inFlight map[string]inFlightAssignment // taskID -> which worker it was handed to
	nextIdx  int
}

// inFlightAssignment records that a task has been handed to a worker but
// its TaskCompleted hasn't arrived yet — the bookkeeping that lets a dead
// worker's tasks be found and requeued.
type inFlightAssignment struct {
	Task     task.Task
	WorkerID string
}

func NewServer(queue *task.Queue, results *task.ResultStore) *Server {
	return &Server{
		Registry:               registry.New(),
		Queue:                  queue,
		Results:                results,
		HeartbeatTimeout:       DefaultHeartbeatTimeout,
		HeartbeatCheckInterval: DefaultHeartbeatCheckInterval,
		conns:                  make(map[string]chan task.Task),
		inFlight:               make(map[string]inFlightAssignment),
	}
}

// Serve accepts connections on ln and dispatches tasks until ctx is done.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	go s.dispatchLoop(ctx)
	go s.heartbeatMonitor(ctx)

	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
				return err
			}
		}
		go s.handleConn(ctx, conn)
	}
}

func (s *Server) handleConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	go func() {
		<-ctx.Done()
		conn.Close()
	}()

	r := bufio.NewReader(conn)

	env, err := protocol.ReadMessage(r)
	if err != nil {
		log.Printf("leader: reading registration: %v", err)
		return
	}
	if env.Type != protocol.TypeRegisterWorker {
		log.Printf("leader: expected %s, got %s", protocol.TypeRegisterWorker, env.Type)
		return
	}
	var reg protocol.RegisterWorker
	if err := json.Unmarshal(env.Payload, &reg); err != nil {
		log.Printf("leader: bad registration payload: %v", err)
		return
	}

	// The registry records the connection's observed remote address
	// rather than trusting a self-reported one from the worker.
	s.Registry.Register(reg.WorkerID, conn.RemoteAddr().String(), time.Now())
	if err := protocol.WriteMessage(conn, protocol.TypeAck, protocol.Ack{OK: true}); err != nil {
		return
	}
	log.Printf("leader: worker %s registered from %s", reg.WorkerID, conn.RemoteAddr())

	assignments := make(chan task.Task, 8)
	s.mu.Lock()
	s.conns[reg.WorkerID] = assignments
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.conns, reg.WorkerID)
		s.mu.Unlock()
		// Fast failure-detection path: a closed/broken TCP connection is
		// usually a worker that crashed cleanly, and we know about it as
		// soon as the read loop below errors out — no need to wait for
		// the heartbeat timeout. A worker that's merely hung (still
		// connected, but wedged) won't hit this path; that's what the
		// slower heartbeatMonitor is for.
		s.Registry.MarkDead(reg.WorkerID)
		s.requeueWorkerTasks(ctx, reg.WorkerID)
		log.Printf("leader: worker %s disconnected", reg.WorkerID)
	}()

	go s.writeAssignments(conn, assignments)

	for {
		env, err := protocol.ReadMessage(r)
		if err != nil {
			return
		}
		switch env.Type {
		case protocol.TypeHeartbeat:
			s.Registry.Heartbeat(reg.WorkerID, time.Now())
		case protocol.TypeTaskCompleted:
			var tc protocol.TaskCompleted
			if err := json.Unmarshal(env.Payload, &tc); err != nil {
				log.Printf("leader: bad task-completed payload: %v", err)
				continue
			}
			s.Results.Set(tc.Result)
			s.mu.Lock()
			delete(s.inFlight, tc.Result.TaskID)
			s.mu.Unlock()
			log.Printf("leader: task %s completed by %s: %s", tc.Result.TaskID, reg.WorkerID, tc.Result.Status)
		default:
			log.Printf("leader: unexpected message type %s from %s", env.Type, reg.WorkerID)
		}
	}
}

func (s *Server) writeAssignments(conn net.Conn, assignments <-chan task.Task) {
	for t := range assignments {
		if err := protocol.WriteMessage(conn, protocol.TypeTaskAssignment, protocol.TaskAssignment{Task: t}); err != nil {
			return
		}
	}
}

// dispatchLoop pulls tasks off the shared queue and hands each to the
// next ALIVE worker, round-robin. Scheduling policy is deliberately this
// simple — round-robin only is in scope, alternative policies are not.
func (s *Server) dispatchLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case t, ok := <-s.Queue.Tasks():
			if !ok {
				return
			}
			s.assign(ctx, t)
		}
	}
}

func (s *Server) assign(ctx context.Context, t task.Task) {
	for {
		alive := s.Registry.Alive()
		if len(alive) > 0 {
			s.mu.Lock()
			w := alive[s.nextIdx%len(alive)]
			ch, ok := s.conns[w.ID]
			s.nextIdx++
			if ok {
				s.inFlight[t.ID] = inFlightAssignment{Task: t, WorkerID: w.ID}
			}
			s.mu.Unlock()
			if ok {
				select {
				case ch <- t:
					return
				case <-ctx.Done():
					return
				}
			}
			continue // chosen worker's connection already torn down; retry
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// heartbeatMonitor periodically scans for workers whose last heartbeat is
// older than HeartbeatTimeout, marks them DEAD, and requeues whatever
// they had in flight. This is the slow-path failure detector: it catches
// a worker that is still connected but has stopped making progress,
// which a closed-connection check alone would never notice.
func (s *Server) heartbeatMonitor(ctx context.Context) {
	ticker := time.NewTicker(s.HeartbeatCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.reapStaleWorkers(ctx)
		}
	}
}

func (s *Server) reapStaleWorkers(ctx context.Context) {
	now := time.Now()
	for _, w := range s.Registry.Alive() {
		if now.Sub(w.LastHeartbeat) > s.HeartbeatTimeout {
			log.Printf("leader: worker %s missed heartbeat deadline (last seen %s ago), marking DEAD", w.ID, now.Sub(w.LastHeartbeat))
			s.Registry.MarkDead(w.ID)
			s.requeueWorkerTasks(ctx, w.ID)
		}
	}
}

// requeueWorkerTasks moves every task in flight on workerID back onto the
// shared queue, where dispatchLoop will hand it to whichever worker is
// next in the round-robin. It is safe to call more than once for the
// same worker (e.g. both failure-detection paths racing): once a task is
// removed from inFlight the second call simply finds nothing left to do.
//
// This is also the mechanism behind the project's honest at-least-once
// (not exactly-once) guarantee: a task can be requeued and re-executed
// elsewhere at the same moment the original worker's TaskCompleted for it
// is in flight over the network, so the same task can run twice.
func (s *Server) requeueWorkerTasks(ctx context.Context, workerID string) {
	s.mu.Lock()
	var stale []task.Task
	for id, a := range s.inFlight {
		if a.WorkerID == workerID {
			stale = append(stale, a.Task)
			delete(s.inFlight, id)
		}
	}
	s.mu.Unlock()

	for _, t := range stale {
		log.Printf("leader: requeuing task %s from dead worker %s", t.ID, workerID)
		go func(t task.Task) {
			if err := s.Queue.Submit(ctx, t); err != nil {
				log.Printf("leader: requeuing task %s: %v", t.ID, err)
			}
		}(t)
	}
}
