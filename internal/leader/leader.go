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
	"github.com/sshivamanand/distributed-task-framework/internal/raft"
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

// Server is the leader-eligible node: it accepts worker connections and
// peer election RPCs on the same listener, dispatches queued tasks to
// registered workers round-robin, records completed results, and
// reassigns tasks stranded by a dead worker. It only accepts worker
// registrations while its embedded Raft node is actually the elected
// leader; see handleConn.
type Server struct {
	Registry *registry.Registry
	Queue    *task.Queue
	Results  *task.ResultStore
	Raft     *raft.Node

	// HeartbeatTimeout and HeartbeatCheckInterval configure the failure
	// detector; NewServer sets sane defaults, tests may shrink them.
	HeartbeatTimeout       time.Duration
	HeartbeatCheckInterval time.Duration

	mu       sync.Mutex
	conns    map[string]workerConn         // workerID -> its connection + pending-assignment channel
	inFlight map[string]inFlightAssignment // taskID -> which worker it was handed to
	nextIdx  int
}

type workerConn struct {
	conn        net.Conn
	assignments chan task.Task
}

// inFlightAssignment records that a task has been handed to a worker but
// its TaskCompleted hasn't arrived yet — the bookkeeping that lets a dead
// worker's tasks be found and requeued.
type inFlightAssignment struct {
	Task     task.Task
	WorkerID string
}

// NewServer wires r's OnBecomeFollower so that if this node is ever
// demoted, every worker currently registered with it gets disconnected —
// they need to rediscover whoever the new leader is, since (by design;
// see information.md) leadership doesn't carry any queue/registry state
// with it.
func NewServer(queue *task.Queue, results *task.ResultStore, r *raft.Node) *Server {
	s := &Server{
		Registry:               registry.New(),
		Queue:                  queue,
		Results:                results,
		Raft:                   r,
		HeartbeatTimeout:       DefaultHeartbeatTimeout,
		HeartbeatCheckInterval: DefaultHeartbeatCheckInterval,
		conns:                  make(map[string]workerConn),
		inFlight:               make(map[string]inFlightAssignment),
	}
	r.OnBecomeFollower = s.dropAllWorkers
	return s
}

// dropAllWorkers closes every currently-registered worker's connection.
// Each connection's own read loop (in handleWorkerConn) then runs its
// normal dead-worker cleanup — marking it DEAD and requeuing whatever it
// had in flight — so demotion reuses the exact same fast failure-
// detection path a worker crash would take, rather than needing its own
// cleanup logic.
func (s *Server) dropAllWorkers() {
	s.mu.Lock()
	conns := make([]net.Conn, 0, len(s.conns))
	for _, wc := range s.conns {
		conns = append(conns, wc.conn)
	}
	s.mu.Unlock()

	for _, c := range conns {
		c.Close()
	}
	if len(conns) > 0 {
		log.Printf("leader: stepped down, dropped %d worker connection(s)", len(conns))
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

// handleConn is the single entry point for every connection this node
// accepts, whether it turns out to be a worker or a fellow raft peer:
// the first message's type is what tells them apart, so both kinds of
// traffic can share one TCP listener rather than needing a second port.
func (s *Server) handleConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	go func() {
		<-ctx.Done()
		conn.Close()
	}()

	r := bufio.NewReader(conn)

	env, err := protocol.ReadMessage(r)
	if err != nil {
		log.Printf("leader: reading first message: %v", err)
		return
	}

	switch env.Type {
	case protocol.TypeRequestVote:
		s.Raft.HandleRequestVoteConn(conn, env)
		return
	case protocol.TypeAppendEntries:
		s.Raft.HandleAppendEntriesConn(conn, env)
		return
	case protocol.TypeRegisterWorker, protocol.TypeSubmitTask, protocol.TypeQueryResult:
		// All three need this node to actually be the elected leader,
		// checked once below; a switch case can't easily "fall through"
		// past that check, so each path lives in its own function.
	default:
		log.Printf("leader: unexpected first message type %s", env.Type)
		return
	}

	if !s.Raft.IsLeader() {
		// Not a rejection of the caller itself — just "ask someone
		// else": both worker.Client's and internal/client's discovery
		// loops try the next address in their configured list.
		protocol.WriteMessage(conn, protocol.TypeAck, protocol.Ack{OK: false, Error: "not leader"})
		return
	}

	switch env.Type {
	case protocol.TypeRegisterWorker:
		var reg protocol.RegisterWorker
		if err := json.Unmarshal(env.Payload, &reg); err != nil {
			log.Printf("leader: bad registration payload: %v", err)
			return
		}
		s.handleWorkerConn(ctx, conn, r, reg)
	case protocol.TypeSubmitTask:
		s.handleSubmitTask(ctx, conn, env)
	case protocol.TypeQueryResult:
		s.handleQueryResult(conn, env)
	}
}

func (s *Server) handleSubmitTask(ctx context.Context, conn net.Conn, env protocol.Envelope) {
	var st protocol.SubmitTask
	if err := json.Unmarshal(env.Payload, &st); err != nil {
		protocol.WriteMessage(conn, protocol.TypeAck, protocol.Ack{OK: false, Error: "bad payload"})
		return
	}
	if err := s.Queue.Submit(ctx, st.Task); err != nil {
		protocol.WriteMessage(conn, protocol.TypeAck, protocol.Ack{OK: false, Error: err.Error()})
		return
	}
	log.Printf("leader: client submitted task %s", st.Task.ID)
	protocol.WriteMessage(conn, protocol.TypeAck, protocol.Ack{OK: true})
}

func (s *Server) handleQueryResult(conn net.Conn, env protocol.Envelope) {
	var qr protocol.QueryResult
	if err := json.Unmarshal(env.Payload, &qr); err != nil {
		return
	}
	res, ok := s.Results.Get(qr.TaskID)
	protocol.WriteMessage(conn, protocol.TypeResultStatus, protocol.ResultStatus{Found: ok, Result: res})
}

func (s *Server) handleWorkerConn(ctx context.Context, conn net.Conn, r *bufio.Reader, reg protocol.RegisterWorker) {
	// The registry records the connection's observed remote address
	// rather than trusting a self-reported one from the worker.
	s.Registry.Register(reg.WorkerID, conn.RemoteAddr().String(), time.Now())
	if err := protocol.WriteMessage(conn, protocol.TypeAck, protocol.Ack{OK: true}); err != nil {
		return
	}
	log.Printf("leader: worker %s registered from %s", reg.WorkerID, conn.RemoteAddr())

	assignments := make(chan task.Task, 8)
	s.mu.Lock()
	s.conns[reg.WorkerID] = workerConn{conn: conn, assignments: assignments}
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
		// slower heartbeatMonitor is for. This same path also fires when
		// dropAllWorkers closes the connection on demotion.
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
			wc, ok := s.conns[w.ID]
			s.nextIdx++
			if ok {
				s.inFlight[t.ID] = inFlightAssignment{Task: t, WorkerID: w.ID}
			}
			s.mu.Unlock()
			if ok {
				select {
				case wc.assignments <- t:
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
