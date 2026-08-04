package worker

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/sshivamanand/distributed-task-framework/internal/protocol"
	"github.com/sshivamanand/distributed-task-framework/internal/task"
)

const (
	heartbeatInterval = 3 * time.Second
	dialTimeout       = 500 * time.Millisecond
	// reconnectDelay paces retries after a lost/failed connection so a
	// worker doesn't busy-loop hammering a cluster with no leader yet
	// (e.g. mid-election).
	reconnectDelay = 500 * time.Millisecond
)

// Client is a worker node: it discovers whichever node in LeaderAddrs is
// currently the elected leader, registers with it, executes whatever
// tasks it's assigned (reusing the phase-1 worker pool locally), and
// reports each result back over the same connection. If that connection
// is ever lost — the leader crashed, or a Raft election demoted it — it
// re-runs discovery from scratch, since whoever is leader may have
// changed.
type Client struct {
	ID          string
	LeaderAddrs []string
	Concurrency int
}

// Run discovers and registers with the current leader, executes tasks
// until that connection is lost, and then repeats, until ctx is done.
func (c *Client) Run(ctx context.Context) error {
	for {
		if err := c.runOnce(ctx); err != nil {
			log.Printf("worker: %v", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(reconnectDelay):
		}
	}
}

func (c *Client) runOnce(ctx context.Context) error {
	conn, r, err := c.registerWithLeader()
	if err != nil {
		return err
	}
	defer conn.Close()

	go func() {
		<-ctx.Done()
		conn.Close()
	}()

	queue := task.NewQueue(c.Concurrency)
	results := task.NewResultStore()

	poolDone := make(chan struct{})
	go func() {
		defer close(poolDone)
		task.RunWorkerPool(ctx, c.Concurrency, queue, results, func(res task.Result) {
			if err := protocol.WriteMessage(conn, protocol.TypeTaskCompleted, protocol.TaskCompleted{Result: res}); err != nil {
				log.Printf("worker: reporting result for %s: %v", res.TaskID, err)
			}
		})
	}()

	go c.sendHeartbeats(ctx, conn)

	for {
		env, err := protocol.ReadMessage(r)
		if err != nil {
			queue.Close()
			<-poolDone
			return fmt.Errorf("connection to leader lost: %w", err)
		}
		switch env.Type {
		case protocol.TypeTaskAssignment:
			var ta protocol.TaskAssignment
			if err := json.Unmarshal(env.Payload, &ta); err != nil {
				log.Printf("worker: bad task assignment: %v", err)
				continue
			}
			if err := queue.Submit(ctx, ta.Task); err != nil {
				log.Printf("worker: submitting task %s: %v", ta.Task.ID, err)
			}
		default:
			log.Printf("worker: unexpected message type %s", env.Type)
		}
	}
}

// registerWithLeader tries each address in LeaderAddrs in turn until one
// both accepts the connection and its Ack says OK — that's the current
// leader. A node that's up but not currently leader still Acks, just
// with OK: false, so a rejection isn't an error, just "try the next
// one." The bufio.Reader used for the handshake is returned and reused
// for the rest of the connection's life: a fresh one could otherwise
// silently drop bytes the OS had already delivered into the old reader's
// internal buffer (e.g. a TaskAssignment arriving right on the heels of
// the Ack).
func (c *Client) registerWithLeader() (net.Conn, *bufio.Reader, error) {
	for _, addr := range c.LeaderAddrs {
		conn, err := net.DialTimeout("tcp", addr, dialTimeout)
		if err != nil {
			continue
		}
		if err := protocol.WriteMessage(conn, protocol.TypeRegisterWorker, protocol.RegisterWorker{WorkerID: c.ID}); err != nil {
			conn.Close()
			continue
		}
		conn.SetReadDeadline(time.Now().Add(dialTimeout))
		r := bufio.NewReader(conn)
		env, err := protocol.ReadMessage(r)
		if err != nil {
			conn.Close()
			continue
		}
		conn.SetReadDeadline(time.Time{})
		var ack protocol.Ack
		if err := json.Unmarshal(env.Payload, &ack); err != nil || !ack.OK {
			conn.Close()
			continue // not the leader (or rejected); try the next address
		}
		log.Printf("worker: registered with leader %s as %s", addr, c.ID)
		return conn, r, nil
	}
	return nil, nil, fmt.Errorf("no leader found among %v", c.LeaderAddrs)
}

func (c *Client) sendHeartbeats(ctx context.Context, conn net.Conn) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := protocol.WriteMessage(conn, protocol.TypeHeartbeat, protocol.Heartbeat{WorkerID: c.ID}); err != nil {
				return
			}
		}
	}
}
