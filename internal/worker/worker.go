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

const heartbeatInterval = 3 * time.Second

// Client is a worker node: it registers with a leader, executes whatever
// tasks the leader assigns it (reusing the phase-1 worker pool locally),
// and reports each result back over the same connection.
type Client struct {
	ID          string
	LeaderAddr  string
	Concurrency int
}

func (c *Client) Run(ctx context.Context) error {
	conn, err := net.Dial("tcp", c.LeaderAddr)
	if err != nil {
		return fmt.Errorf("worker: dial leader %s: %w", c.LeaderAddr, err)
	}
	defer conn.Close()

	go func() {
		<-ctx.Done()
		conn.Close()
	}()

	if err := protocol.WriteMessage(conn, protocol.TypeRegisterWorker, protocol.RegisterWorker{WorkerID: c.ID}); err != nil {
		return fmt.Errorf("worker: register: %w", err)
	}

	r := bufio.NewReader(conn)
	env, err := protocol.ReadMessage(r)
	if err != nil {
		return fmt.Errorf("worker: reading registration ack: %w", err)
	}
	var ack protocol.Ack
	if err := json.Unmarshal(env.Payload, &ack); err != nil || !ack.OK {
		return fmt.Errorf("worker: registration rejected: %s", ack.Error)
	}
	log.Printf("worker: registered with leader %s as %s", c.LeaderAddr, c.ID)

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
			return fmt.Errorf("worker: connection to leader lost: %w", err)
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
