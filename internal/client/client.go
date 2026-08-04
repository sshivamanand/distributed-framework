// Package client is the CLI's caller: submit a task to, and query a
// result from, whichever node in a configured address list is currently
// the elected leader.
package client

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/sshivamanand/distributed-task-framework/internal/protocol"
	"github.com/sshivamanand/distributed-task-framework/internal/task"
)

const dialTimeout = 500 * time.Millisecond

// Client is a one-shot caller, unlike worker.Client: each Submit or
// QueryResult call dials, sends exactly one request, reads exactly one
// response, and closes — there's no long-lived session to keep open,
// since the CLI process exits after a single command.
type Client struct {
	LeaderAddrs []string
}

func (c *Client) Submit(t task.Task) error {
	env, err := c.call(protocol.TypeSubmitTask, protocol.SubmitTask{Task: t})
	if err != nil {
		return err
	}
	var ack protocol.Ack
	if err := json.Unmarshal(env.Payload, &ack); err != nil {
		return fmt.Errorf("decoding ack: %w", err)
	}
	if !ack.OK {
		return fmt.Errorf("leader rejected task: %s", ack.Error)
	}
	return nil
}

// QueryResult reports whether the current leader has a result for
// taskID. Found is false both for a task still queued/in-flight and for
// one this leader never heard of at all — including a task that
// completed on a *previous* leader before a failover, since results
// aren't replicated. The caller can't tell those cases apart.
func (c *Client) QueryResult(taskID string) (task.Result, bool, error) {
	env, err := c.call(protocol.TypeQueryResult, protocol.QueryResult{TaskID: taskID})
	if err != nil {
		return task.Result{}, false, err
	}
	var status protocol.ResultStatus
	if err := json.Unmarshal(env.Payload, &status); err != nil {
		return task.Result{}, false, fmt.Errorf("decoding result status: %w", err)
	}
	return status.Result, status.Found, nil
}

// call tries each address in LeaderAddrs in turn — the same discovery
// pattern worker.Client uses, just for a single request/response instead
// of a long-lived session — until one doesn't reject it as "not leader".
func (c *Client) call(msgType protocol.MessageType, payload any) (protocol.Envelope, error) {
	var lastErr error
	for _, addr := range c.LeaderAddrs {
		env, err := callOnce(addr, msgType, payload)
		if err != nil {
			lastErr = err
			continue
		}
		if env.Type == protocol.TypeAck {
			var ack protocol.Ack
			if json.Unmarshal(env.Payload, &ack) == nil && !ack.OK && ack.Error == "not leader" {
				continue // this node isn't the leader; try the next address
			}
		}
		return env, nil
	}
	if lastErr != nil {
		return protocol.Envelope{}, fmt.Errorf("no leader found among %v: %w", c.LeaderAddrs, lastErr)
	}
	return protocol.Envelope{}, fmt.Errorf("no leader found among %v", c.LeaderAddrs)
}

func callOnce(addr string, msgType protocol.MessageType, payload any) (protocol.Envelope, error) {
	conn, err := net.DialTimeout("tcp", addr, dialTimeout)
	if err != nil {
		return protocol.Envelope{}, err
	}
	defer conn.Close()

	if err := protocol.WriteMessage(conn, msgType, payload); err != nil {
		return protocol.Envelope{}, err
	}
	conn.SetReadDeadline(time.Now().Add(dialTimeout))
	return protocol.ReadMessage(bufio.NewReader(conn))
}
