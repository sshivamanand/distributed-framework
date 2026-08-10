# distributed-task-framework

A fault-tolerant distributed task execution framework in Go, built from scratch to demonstrate real distributed-systems mechanics — not a wrapper around an existing scheduler. Inspired by Celery, Kubernetes Jobs, and Raft, but implemented over a hand-rolled TCP protocol instead of gRPC.

You submit a task (a shell command) to whichever node in a cluster is currently the elected leader. The leader hands it off to a worker to execute and reports the result back. If a worker dies mid-task, the leader notices and reassigns the work. If the *leader* dies, the remaining nodes hold a Raft-inspired election and pick a new one — while the cluster keeps running.

## Architecture

- **Leader-eligible nodes** (`cmd/leader`) — a cluster of nodes that run leader election among themselves. Whichever one wins accepts task submissions, maintains a queue, tracks a worker registry, detects worker failures via heartbeat timeout, and reassigns their tasks. The others sit as followers, ready to take over.
- **Worker nodes** (`cmd/worker`) — register with whichever node is currently leader, execute assigned tasks, report results, and send periodic heartbeats. If their connection drops (leader crash, or an election demoting it), they rediscover the current leader on their own.
- **Client** (`cmd/client`) — a CLI to submit tasks and query results from outside the cluster.
- **Transport** — plain TCP with a newline-delimited JSON wire protocol (`internal/protocol`). Every message is one JSON object per line; framing is just "read until `\n`". No gRPC, no external serialization library — the whole protocol is about 100 lines.

```
client ──submit/query──▶ leader (elected) ──assign──▶ worker
                             ▲  │                        │
                    RequestVote/  ◀──heartbeat/result─────┘
                    AppendEntries
                             │
                    other leader-eligible nodes (followers)
```

## How leader election works

Each leader-eligible node runs a small Raft-inspired state machine (`internal/raft`): `Follower` → `Candidate` → `Leader`, with a randomized election timeout (1.5–3s) and a 400ms heartbeat once elected.

- **Term numbers prevent two simultaneous leaders.** Every RPC carries a term. A node that sees a higher term than its own immediately steps down and adopts it — even a sitting leader, on its next heartbeat response. Each node grants at most one vote per term, and winning requires a strict majority of the whole cluster, so at most one leader can exist in any given term.
- **Randomized timeouts prevent infinite split votes.** If two nodes start elections in the same term and neither wins, they simply retry — but each retry draws a fresh, independent random timeout, so the tie doesn't reproduce indefinitely.
- **Quorum is computed against the configured cluster size, not the currently-reachable set.** A 3-node cluster tolerates exactly 1 failure: losing 1 still leaves 2-of-3, a majority — but losing 2 leaves a lone node that can never reach majority, so the cluster correctly refuses to elect anyone rather than risk a partitioned minority declaring itself leader.

**Deliberately not implemented: log replication.** Real Raft replicates a command log across nodes so a new leader can reconstruct exact state. This project implements election only — enough to demonstrate the consensus mechanics — and is honest about the consequence: a newly-elected leader starts with an empty queue. If the old leader crashes with tasks queued or in-flight, that work is lost; the *cluster* recovers, but that specific work doesn't. This is what gives the system **at-least-once**, not exactly-once, task execution — a real, common distributed-systems tradeoff (availability of the control plane vs. durability of in-flight work), not an oversight.

## Running it

```bash
docker compose up
```

Brings up 3 leader-eligible nodes and 3 workers. Submit a task and check its result from outside the cluster:

```bash
docker compose run --rm client -leaders leader1:8080,leader2:8080,leader3:8080 submit echo "hello"
docker compose run --rm client -leaders leader1:8080,leader2:8080,leader3:8080 result <task-id>
```

### The failover demo

```bash
./demo.sh
```

Brings the cluster up, submits a task, finds and kills whichever container is *actually* the elected leader (by checking which one logged the submission — only the real leader does), and watches the survivors re-elect a new one and keep serving work. Real output from an actual run:

<details>
<summary>demo.sh output (click to expand)</summary>

```
$ ./demo.sh

=== Bringing up the cluster (3 leader-eligible nodes + 3 workers) ===
 ...
 Container distributed-framework-leader1-1 Started
 Container distributed-framework-leader2-1 Started
 Container distributed-framework-leader3-1 Started
 Container distributed-framework-worker1-1 Started
 Container distributed-framework-worker2-1 Started
 Container distributed-framework-worker3-1 Started

=== Submitting task #1 ===

demo-task-1
status: COMPLETED
output: hello before failover

=== Finding the current leader ===

Current leader is: leader1

=== Killing the leader (leader1) mid-run ===

 Container distributed-framework-leader1-1 Killed

=== Re-election in the container logs ===

leader2-1  | raft[leader2]: won election for term 2 with 2/3 votes
leader2-1  | leader: worker worker3 registered from 172.23.0.6:40178
leader2-1  | leader: worker worker1 registered from 172.23.0.2:56858
leader2-1  | leader: worker worker2 registered from 172.23.0.5:58208
leader3-1  | raft[leader3]: stepping down to FOLLOWER, adopting term 2
leader1-1  | raft[leader1]: won election for term 1 with 3/3 votes

=== Submitting task #2 (after failover — proves the cluster still works, not just that it's alive) ===

demo-task-2
status: COMPLETED
output: still alive after failover

=== Done. Cluster is still running — run 'docker compose down' when finished exploring. ===
```

Full unabridged output: [demo-output.txt](demo-output.txt).

</details>

## Kubernetes

The same images also run on a real cluster (`k8s/`) — leader-eligible nodes as a `StatefulSet` (they need stable identity for peer addressing), workers as a plain `Deployment` (they don't, which is what makes them freely scalable). A small entrypoint wrapper (`docker/leader-entrypoint.sh` / `worker-entrypoint.sh`) derives each pod's identity from its own hostname when no explicit flags are given — the same image works unmodified under both Compose and Kubernetes.

**Kubernetes' own leader election was deliberately not used here.** `internal/raft` still runs the election, completely unaware it's in a pod — Kubernetes is a deployment and scaling layer on top, not a replacement for it.

```bash
./k8s-demo.sh
```

Builds the images, applies the manifests to whatever cluster your current `kubectl` context points at (defaults to Docker Desktop's built-in Kubernetes), submits a task, **scales workers 3 → 6 → 2** live, then deletes the actual current leader's *pod* and watches the survivors re-elect while Kubernetes reschedules a clean replacement:

<details>
<summary>k8s-demo.sh output (click to expand)</summary>

```
$ ./k8s-demo.sh

=== Applying manifests ===

NAME                      READY   STATUS    RESTARTS   AGE
leader-0                  1/1     Running   0          5s
leader-1                  1/1     Running   0          4s
leader-2                  1/1     Running   0          2s
worker-58d8db4b4b-4rx27   1/1     Running   0          5s
worker-58d8db4b4b-7v99c   1/1     Running   0          5s
worker-58d8db4b4b-9r8h8   1/1     Running   0          5s

=== Submitting task #1 ===

status: COMPLETED
output: hello from kubernetes

=== Scaling workers 3 -> 6 ===
 ... 6 pods Running ...

=== Scaling workers 6 -> 2 ===
 ... 2 pods Running ...

=== Finding the current leader ===

Current leader is: leader-0

=== Deleting the leader pod (leader-0) mid-run ===

pod "leader-0" deleted from distributed-framework namespace
NAME       READY   STATUS    RESTARTS   AGE
leader-0   1/1     Running   0          8s   ← same name, a brand-new pod
leader-1   1/1     Running   0          30s
leader-2   1/1     Running   0          28s

=== Re-election in the container logs ===

raft[leader-1]: won election for term 3 with 2/3 votes

=== Submitting task #2 (after the leader pod was replaced) ===

status: COMPLETED
output: still alive after pod failover
```

Full unabridged output: [k8s-demo-output.txt](k8s-demo-output.txt).

</details>

## Testing

```bash
go test -race ./...
```

Every concurrent package is race-clean, including real-TCP integration tests: a 3-node raft cluster proving exactly one leader is ever elected and that killing it produces a *different* elected leader from the survivors; a lone node proven to never win without quorum; and a full-stack test where two workers reconnect to a newly-elected leader after the old one is killed and complete a fresh task.

## Project layout

```
cmd/leader/     leader-eligible node binary
cmd/worker/     worker node binary
cmd/client/     CLI: submit tasks, query results
internal/task/     in-memory task queue, worker pool, result store
internal/protocol/ newline-delimited JSON wire format
internal/registry/ mutex-guarded worker registry
internal/raft/     Raft-inspired leader election
internal/leader/   leader-side server: dispatch, failure detection, election wiring
internal/worker/   worker-side client: registration, execution, leader discovery
internal/client/   one-shot CLI caller
docker/            Dockerfile (multi-stage, multi-target) + entrypoint wrapper scripts
docker-compose.yml one-command cluster
demo.sh            automated Compose failover demo
k8s/               StatefulSet (leaders) + Deployment (workers) manifests
k8s-demo.sh        automated Kubernetes failover + scaling demo
```

## Scope

Deliberately out of scope, as documented tradeoffs rather than gaps:

- **gRPC** — plain TCP is the point; owning the wire format is what makes the protocol explainable.
- **A database** — results are in-memory only; the project is about distributed-systems mechanics, not persistence.
- **Multiple scheduling policies** — round-robin only.
- **Raft log replication** — election only, as explained above.
- **Kubernetes' own leader election** — `internal/raft` still does this, on purpose; Kubernetes here is a deployment/scaling layer, not a replacement for the part of the project that's actually about distributed systems.
