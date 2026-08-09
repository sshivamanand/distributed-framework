#!/usr/bin/env bash
# demo.sh — automates the project's headline demo: bring up the cluster,
# submit a task, kill the *actual elected leader* mid-run, watch the
# cluster re-elect and keep serving work. Meant to be run while
# recording a terminal for the README (part 4/4).
#
# Requires Docker Desktop (or another Docker daemon) running and the
# `docker compose` CLI plugin. Leaves the cluster running at the end for
# further exploration — run `docker compose down` when you're done.
set -euo pipefail
cd "$(dirname "$0")"

CLIENT="docker compose run --rm client -leaders leader1:8080,leader2:8080,leader3:8080"

banner() {
	echo
	echo "=== $1 ==="
	echo
}

banner "Bringing up the cluster (3 leader-eligible nodes + 3 workers)"
docker compose up -d --build
sleep 5
docker compose ps

banner "Submitting task #1"
$CLIENT submit -id demo-task-1 echo "hello before failover"
sleep 1
$CLIENT result demo-task-1

banner "Finding the current leader"
# Only the elected leader ever logs a client's submission, so whichever
# service's log contains this line is the current leader — no log-
# timestamp parsing or extra tooling required.
LEADER_LINE=$(docker compose logs leader1 leader2 leader3 2>&1 | grep "client submitted task demo-task-1" | tail -1)
LEADER_SERVICE=$(echo "$LEADER_LINE" | sed -E 's/^(leader[0-9]+)-1.*/\1/')
echo "Current leader is: $LEADER_SERVICE"
sleep 2

banner "Killing the leader ($LEADER_SERVICE) mid-run"
docker compose kill "$LEADER_SERVICE"
sleep 6

banner "Re-election in the container logs"
docker compose logs leader1 leader2 leader3 2>&1 | grep -E "won election|stepping down|registered" | tail -10

banner "Submitting task #2 (after failover — proves the cluster still works, not just that it's alive)"
$CLIENT submit -id demo-task-2 echo "still alive after failover"
sleep 1
$CLIENT result demo-task-2

banner "Done. Cluster is still running — run 'docker compose down' when finished exploring."
