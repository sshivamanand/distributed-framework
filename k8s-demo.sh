#!/usr/bin/env bash
# k8s-demo.sh — the Kubernetes counterpart to demo.sh: build the images,
# apply the manifests, submit a task, scale workers up and back down (the
# thing a StatefulSet+Raft election alone can't show — this is what a
# real orchestrator adds), then delete the elected leader's *pod* and
# watch the StatefulSet reschedule it while the survivors re-elect.
#
# Requires a local cluster reachable via `kubectl` — this defaults to
# Docker Desktop's built-in Kubernetes (context "docker-desktop"), which
# shares its image cache with `docker build` directly, so images never
# need to be pushed to a registry or explicitly loaded. Override with
# K8S_CONTEXT if you're using kind/minikube/something else (note: kind
# and minikube keep a separate image store from the host Docker daemon,
# so you'd need `kind load docker-image` or `minikube image load` first).
set -euo pipefail
cd "$(dirname "$0")"

CTX="${K8S_CONTEXT:-docker-desktop}"
NS=distributed-framework
LEADERS="leader-0.leader-headless:8080,leader-1.leader-headless:8080,leader-2.leader-headless:8080"

kctl() { kubectl --context "$CTX" "$@"; }

banner() {
	echo
	echo "=== $1 ==="
	echo
}

run_client() {
	# Each invocation is a one-off pod, same idea as
	# `docker compose run --rm client`.
	kctl -n "$NS" run "client-$(date +%s%N)" --rm --attach --restart=Never \
		--image=distributed-framework-client:latest --image-pull-policy=IfNotPresent \
		-- -leaders "$LEADERS" "$@"
}

banner "Building images"
docker build --target leader -t distributed-framework-leader:latest -f docker/Dockerfile . >/dev/null
docker build --target worker -t distributed-framework-worker:latest -f docker/Dockerfile . >/dev/null
docker build --target client -t distributed-framework-client:latest -f docker/Dockerfile . >/dev/null

banner "Applying manifests"
kctl apply -f k8s/namespace.yaml
kctl apply -f k8s/leader-headless-service.yaml
kctl apply -f k8s/leader-statefulset.yaml
kctl apply -f k8s/worker-deployment.yaml
# kubectl wait on a label selector only checks pods that already exist,
# which is wrong for a StatefulSet: it creates replicas one at a time,
# so an early wait can be satisfied by pod 0 alone before 1 and 2 even
# exist. `rollout status` understands the target replica count instead.
kctl -n "$NS" rollout status statefulset/leader --timeout=60s
kctl -n "$NS" rollout status deployment/worker --timeout=60s
kctl -n "$NS" get pods

banner "Submitting task #1"
run_client submit -id k8s-task-1 echo "hello from kubernetes"
sleep 1
run_client result k8s-task-1

banner "Scaling workers 3 -> 6"
kctl -n "$NS" scale deployment worker --replicas=6
sleep 6
kctl -n "$NS" get pods -l app=worker

banner "Scaling workers 6 -> 2"
kctl -n "$NS" scale deployment worker --replicas=2
sleep 6
kctl -n "$NS" get pods -l app=worker

banner "Finding the current leader"
LEADER_POD=""
for p in leader-0 leader-1 leader-2; do
	if kctl -n "$NS" logs "$p" 2>&1 | grep -q "client submitted task k8s-task-1"; then
		LEADER_POD="$p"
	fi
done
echo "Current leader is: $LEADER_POD"

banner "Deleting the leader pod ($LEADER_POD) mid-run"
kctl -n "$NS" delete pod "$LEADER_POD"
sleep 8
kctl -n "$NS" get pods -l app=leader

banner "Re-election in the container logs"
for p in leader-0 leader-1 leader-2; do
	# A freshly-rescheduled pod may not have logged a transition yet
	# (e.g. it started clean as a Follower and simply stayed one) — that
	# is a legitimate outcome, not a script failure, so don't let grep's
	# no-match exit code trip `set -e`.
	kctl -n "$NS" logs "$p" 2>&1 | grep -E "won election|stepping down" | tail -3 || true
done

banner "Submitting task #2 (after the leader pod was replaced)"
run_client submit -id k8s-task-2 echo "still alive after pod failover"
sleep 1
run_client result k8s-task-2

banner "Done. Cluster is still running — run 'kubectl delete namespace $NS' when finished exploring."
