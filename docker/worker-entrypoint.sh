#!/bin/sh
# Same idea as leader-entrypoint.sh: explicit args (docker-compose) pass
# straight through unchanged; with none (Kubernetes), derive -id from
# this pod's own hostname, which is already unique per pod under both a
# Deployment and a StatefulSet. LEADERS/CONCURRENCY come from the
# Deployment's env — see k8s/worker-deployment.yaml.
set -eu
if [ "$#" -gt 0 ]; then
	exec worker "$@"
fi

exec worker -id "$HOSTNAME" -leaders "$LEADERS" -concurrency "${CONCURRENCY:-2}"
