#!/bin/sh
# Lets the same leader image run unchanged under both docker-compose
# (which passes explicit -id/-peers args) and Kubernetes (which doesn't
# — a StatefulSet gives every replica the identical pod spec, so there's
# no per-replica field to put per-replica flags in).
#
# If args were given, just run the binary with them, unchanged.
set -eu
if [ "$#" -gt 0 ]; then
	exec leader "$@"
fi

# Otherwise, derive this node's identity from its own pod hostname. A
# StatefulSet names its pods <name>-<ordinal> (e.g. leader-0), so the
# ordinal is just the suffix after the last '-'. REPLICAS and
# HEADLESS_SERVICE come from the StatefulSet's env — see
# k8s/leader-statefulset.yaml.
ordinal="${HOSTNAME##*-}"

peers=""
i=0
while [ "$i" -lt "$REPLICAS" ]; do
	if [ "$i" != "$ordinal" ]; then
		if [ -n "$peers" ]; then
			peers="${peers},"
		fi
		peers="${peers}leader-${i}.${HEADLESS_SERVICE}:8080"
	fi
	i=$((i + 1))
done

exec leader -id "leader-${ordinal}" -addr :8080 -peers "$peers"
