#!/bin/bash
# Ray with fixed ports, so the cluster firewall is a short allow-list between machines.
# head:   deploy/ray-start.sh head
# worker: deploy/ray-start.sh worker <head-ip>
set -euo pipefail
COMMON=(--node-manager-port=6380 --object-manager-port=6381 --min-worker-port=10002 --max-worker-port=19999
        --metrics-export-port=8080 --dashboard-agent-listen-port=52365)
# Ray's object store and the GPU server both use /dev/shm: cap Ray's share.
OBJ=${RAY_OBJECT_STORE_BYTES:-$((64 * 1024 * 1024 * 1024))}
case "${1:-}" in
  head)   ray start --head --port=6379 --ray-client-server-port=10001 --dashboard-host=127.0.0.1 --dashboard-port=8265 \
            --object-store-memory="$OBJ" "${COMMON[@]}" ;;
  worker) ray start --address="${2:?head ip}:6379" --object-store-memory="$OBJ" "${COMMON[@]}" ;;
  *) echo "usage: $0 head | worker <head-ip>"; exit 2 ;;
esac
