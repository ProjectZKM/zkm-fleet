#!/bin/bash
# Independent local Ray instance, never joined to another machine.
# CUDA_VISIBLE_DEVICES limits managed cards; RAY_NUM_CPUS limits managed CPUs.
# RAY_METRICS_EXPORT_PORT and RAY_MIN/MAX_WORKER_PORT move ports another service holds.
set -euo pipefail
COMMON=(--node-manager-port=6380 --object-manager-port=6381
        --min-worker-port="${RAY_MIN_WORKER_PORT:-10002}" --max-worker-port="${RAY_MAX_WORKER_PORT:-19999}"
        --metrics-export-port="${RAY_METRICS_EXPORT_PORT:-8080}" --dashboard-agent-listen-port=52365 --disable-usage-stats)
# Ray's object store and the GPU server both use /dev/shm; Ray sizes its share itself unless told.
[ -n "${RAY_OBJECT_STORE_BYTES:-}" ] && COMMON+=(--object-store-memory="$RAY_OBJECT_STORE_BYTES")
# The node's workspace: Ray's session files (logs, sockets) and spilled objects, off the root disk.
# Ray puts unix sockets under <temp-dir>/session_<timestamp>/sockets/, and a socket path is
# capped at 107 bytes: Ray gets a short link into the workspace, not the workspace path itself.
if [ -n "${FLEET_WORKDIR:-}" ]; then
  mkdir -p "$FLEET_WORKDIR/ray" "$FLEET_WORKDIR/spill"
  LINK="${FLEET_RAY_LINK:-/tmp/zkf-ray-$(id -u)}"
  ln -sfn "$FLEET_WORKDIR/ray" "$LINK"
  COMMON+=(--temp-dir="$LINK" --object-spilling-directory="$FLEET_WORKDIR/spill")
fi
# CPUs Ray may hand out here (a machine shared with other work keeps the rest).
[ -n "${RAY_NUM_CPUS:-}" ] && COMMON+=(--num-cpus="$RAY_NUM_CPUS")
COMMON+=(--node-ip-address=127.0.0.1)
if [ -n "${CUDA_VISIBLE_DEVICES:-}" ]; then
  export CUDA_VISIBLE_DEVICES
  COMMON+=(--num-gpus="$(echo "$CUDA_VISIBLE_DEVICES" | tr ',' '\n' | grep -c .)")
else
  unset CUDA_VISIBLE_DEVICES
  COMMON+=(--num-gpus=0)
fi
# `ray stop` can leave a gcs_server holding the head port; the next head start then fails
# ("Session name ... does not match persisted value") every minute until it is killed. This
# script only runs when this machine has no raylet, so any gcs_server / raylet from this
# venv, of this user, is such a leftover.
VENV=$(dirname "$(dirname "$(readlink -f "$(command -v ray)")")")
for n in gcs_server raylet; do
  for p in $(pgrep -x -u "$(id -u)" "$n"); do
    case "$(readlink "/proc/$p/exe")" in "$VENV"/*) echo "killing a leftover $n ($p)"; kill -9 "$p" ;; esac
  done
done
[ $# -eq 0 ] || { echo "Ray is local; this script accepts no head/worker arguments" >&2; exit 2; }
ray start --head --port=6379 --include-dashboard=false "${COMMON[@]}"
