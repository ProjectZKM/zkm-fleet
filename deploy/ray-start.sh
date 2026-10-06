#!/bin/bash
# Ray with fixed ports, so the cluster firewall is a short allow-list between machines.
#   ray-start.sh head | worker <head-ip>
# CUDA_VISIBLE_DEVICES = the cards Ray may hand out here (empty: none); RAY_NUM_CPUS = the CPUs (default: all);
# RAY_NODE_IP = this machine's cluster address; FLEET_WORKDIR = this node's workspace.
set -euo pipefail
COMMON=(--node-manager-port=6380 --object-manager-port=6381 --min-worker-port=10002 --max-worker-port=19999
        --metrics-export-port=8080 --dashboard-agent-listen-port=52365 --disable-usage-stats)
# Ray's object store and the GPU server both use /dev/shm; Ray sizes its share itself unless told.
[ -n "${RAY_OBJECT_STORE_BYTES:-}" ] && COMMON+=(--object-store-memory="$RAY_OBJECT_STORE_BYTES")
# The node's workspace: Ray's session files (logs, sockets) and spilled objects, off the root disk.
# Ray puts unix sockets under <temp-dir>/session_<timestamp>/sockets/, and a socket path is
# capped at 107 bytes: Ray gets a short link into the workspace, not the workspace path itself.
if [ -n "${FLEET_WORKDIR:-}" ]; then
  mkdir -p "$FLEET_WORKDIR/ray" "$FLEET_WORKDIR/spill"
  LINK="/tmp/zkf-ray-$(id -u)"
  ln -sfn "$FLEET_WORKDIR/ray" "$LINK"
  COMMON+=(--temp-dir="$LINK" --object-spilling-directory="$FLEET_WORKDIR/spill")
fi
# CPUs Ray may hand out here (a machine shared with other work keeps the rest).
[ -n "${RAY_NUM_CPUS:-}" ] && COMMON+=(--num-cpus="$RAY_NUM_CPUS")
[ -n "${RAY_NODE_IP:-}" ] && COMMON+=(--node-ip-address="$RAY_NODE_IP")
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
case "${1:-}" in
  head)   ray start --head --port=6379 --include-dashboard=false "${COMMON[@]}" ;;
  worker) ray start --address="${2:?head ip}:6379" "${COMMON[@]}" ;;
  *) echo "usage: $0 head | worker <head-ip>"; exit 2 ;;
esac
