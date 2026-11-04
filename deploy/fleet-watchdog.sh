#!/bin/bash
# This machine's local Ray plus outbound fleet-node; control hosts need neither.
set -euo pipefail
. "${FLEET_ENV:-/etc/zkm-fleet/watchdog.env}"
export PATH="$FLEET_HOME/venv/bin:$PATH" CUDA_VISIBLE_DEVICES RAY_NUM_CPUS FLEET_WORKDIR
export RAY_METRICS_EXPORT_PORT RAY_MIN_WORKER_PORT RAY_MAX_WORKER_PORT
mkdir -p "$FLEET_LOG"
exec 8>"$FLEET_WORKDIR/watchdog.lock"
flock -n 8 || exit 0
FOUND=0
for PID in $(pgrep -x -u "$(id -u)" raylet || true); do
  case "$(readlink "/proc/$PID/exe")" in "$FLEET_HOME/venv/"*) FOUND=1;; esac
done
if [ "$FOUND" = 0 ]; then
  "$FLEET_HOME/deploy/ray-start.sh" >> "$FLEET_LOG/ray.log" 2>&1
fi
# node-start's lock prevents duplicates, including a separately supervised agent.
if [ "${FLEET_NODE_SUPERVISED:-0}" != 1 ]; then
nohup env FLEET_ENV="${FLEET_ENV:-/etc/zkm-fleet/watchdog.env}" "$FLEET_HOME/deploy/node-start.sh" \
  8>&- >> "$FLEET_LOG/node.log" 2>&1 < /dev/null &
fi
