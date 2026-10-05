#!/bin/bash
# Keeps this machine's Ray node up (run from cron every minute; install.sh adds the line).
# The scheduler runs under docker compose (restart: unless-stopped); prover actors are
# detached and restarted by Ray itself.
set -u
. "${FLEET_ENV:-/etc/zkm-fleet/watchdog.env}"
export PATH="$FLEET_HOME/venv/bin:$PATH" RAY_NODE_IP CUDA_VISIBLE_DEVICES RAY_NUM_CPUS FLEET_WORKDIR
mkdir -p "$FLEET_LOG"
# This machine's own Ray node: its raylet (asking the cluster would answer for the head, not for us).
if ! pgrep -x raylet > /dev/null; then
  echo "$(date -u +%FT%TZ) starting ray $RAY_ROLE"
  "$FLEET_HOME/deploy/ray-start.sh" "$RAY_ROLE" "${RAY_HEAD:-}" >> "$FLEET_LOG/ray.log" 2>&1
fi
