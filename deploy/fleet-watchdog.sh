#!/bin/bash
# For machines without systemd access: run from cron every minute.
#   * * * * * /opt/zkm-fleet/deploy/fleet-watchdog.sh >> /var/log/zkm-fleet/watchdog.log 2>&1
# Settings come from $FLEET_ENV (default /etc/zkm-fleet/watchdog.env):
#   FLEET_HOME, FLEET_ETC, RAY_ROLE (head|worker|none), RAY_HEAD, CUDA_VISIBLE_DEVICES
# The scheduler is not watched here: it runs under docker compose (restart: unless-stopped).
set -u
. "${FLEET_ENV:-/etc/zkm-fleet/watchdog.env}"
H=$FLEET_HOME; E=$FLEET_ETC; L=${FLEET_LOG:-/var/log/zkm-fleet}
mkdir -p "$L"
ts(){ date -u +%FT%TZ; }
if [ "${RAY_ROLE:-none}" != none ] && ! "$H/venv/bin/ray" status --address 127.0.0.1:6379 > /dev/null 2>&1; then
  if [ "$RAY_ROLE" = head ] || [ -n "${RAY_HEAD:-}" ]; then
    echo "$(ts) starting ray $RAY_ROLE"
    PATH="$H/venv/bin:$PATH" "$H/deploy/ray-start.sh" "$RAY_ROLE" "${RAY_HEAD:-}" >> "$L/ray.log" 2>&1
  fi
fi
if [ "${RAY_ROLE:-none}" = head ] && ! pgrep -f "python -m fleet_bridge run" > /dev/null; then
  echo "$(ts) starting bridge"
  (cd "$H/bridge" && setsid "$H/venv/bin/python" -m fleet_bridge run --services "$E/services.yaml" --address 127.0.0.1:6379 \
     >> "$L/bridge.log" 2>&1 < /dev/null &)
fi
