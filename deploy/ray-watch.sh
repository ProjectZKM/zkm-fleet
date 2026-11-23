#!/bin/bash
# Keep Ray's existing watchdog active without owning the node or shim lifetimes.
set -euo pipefail
. "${FLEET_ENV:-/etc/zkm-fleet/watchdog.env}"
export FLEET_WATCH_RAY_ONLY=1
while true; do
  if ! "$FLEET_HOME/deploy/fleet-watchdog.sh"; then
    echo '[ray-watch] watchdog failed; retaining Ray and retrying' >&2
  fi
  sleep 30
done
