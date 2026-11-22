#!/bin/bash
# Independently supervised. Node restarts must never stop this service.
set -euo pipefail
. "${FLEET_ENV:-/etc/zkm-fleet/watchdog.env}"
cd "$FLEET_HOME/bridge"
exec "$FLEET_HOME/venv/bin/python" -m fleet_bridge.shim serve --apps "$FLEET_ETC/apps.yaml"
