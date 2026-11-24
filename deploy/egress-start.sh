#!/bin/bash
# The node's way out for batch jobs (apps.yaml: batch_egress): makes the internal Docker
# network, starts its proxy, checks that the network reaches nothing of the node, and stays
# with the proxy.
set -euo pipefail
. "${FLEET_ENV:-/etc/zkm-fleet/watchdog.env}"
cd "$FLEET_HOME/bridge"
exec "$FLEET_HOME/venv/bin/python" -m fleet_bridge.egress serve --apps "$FLEET_ETC/apps.yaml"
