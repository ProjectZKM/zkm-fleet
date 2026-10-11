#!/bin/bash
# May be supervised by systemd or the cron watchdog. No inbound node API.
set -euo pipefail
. "${FLEET_ENV:-/etc/zkm-fleet/watchdog.env}"
exec 9>"$FLEET_WORKDIR/node-agent.lock"
flock -n 9 || exit 0
export RUST_LOG=${RUST_LOG:-info}
ARGS=(--scheduler "$FLEET_SCHEDULER" --cluster-token-file "$FLEET_ETC/cluster.token"
      --name "$FLEET_NODE_NAME" --services "$FLEET_ETC/services.yaml" --profiles "$FLEET_ETC/jobs.yaml"
      --jobs-dir "$FLEET_JOBS_DIR" --bridge "$FLEET_HOME/bridge" --python "$FLEET_HOME/venv/bin/python")
[ -z "${FLEET_SCHEDULER_CA:-}" ] || ARGS+=(--scheduler-ca-file "$FLEET_SCHEDULER_CA")
# One approved root per line. Keep roots configured when proof actors are disabled.
if [ -n "${FLEET_PROOF_STORAGE_ROOTS_FILE:-}" ]; then
    ARGS+=(--proof-node-binary "$FLEET_HOME/bin/fleet-ziren-node")
    while IFS= read -r root || [ -n "$root" ]; do
        [ -z "$root" ] || ARGS+=(--proof-storage-root "$root")
    done < "$FLEET_PROOF_STORAGE_ROOTS_FILE"
fi
exec "$FLEET_HOME/bin/fleet-node" "${ARGS[@]}"
