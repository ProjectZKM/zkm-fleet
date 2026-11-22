#!/bin/bash
# The image owns the SDK worker and its GPU server as one lifecycle/version unit.
set -euo pipefail
if [ "${FLEET_BACKEND:-cuda}" != cuda ]; then
  exec fleet-ziren-node "$@"
fi
pids=()
stop() {
  trap '' TERM INT
  kill -TERM "${pids[@]}" 2>/dev/null || true
  wait "${pids[@]}" 2>/dev/null || true
}
trap stop EXIT
trap 'exit 143' TERM
trap 'exit 130' INT
zkm-gpu-server &
pids+=("$!")
fleet-ziren-node "$@" &
pids+=("$!")
# Any unexpected child exit ends the container; shim owns restart and cleanup.
set +e
wait -n "${pids[@]}"
status=$?
exit "$((status == 0 ? 1 : status))"
