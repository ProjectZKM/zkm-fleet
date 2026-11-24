#!/bin/bash
# Runs a Ziren GPU server and an app's own worker in one container.
#
#   fleet-gpu-worker WORKER [ARG...]
#
# The container's environment is the GPU server's. The worker gets its own: PATH, HOME,
# the KEY=VALUE lines of the private file WORKER_ENV_FILE, then every FLEET_* value,
# FLEET_GPU_ENDPOINT and RAYON_NUM_THREADS (WORKER_THREADS), which the file cannot replace.
# The worker starts once the server answers ready; from then on /healthz answers 200 on
# HEALTH_PORT (default 8080). The container ends when either process does. On SIGTERM
# the worker is told to stop, and the server runs until the worker has exited.
set -euo pipefail

[ $# -ge 1 ] || { echo "usage: fleet-gpu-worker WORKER [ARG...]" >&2; exit 2; }
export ZIREN_SERVER_PORT=${ZIREN_SERVER_PORT:-3000}
endpoint="http://127.0.0.1:$ZIREN_SERVER_PORT/twirp/"

worker_env=("PATH=$PATH" "HOME=${HOME:-/tmp}")
if [ -n "${WORKER_ENV_FILE:-}" ]; then
  number=0
  while IFS= read -r line || [ -n "$line" ]; do
    number=$((number + 1))
    line=${line#"${line%%[![:space:]]*}"}
    line=${line%"${line##*[![:space:]]}"}
    [[ -z $line || $line == \#* ]] && continue
    line=${line#export }
    # One plain word or one quoted string; nothing is expanded. A message names the
    # line, never its content.
    if [[ $line =~ ^([A-Za-z_][A-Za-z0-9_]*)=\"([^\"\\]*)\"$ || $line =~ ^([A-Za-z_][A-Za-z0-9_]*)=\'([^\']*)\'$
        || $line =~ ^([A-Za-z_][A-Za-z0-9_]*)=([^[:space:]\"\'\\#]*)$ ]]; then
      worker_env+=("${BASH_REMATCH[1]}=${BASH_REMATCH[2]}")
    else
      echo "fleet-gpu-worker: $WORKER_ENV_FILE line $number is not KEY=VALUE with one plain word or one quoted string" >&2
      exit 2
    fi
  done < "$WORKER_ENV_FILE"
fi
for key in "${!FLEET_@}"; do worker_env+=("$key=${!key}"); done
worker_env+=("FLEET_GPU_ENDPOINT=$endpoint")
[ -z "${WORKER_THREADS:-}" ] || worker_env+=("RAYON_NUM_THREADS=$WORKER_THREADS")

server='' worker=''
stop() { kill -TERM "${worker:-$server}" 2>/dev/null || true; }
trap stop TERM INT

(cd / && exec zkm-gpu-server) &
server=$!
ready() {
  [ "$(curl -fsS -m 3 -X POST -H 'content-type: application/protobuf' --data-binary '' \
    "${endpoint}api.ProverService/Ready" 2>/dev/null | od -An -tx1 | tr -d ' \n')" = 0801 ]
}
until ready; do
  kill -0 "$server" 2>/dev/null || { echo "fleet-gpu-worker: the GPU server ended before it was ready" >&2; exit 1; }
  sleep 2
done

env -i "${worker_env[@]}" "$@" &
worker=$!
health=$(mktemp -d)
touch "$health/healthz"
busybox httpd -f -p "${HEALTH_PORT:-8080}" -h "$health" &

# A signal interrupts the wait with neither ended: keep waiting.
ended='' code=0
while [ -z "${ended:-}" ]; do
  code=0
  wait -n -p ended "$server" "$worker" || code=$?
done
if [ "$ended" = "$server" ]; then
  echo "fleet-gpu-worker: the GPU server ended; stopping the worker" >&2
  kill -TERM "$worker" 2>/dev/null || true
  wait "$worker" || true
  exit 1
fi
kill -TERM "$server" 2>/dev/null || true
wait "$server" || true
exit "$code"
