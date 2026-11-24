#!/bin/bash
# Runs an app's own worker beside its GPU server in one container, as the legacy executor
# runs them on a node: the worker starts once the server is ready; it gets Fleet's identity
# and its private environment file, and none of the server's settings; on a stop it finishes
# against a live server, which goes after it. Either one exiting ends the container.
# The container's command is the worker's: /ABSOLUTE/WORKER [ARGUMENT...]
set -euo pipefail
[[ ${1:-} == /* && -x $1 ]] || { echo "the container's command must be the worker's absolute path in this image" >&2; exit 64; }
port=${ZIREN_SERVER_PORT:-3000}

# The worker's environment. The container's own is the GPU server's.
worker_env=("PATH=$PATH" "HOME=${HOME:-/root}")
if [[ -n ${FLEET_WORKER_ENV_FILE:-} ]]; then
  # KEY=VALUE lines; a value is one bare word or one quoted string, and nothing in it is expanded.
  assignment='^(export[[:space:]]+)?([A-Za-z_][A-Za-z0-9_]*)=(.*)$'
  double='^"([^"\\]*)"[[:space:]]*(#.*)?$'
  single="^'([^']*)'[[:space:]]*(#.*)?\$"
  bare='^([^[:space:]"'\''\\#]*)[[:space:]]*(#.*)?$'
  number=0
  while IFS= read -r line || [[ -n $line ]]; do
    number=$((number + 1))
    line=${line#"${line%%[![:space:]]*}"}
    [[ -z $line || $line == \#* ]] && continue
    [[ $line =~ $assignment ]] || { echo "worker environment file: line $number is not KEY=VALUE" >&2; exit 64; }
    key=${BASH_REMATCH[2]} value=${BASH_REMATCH[3]}
    [[ $value =~ $double || $value =~ $single || $value =~ $bare ]] \
      || { echo "worker environment file: line $number needs one bare or plainly quoted value" >&2; exit 64; }
    worker_env+=("$key=${BASH_REMATCH[1]}")
  done < "$FLEET_WORKER_ENV_FILE"
fi
# Fleet's identity for the worker, which its file cannot replace.
for name in $(compgen -v FLEET_); do
  [[ $name == FLEET_WORKER_* ]] || worker_env+=("$name=${!name}")
done
worker_env+=("FLEET_WORKER_ROLE=app" "FLEET_EXIT_WITH_PARENT=true" "FLEET_GPU_ENDPOINT=http://127.0.0.1:$port/twirp/"
  "RUST_LOG=${FLEET_WORKER_RUST_LOG:-info}" "RAYON_NUM_THREADS=${FLEET_CPUS:-1}")

server='' worker=''
stop() {
  trap '' TERM INT
  for pid in $worker $server; do
    kill -TERM "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  done
}
trap stop EXIT
trap 'exit 143' TERM
trap 'exit 130' INT

zkm-gpu-server &
server=$!
ready() {
  [[ $(curl -fsS -m 3 -X POST -H 'content-type: application/protobuf' --data-binary '' \
      "http://127.0.0.1:$port/twirp/api.ProverService/Ready" 2>/dev/null | od -An -tx1 | tr -d ' \n') == 0801 ]]
}
timeout=${FLEET_WORKER_READY_TIMEOUT_S:-600}
deadline=$((SECONDS + ${timeout%.*}))
until ready; do
  kill -0 "$server" 2>/dev/null || { echo 'the GPU server exited before it was ready' >&2; exit 1; }
  ((SECONDS < deadline)) || { echo 'the GPU server did not become ready in time' >&2; exit 1; }
  sleep 2 &
  wait $! || true
done

env -i "${worker_env[@]}" "$@" &
worker=$!
set +e
wait -n "$server" "$worker"
status=$?
exit "$((status == 0 ? 1 : status))"
