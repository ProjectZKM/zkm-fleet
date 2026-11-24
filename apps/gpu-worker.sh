#!/bin/bash
# Runs a Ziren GPU server and an app's own worker in one container: the entrypoint of a
# container app whose image is the GPU image and whose worker package is mounted in.
#
#   gpu-worker WORKER [ARG...]
#
# The container's environment is the GPU server's. The worker gets only PATH, HOME, every
# FLEET_* value, FLEET_GPU_ENDPOINT and RAYON_NUM_THREADS (from WORKER_THREADS); it reads
# its private settings itself. It starts once the server answers ready; from then on any
# GET answers 200 on HEALTH_PORT (default 8080). The container ends when either process
# does. On SIGTERM the worker is told to stop, and the server runs until it has exited.
set -euo pipefail

[ $# -ge 1 ] || { echo "usage: gpu-worker WORKER [ARG...]" >&2; exit 2; }
export ZIREN_SERVER_PORT=${ZIREN_SERVER_PORT:-3000}
endpoint="http://127.0.0.1:$ZIREN_SERVER_PORT/twirp/"
worker_env=("PATH=$PATH" "HOME=${HOME:-/}" "FLEET_GPU_ENDPOINT=$endpoint")
for key in "${!FLEET_@}"; do worker_env+=("$key=${!key}"); done
[ -z "${WORKER_THREADS:-}" ] || worker_env+=("RAYON_NUM_THREADS=$WORKER_THREADS")

server='' worker=''
stop() { kill -TERM "${worker:-$server}" 2>/dev/null || true; }
trap stop TERM INT

(cd / && exec zkm-gpu-server) &
server=$!
ready() {  # the GPU image has perl, and no curl
  perl -MIO::Socket::INET -e '
    my $server = IO::Socket::INET->new(PeerAddr => "127.0.0.1:$ARGV[0]", Timeout => 3) or exit 1;
    print $server "POST /twirp/api.ProverService/Ready HTTP/1.0\r\nHost: 127.0.0.1\r\n",
      "Content-Type: application/protobuf\r\nContent-Length: 0\r\n\r\n";
    local $/;
    exit(<$server> =~ m{^HTTP/1\.\d 200 .*\r\n\r\n\x08\x01\z}s ? 0 : 1)' "$ZIREN_SERVER_PORT"
}
until ready; do
  kill -0 "$server" 2>/dev/null || { echo "gpu-worker: the GPU server ended before it was ready" >&2; exit 1; }
  sleep 2
done

env -i "${worker_env[@]}" "$@" &
worker=$!
perl -MIO::Socket::INET -e '
  my $listener = IO::Socket::INET->new(LocalPort => $ARGV[0], Listen => 16, ReuseAddr => 1) or die "health port: $!\n";
  while (my $client = $listener->accept) {
    $client->timeout(5);
    sysread $client, my $request, 4096;
    print $client "HTTP/1.0 200 OK\r\nContent-Length: 3\r\n\r\nok\n";
    close $client;
  }' "${HEALTH_PORT:-8080}" &

# A signal interrupts the wait with neither ended: keep waiting.
ended='' code=0
while [ -z "${ended:-}" ]; do
  code=0
  wait -n -p ended "$server" "$worker" || code=$?
done
if [ "$ended" = "$server" ]; then
  echo "gpu-worker: the GPU server ended; stopping the worker" >&2
  kill -TERM "$worker" 2>/dev/null || true
  wait "$worker" || true
  exit 1
fi
kill -TERM "$server" 2>/dev/null || true
wait "$server" || true
exit "$code"
