#!/usr/bin/env bash
# Package prebuilt binaries; PUBLISH=true additionally uploads the images.
set -euo pipefail
[[ $# == 3 ]] || { echo 'usage: build-component-images.sh BIN_DIR IMAGE_PREFIX TAG' >&2; exit 2; }
BIN_DIR=$(realpath "$1")
PREFIX=$2
TAG=$3
SRC=$(cd "$(dirname "$0")" && pwd)
STAGE=$(mktemp -d)
trap 'rm -rf "$STAGE"' EXIT
cp "$SRC/images/components/Dockerfile" "$STAGE/"
for binary in fleet-scheduler fleetctl fleet-mcp fleet-node fleet-proof-app; do
    install -m 755 "$BIN_DIR/$binary" "$STAGE/"
    strip --strip-unneeded "$STAGE/$binary"
done
for component in scheduler ctl mcp node proof-app; do
    binary="fleet-$component"
    [[ $component != ctl ]] || binary=fleetctl
    image="$PREFIX-$component:$TAG"
    docker build --build-arg "BINARY=$binary" \
        --label "org.opencontainers.image.source=https://github.com/${GITHUB_REPOSITORY:-ProjectZKM/zkm-fleet}" \
        --label "org.opencontainers.image.revision=$(git -C "$SRC" rev-parse HEAD)" \
        -t "$image" "$STAGE"
    if [[ $component == mcp ]]; then
        # MCP is an HTTP service without a CLI parser. Initialize it locally;
        # tool execution requires credentials and is deliberately not exercised.
        docker run --rm -i --entrypoint sh "$image" -se <<'SH'
fleet-mcp &
server=$!
trap 'kill "$server"; wait "$server" || true' EXIT
curl -fsS --retry 5 --retry-connrefused --retry-delay 1 --max-time 2 \
    -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
    --data '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"image-check","version":"1"}}}' \
    http://127.0.0.1:8906/mcp > /tmp/initialize
grep -q '"serverInfo"' /tmp/initialize
SH
    else
        docker run --rm "$image" --help >/dev/null
    fi
    if [[ ${PUBLISH:-false} == true ]]; then docker push "$image"; fi
done
