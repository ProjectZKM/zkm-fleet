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
done
for component in scheduler ctl mcp node proof-app; do
    binary="fleet-$component"
    [[ $component != ctl ]] || binary=fleetctl
    image="$PREFIX-$component:$TAG"
    docker build --build-arg "BINARY=$binary" \
        --label "org.opencontainers.image.source=https://github.com/${GITHUB_REPOSITORY:-ProjectZKM/zkm-fleet}" \
        --label "org.opencontainers.image.revision=$(git -C "$SRC" rev-parse HEAD)" \
        -t "$image" "$STAGE"
    docker run --rm "$image" --help >/dev/null
    if [[ ${PUBLISH:-false} == true ]]; then docker push "$image"; fi
done
