#!/bin/bash
# Build a complete proof worker image. Does not start a prover or publish an image.
# Usage: build-proof-image.sh OUTPUT_IMAGE VALIDATED_GPU_IMAGE@sha256:DIGEST
set -euo pipefail
[ "$#" = 2 ] || { echo "usage: $0 OUTPUT_IMAGE VALIDATED_GPU_IMAGE@sha256:DIGEST" >&2; exit 2; }
SRC=$(cd "$(dirname "$0")/.." && pwd)
cd "$SRC"
# Supply the operator-validated GPU build matching the SDK revision below.
GPU_IMAGE=$2
[[ "$GPU_IMAGE" =~ ^[^[:space:]@]+@sha256:[0-9a-f]{64}$ ]] || { echo 'GPU image must be a validated registry digest' >&2; exit 2; }
SDK_REV=c9988a098d92bc55a85ca9307c470d1f97be3337
STAGE=$(mktemp -d)
trap 'rm -rf "$STAGE"' EXIT
# Keep the build context minimal: never send workspace credentials or artifacts.
export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/zkm-fleet/proof-image}
python3 - "$SRC/Cargo.lock" "$SDK_REV" <<'PY'
import sys, tomllib
with open(sys.argv[1], 'rb') as source:
    packages = tomllib.load(source)['package']
for name in ('zkm-sdk', 'zkm-prover', 'zkm-cuda'):
    matches = [p for p in packages if p['name'] == name]
    if len(matches) != 1 or not matches[0].get('source', '').endswith('#' + sys.argv[2]):
        raise SystemExit('SDK/CUDA revision differs from validated GPU image; validate and update the tuple together')
PY
cargo build --manifest-path "$SRC/Cargo.toml" --locked --release -p fleet-ziren-node
docker pull "$GPU_IMAGE"
cp "$CARGO_TARGET_DIR/release/fleet-ziren-node" "$STAGE/"
cp "$SRC/deploy/images/prover/"{Dockerfile,entrypoint.sh} "$STAGE/"
python3 - "$STAGE" "$SDK_REV" "$GPU_IMAGE" <<'PY'
import hashlib, json, pathlib, sys
root = pathlib.Path(sys.argv[1])
root.joinpath('build.json').write_text(json.dumps({
    'sdk_revision': sys.argv[2], 'gpu_image': sys.argv[3],
    'worker_sha256': hashlib.file_digest(root.joinpath('fleet-ziren-node').open('rb'), 'sha256').hexdigest()
}) + '\n')
PY
docker build --build-arg "GPU_IMAGE=$GPU_IMAGE" --label "org.projectzkm.ziren.revision=$SDK_REV" -t "$1" "$STAGE"
