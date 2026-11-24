#!/bin/bash
# Build an app's own worker image: its worker package beside the validated GPU server, for
# an integrated worker that runs on the shim. Does not start a prover or publish an image.
# Usage: build-worker-image.sh OUTPUT_IMAGE VALIDATED_GPU_IMAGE@sha256:DIGEST PACKAGE_DIRECTORY [PATH_IN_IMAGE]
set -euo pipefail
[ "$#" = 3 ] || [ "$#" = 4 ] || {
  echo "usage: $0 OUTPUT_IMAGE VALIDATED_GPU_IMAGE@sha256:DIGEST PACKAGE_DIRECTORY [PATH_IN_IMAGE]" >&2; exit 2; }
SRC=$(cd "$(dirname "$0")/.." && pwd)
# Supply the operator-validated GPU build matching the worker's SDK revision.
GPU_IMAGE=$2
[[ "$GPU_IMAGE" =~ ^[^[:space:]@]+@sha256:[0-9a-f]{64}$ ]] || { echo 'GPU image must be a validated registry digest' >&2; exit 2; }
[ -d "$3" ] || { echo "no worker package directory $3" >&2; exit 2; }
PACKAGE=$(realpath -- "$3")
# The package keeps its path, so the entry's worker_command and the paths in the worker's
# settings stay as they are. Give another path for a package that lies under one of the
# app's volumes: the mount would hide the image's copy.
WORKER_PATH=${4:-$PACKAGE}
[[ "$WORKER_PATH" =~ ^/[A-Za-z0-9._/-]+$ && "$WORKER_PATH" != *..* ]] || { echo 'the path in the image must be absolute and plain' >&2; exit 2; }
STAGE=$(mktemp -d)
trap 'rm -rf "$STAGE"' EXIT
# The build context holds only the package, the launcher, the Dockerfile and the build record.
mkdir "$STAGE/package"
cp -a -- "$PACKAGE/." "$STAGE/package/"
cp "$SRC/deploy/images/worker/"{Dockerfile,entrypoint.sh} "$STAGE/"
python3 - "$STAGE" "$GPU_IMAGE" "$WORKER_PATH" <<'PY'
import hashlib, json, pathlib, sys
root = pathlib.Path(sys.argv[1])
package = root / 'package'
files = {str(p.relative_to(package)): hashlib.file_digest(p.open('rb'), 'sha256').hexdigest()
         for p in sorted(package.rglob('*')) if p.is_file() and not p.is_symlink()}
root.joinpath('build.json').write_text(json.dumps({
    'gpu_image': sys.argv[2], 'worker_path': sys.argv[3], 'package_sha256': files}) + '\n')
PY
docker pull "$GPU_IMAGE"
docker build --build-arg "GPU_IMAGE=$GPU_IMAGE" --build-arg "WORKER_PATH=$WORKER_PATH" -t "$1" "$STAGE"
