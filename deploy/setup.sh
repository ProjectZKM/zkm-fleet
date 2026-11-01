#!/bin/bash
# Bootstrap Fleet from a published GitHub Release package.
#
# setup.sh --version v0.1.0 node --name gpu-1 \
#   --scheduler https://fleet.example:8905 --cluster-token FILE
# setup.sh --version v0.1.0 control --public-url https://fleet.example:8905
set -euo pipefail

REPO=${FLEET_RELEASE_REPO:-ProjectZKM/zkm-fleet}
VERSION=${FLEET_RELEASE_VERSION:-latest}
KEEP=0
ARGS=()

while [ $# -gt 0 ]; do
  case "$1" in
    --version) [ $# -ge 2 ] || { echo "setup.sh: --version needs a value" >&2; exit 2; }; VERSION=$2; shift 2;;
    --repo) [ $# -ge 2 ] || { echo "setup.sh: --repo needs a value" >&2; exit 2; }; REPO=$2; shift 2;;
    --keep-package) KEEP=1; shift;;
    *) ARGS+=("$1"); shift;;
  esac
done

ROLE=${ARGS[0]:-}
case "$ROLE" in control|node) ;; *) cat >&2 <<'EOF'
usage: setup.sh [--version vX.Y.Z|latest] [--repo OWNER/REPO] control|node [install.sh options]
EOF
  exit 2;; esac

if [ "$VERSION" = latest ]; then
  command -v python3 >/dev/null || { echo "setup.sh: python3 is required to resolve latest" >&2; exit 1; }
  TAG=$(python3 - "$REPO" <<'PY'
import json, sys, urllib.request
with urllib.request.urlopen(
    f"https://api.github.com/repos/{sys.argv[1]}/releases/latest",
    timeout=30,
) as response:
    print(json.load(response)["tag_name"])
PY
)
else
  case "$VERSION" in v*) TAG=$VERSION ;; *) TAG="v$VERSION" ;; esac
fi
TAG=$(printf '%s' "$TAG" | tr -d '\r\n')
[ -n "$TAG" ] || { echo "setup.sh: could not resolve release tag" >&2; exit 1; }
VERSION_NUMBER=${TAG#v}

command -v curl >/dev/null || { echo "setup.sh: curl is required" >&2; exit 1; }
command -v tar >/dev/null || { echo "setup.sh: tar is required" >&2; exit 1; }
command -v sha256sum >/dev/null || { echo "setup.sh: sha256sum is required" >&2; exit 1; }

STAGE=$(mktemp -d)
cleanup() { if [ "$KEEP" = 0 ]; then rm -rf "$STAGE"; fi; }
trap cleanup EXIT
PACKAGE="$STAGE/zkm-fleet-${VERSION_NUMBER}-linux-x86_64.tar.gz"
CHECKSUM="$PACKAGE.sha256"
BASE="https://github.com/$REPO/releases/download/$TAG"

curl -fsSL --retry 3 "$BASE/zkm-fleet-${VERSION_NUMBER}-linux-x86_64.tar.gz" -o "$PACKAGE"
curl -fsSL --retry 3 "$BASE/zkm-fleet-${VERSION_NUMBER}-linux-x86_64.tar.gz.sha256" -o "$CHECKSUM"

sed 's#  dist/#  #g' "$CHECKSUM" > "$STAGE/package.sha256"
(cd "$STAGE" && sha256sum -c package.sha256) || { echo "setup.sh: package checksum mismatch" >&2; exit 1; }

mkdir -p "$STAGE/root"
tar -xzf "$PACKAGE" -C "$STAGE/root"
PACKAGE_ROOT=$(find "$STAGE/root" -mindepth 1 -maxdepth 1 -type d | head -1)
[ -n "$PACKAGE_ROOT" ] || { echo "setup.sh: package is missing its component directory" >&2; exit 1; }
[ -x "$PACKAGE_ROOT/deploy/install.sh" ] || { echo "setup.sh: package is missing deploy/install.sh" >&2; exit 1; }

if [ "$ROLE" = control ]; then
  IMAGE="$STAGE/zkm-fleet-scheduler-${VERSION_NUMBER}-linux-x86_64.tar.gz"
  IMAGE_CHECKSUM="$IMAGE.sha256"
  curl -fsSL --retry 3 "$BASE/zkm-fleet-scheduler-${VERSION_NUMBER}-linux-x86_64.tar.gz" -o "$IMAGE"
  curl -fsSL --retry 3 "$BASE/zkm-fleet-scheduler-${VERSION_NUMBER}-linux-x86_64.tar.gz.sha256" -o "$IMAGE_CHECKSUM"
  sed 's#  dist/#  #g' "$IMAGE_CHECKSUM" > "$STAGE/image.sha256"
  (cd "$STAGE" && sha256sum -c image.sha256) || { echo "setup.sh: scheduler image checksum mismatch" >&2; exit 1; }
  command -v docker >/dev/null || { echo "setup.sh: docker is required for control installation" >&2; exit 1; }
  docker load -i "$IMAGE" >/dev/null
fi

FORWARD=("${ARGS[@]:1}" "--bin" "$PACKAGE_ROOT/bin")
echo "setup.sh: installing $TAG from GitHub Release package"
exec bash "$PACKAGE_ROOT/deploy/install.sh" "$ROLE" "${FORWARD[@]}"
