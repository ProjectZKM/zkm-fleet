#!/bin/bash
# Upgrade, roll back and inspect Fleet: fleet-ctl.sh [-n] COMMAND
#
#   status                        control services, health, nodes and apps (and the node, if configured)
#   scheduler upgrade [TAG|BIN_DIR] install from GitHub Release tag (default latest), or local binaries
#   scheduler rollback            restore the binaries the last upgrade replaced
#   node upgrade [TAG]              install agent, bridge and proof app from GitHub Release
#   node upgrade FLEET_NODE BRIDGE_DIR [FLEET_PROOF_APP]   legacy local-file upgrade
#   node rollback                 restore what the last node upgrade replaced
#   app cleanup SERVICE-0         remove the placement group and container a failed app start left
#
# -n prints what would change and changes nothing. Settings come from the environment or
# ${FLEET_CTL_ENV:-~/.config/zkm-fleet/ctl.env}: FLEET_PREFIX (install dir), FLEET_NODE_SSH
# (ssh host of the node; node commands then run there) and FLEET_NODE_PREFIX (its install
# dir, default ~/app/fleet). Units follow docs/maintenance.md; FLEET_*_UNIT renames them, and
# FLEET_PROOF_APP_UNITS names the node's app endpoint units a fleet-proof-app upgrade restarts.
# Every upgrade copies what it replaces to PREFIX/backup first.
set -euo pipefail
SELF=$(readlink -f "$0")
ENV_FILE=${FLEET_CTL_ENV:-$HOME/.config/zkm-fleet/ctl.env}
[ ! -f "$ENV_FILE" ] || . "$ENV_FILE"
PREFIX=${FLEET_PREFIX:-$(dirname "$(dirname "$SELF")")}
NODE_SSH=${FLEET_NODE_SSH:-}; NODE_PREFIX=${FLEET_NODE_PREFIX:-\$HOME/app/fleet}
SCHEDULER_UNIT=${FLEET_SCHEDULER_UNIT:-zkm-scheduler.service}; MCP_UNIT=${FLEET_MCP_UNIT:-zkm-fleet-mcp.service}
NODE_UNIT=${FLEET_NODE_UNIT:-zkm-fleet-node.service}
PROOF_APP_UNITS=${FLEET_PROOF_APP_UNITS:-}
[ -n "${FLEET_TOKEN_FILE:-}" ] || [ ! -f "$PREFIX/etc/admin.token" ] || export FLEET_TOKEN_FILE=$PREFIX/etc/admin.token
DRY=0; [ "${1:-}" != -n ] || { DRY=1; shift; }
die() { echo "fleet-ctl: $*" >&2; exit 1; }
run() { if [ "$DRY" = 1 ]; then echo "+ $*"; else "$@"; fi; }
stamp() { date -u +%Y%m%dT%H%M%SZ; }
RELEASE_REPO=${FLEET_RELEASE_REPO:-ProjectZKM/zkm-fleet}
RELEASE_VERSION=${FLEET_RELEASE_VERSION:-latest}

resolve_release_tag() {
  local requested=${1:-$RELEASE_VERSION}
  if [ "$requested" = latest ]; then
    python3 - "$RELEASE_REPO" <<'PY'
import json, sys, urllib.request
repo = sys.argv[1]
with urllib.request.urlopen(f"https://api.github.com/repos/{repo}/releases/latest", timeout=30) as response:
    print(json.load(response)["tag_name"])
PY
  else
    case "$requested" in
      v*) printf '%s\n' "$requested" ;;
      *) printf 'v%s\n' "$requested" ;;
    esac
  fi
}

prepare_release() {
  local requested=${1:-$RELEASE_VERSION} stage=${2:-}
  RELEASE_TAG=$(resolve_release_tag "$requested") || die "could not resolve release $requested"
  RELEASE_TAG=${RELEASE_TAG//$'\r'/}
  RELEASE_TAG=${RELEASE_TAG//$'\n'/}
  RELEASE_VERSION_NUMBER=${RELEASE_TAG#v}
  if [ -z "$stage" ]; then stage=$(mktemp -d); fi
  RELEASE_PACKAGE="$stage/zkm-fleet-${RELEASE_VERSION_NUMBER}-linux-x86_64.tar.gz"
  RELEASE_CHECKSUM="$RELEASE_PACKAGE.sha256"
  local base="https://github.com/$RELEASE_REPO/releases/download/$RELEASE_TAG"
  if [ "$DRY" = 1 ]; then
    echo "+ curl --fail --location --retry 3 --output $RELEASE_PACKAGE $base/zkm-fleet-${RELEASE_VERSION_NUMBER}-linux-x86_64.tar.gz"
    echo "+ curl --fail --location --retry 3 --output $RELEASE_CHECKSUM $base/zkm-fleet-${RELEASE_VERSION_NUMBER}-linux-x86_64.tar.gz.sha256"
    RELEASE_ROOT=$stage
    return
  fi
  curl -fsSL --retry 3 -o "$RELEASE_PACKAGE" "$base/zkm-fleet-${RELEASE_VERSION_NUMBER}-linux-x86_64.tar.gz" || die "failed to download $RELEASE_TAG package"
  curl -fsSL --retry 3 -o "$RELEASE_CHECKSUM" "$base/zkm-fleet-${RELEASE_VERSION_NUMBER}-linux-x86_64.tar.gz.sha256" || die "failed to download package checksum"
  sed 's#  dist/#  #g' "$RELEASE_CHECKSUM" > "$RELEASE_CHECKSUM.normalized"
  (cd "$stage" && sha256sum -c "$(basename "$RELEASE_CHECKSUM.normalized")") || die "package checksum mismatch"
  mkdir -p "$stage/root"
  tar -xzf "$RELEASE_PACKAGE" -C "$stage/root" || die "invalid release package"
  RELEASE_ROOT=$(find "$stage/root" -mindepth 1 -maxdepth 1 -type d | head -1)
  [ -n "$RELEASE_ROOT" ] || die "release package has no component directory"
  [ -f "$RELEASE_ROOT/VERSION" ] || die "release package has no VERSION file"
}
latest_backup() { # the newest backup this script made: KIND-YYYYmmddTHHMMSSZ
  find "$PREFIX/backup" -maxdepth 1 -type d -name "$1-[0-9]*T[0-9]*Z" 2>/dev/null | sort | tail -1
}
remote() { [ -n "$NODE_SSH" ] && [ "${FLEET_CTL_ON_NODE:-0}" != 1 ]; }
on_node() { # this script, run on the node over ssh
  local flag=; [ "$DRY" != 1 ] || flag=-n
  ssh "$NODE_SSH" "FLEET_CTL_ON_NODE=1 FLEET_PREFIX=$NODE_PREFIX bash -s -- $flag $(printf '%q ' "$@")" < "$SELF"
}

scheduler_upgrade() {
  local source=${1:-$RELEASE_VERSION} bin b; b=$PREFIX/backup/bin-$(stamp)
  local package_image=""
  if [ -d "$source" ]; then
    bin=$source
  else
    local stage; stage=$(mktemp -d)
    prepare_release "$source" "$stage"
    bin=$RELEASE_ROOT/bin
    package_image="$stage/zkm-fleet-scheduler-${RELEASE_VERSION_NUMBER}-linux-x86_64.tar.gz"
    local image_base="https://github.com/$RELEASE_REPO/releases/download/$RELEASE_TAG"
    if [ "$DRY" = 1 ]; then
      echo "+ curl --fail --location --retry 3 --output $package_image $image_base/zkm-fleet-scheduler-${RELEASE_VERSION_NUMBER}-linux-x86_64.tar.gz"
    else
      curl -fsSL --retry 3 -o "$package_image" "$image_base/zkm-fleet-scheduler-${RELEASE_VERSION_NUMBER}-linux-x86_64.tar.gz" || die "release has no scheduler image package"
      local image_checksum="$package_image.sha256"
      curl -fsSL --retry 3 -o "$image_checksum" "$image_base/zkm-fleet-scheduler-${RELEASE_VERSION_NUMBER}-linux-x86_64.tar.gz.sha256" || die "failed to download scheduler image checksum"
      sed 's#  dist/#  #g' "$image_checksum" > "$image_checksum.normalized"
      (cd "$(dirname "$package_image")" && sha256sum -c "$(basename "$image_checksum.normalized")") || die "scheduler image checksum mismatch"
    fi
  fi
  if [ "$DRY" != 1 ]; then
    for f in fleet-scheduler fleetctl fleet-mcp; do [ -x "$bin/$f" ] || die "$bin/$f is not built or packaged"; done
  fi
  run mkdir -p "$b"
  run cp -a "$PREFIX"/bin/{fleet-scheduler,fleetctl,fleet-mcp} "$b/"
  if [ -n "$package_image" ] && [ "$DRY" != 1 ]; then
    run docker save zkm-fleet-scheduler:local -o "$b/scheduler-image.tar.gz"
  fi
  run sudo systemctl stop "$MCP_UNIT" "$SCHEDULER_UNIT"
  if [ -n "$package_image" ] && [ "$DRY" != 1 ]; then
    run docker load -i "$package_image"
    run docker compose --env-file "$PREFIX/compose.env" -p zkm-fleet -f "$PREFIX/deploy/docker-compose.yml" up -d --no-deps --force-recreate scheduler mcp
  else
    run install -m 755 "$bin"/fleet-scheduler "$bin"/fleetctl "$bin"/fleet-mcp "$PREFIX/bin/"
  fi
  run sudo systemctl start "$SCHEDULER_UNIT" "$MCP_UNIT"
  [ "$DRY" = 1 ] || for _ in $(seq 30); do curl -fs -o /dev/null http://127.0.0.1:9191/healthz && break; sleep 2; done
  [ "$DRY" = 1 ] || curl -fs -o /dev/null http://127.0.0.1:9191/healthz || die "scheduler is not healthy; run: fleet-ctl.sh scheduler rollback"
  echo "scheduler upgraded to ${RELEASE_TAG:-local binaries}; previous binaries in $b"
}
scheduler_rollback() {
  local b; b=$(latest_backup bin); [ -n "$b" ] || die "no backup in $PREFIX/backup"
  run sudo systemctl stop "$MCP_UNIT" "$SCHEDULER_UNIT"
  if [ -f "$b/scheduler-image.tar.gz" ] && [ "$DRY" != 1 ]; then
    run docker load -i "$b/scheduler-image.tar.gz"
  fi
  run install -m 755 "$b"/{fleet-scheduler,fleetctl,fleet-mcp} "$PREFIX/bin/"
  run sudo systemctl start "$SCHEDULER_UNIT" "$MCP_UNIT"
  echo "restored $b"
}

node_upgrade() { # node_upgrade FLEET_NODE BRIDGE_DIR [FLEET_PROOF_APP], or node upgrade RELEASE_TAG
  local bin=${1:-} bridge=${2:-} proof_app=${3:-}
  if [ -n "$bin" ] && [ ! -f "$bin" ]; then
    local stage; stage=$(mktemp -d)
    prepare_release "$bin" "$stage"
    bin=$RELEASE_ROOT/bin/fleet-node
    bridge=$RELEASE_ROOT/bridge
    proof_app=$RELEASE_ROOT/bin/fleet-proof-app
  fi
  # A dry run never ships or validates downloaded files.
  if [ "$DRY" != 1 ]; then
    [ -f "$bin" ] && [ -d "$bridge/fleet_bridge" ] || die "usage: node upgrade FLEET_NODE_BINARY DIR_CONTAINING_fleet_bridge [FLEET_PROOF_APP_BINARY] or node upgrade RELEASE_TAG"
    [ -z "$proof_app" ] || [ -f "$proof_app" ] || die "no fleet-proof-app binary $proof_app"
  fi
  if remote; then # ship them, then upgrade on the node
    local stage; stage=/tmp/fleet-ctl-$(stamp)
    run ssh "$NODE_SSH" "mkdir -p $stage/bridge"
    run scp -q "$bin" "$NODE_SSH:$stage/fleet-node"
    run scp -q -r "$bridge/fleet_bridge" "$NODE_SSH:$stage/bridge/"
    if [ -n "$proof_app" ]; then
      run scp -q "$proof_app" "$NODE_SSH:$stage/fleet-proof-app"
      on_node node upgrade "$stage/fleet-node" "$stage/bridge" "$stage/fleet-proof-app"
    else
      on_node node upgrade "$stage/fleet-node" "$stage/bridge"
    fi
    run ssh "$NODE_SSH" "rm -rf $stage"
    return
  fi
  local b; b=$PREFIX/backup/node-$(stamp)
  # Check the new bridge imports in the node's own venv before the agent is touched.
  [ "$DRY" = 1 ] || (cd "$bridge" && "$PREFIX/venv/bin/python" -c "import fleet_bridge.node") || die "the new bridge does not import; nothing changed"
  run mkdir -p "$b"; run cp -a "$PREFIX/bin/fleet-node" "$PREFIX/bridge" "$b/"
  [ -z "$proof_app" ] || [ ! -f "$PREFIX/bin/fleet-proof-app" ] || run cp -a "$PREFIX/bin/fleet-proof-app" "$b/"
  run install -m 755 "$bin" "$PREFIX/bin/fleet-node.new"
  run sudo systemctl stop "$NODE_UNIT"
  run mv "$PREFIX/bin/fleet-node.new" "$PREFIX/bin/fleet-node"
  run cp -a "$bridge/fleet_bridge/." "$PREFIX/bridge/fleet_bridge/"
  run sudo systemctl start "$NODE_UNIT"
  [ "$DRY" = 1 ] || node_connected || die "node did not reconnect; run: fleet-ctl.sh node rollback"
  if [ -n "$proof_app" ]; then
    run install -m 755 "$proof_app" "$PREFIX/bin/fleet-proof-app.new"
    run mv "$PREFIX/bin/fleet-proof-app.new" "$PREFIX/bin/fleet-proof-app"
    local unit; for unit in $PROOF_APP_UNITS; do run sudo systemctl restart "$unit"; done
  fi
  echo "node upgraded; previous files in $b"
}
node_connected() { # the agent reconnecting, not just the unit running
  for _ in $(seq 45); do
    journalctl -u "$NODE_UNIT" --since -30s --no-pager | grep -q "node connected to control plane" && return 0; sleep 2
  done; return 1
}
node_rollback() {
  local b; b=$(latest_backup node); [ -n "$b" ] || die "no backup in $PREFIX/backup"
  run sudo systemctl stop "$NODE_UNIT"
  run install -m 755 "$b/fleet-node" "$PREFIX/bin/fleet-node"
  run cp -a "$b/bridge/." "$PREFIX/bridge/"
  run sudo systemctl start "$NODE_UNIT"
  if [ -f "$b/fleet-proof-app" ]; then
    run install -m 755 "$b/fleet-proof-app" "$PREFIX/bin/fleet-proof-app"
    local unit; for unit in $PROOF_APP_UNITS; do run sudo systemctl restart "$unit"; done
  fi
  echo "restored $b"
}

app_cleanup() {
  "$PREFIX/venv/bin/python" - "${1:?usage: app cleanup SERVICE-0, e.g. ethproofs-v6-gpu-0}" "$DRY" <<'PY'
import subprocess, sys, ray
from ray._raylet import PlacementGroupID
from ray.util import placement_group_table
from ray.util.placement_group import PlacementGroup, remove_placement_group
name, dry = sys.argv[1], sys.argv[2] == "1"
verb = "would remove" if dry else "removing"
ray.init(address="127.0.0.1:6379", namespace="zkm-fleet", log_to_driver=False, logging_level="ERROR")
try:
    ray.get_actor(name, namespace="zkm-fleet")
    sys.exit(f"{name} is running; disable the app instead")
except ValueError:
    pass
found = False
for pg_id, v in placement_group_table().items():
    if v.get("name", "").startswith(f"pg-{name}-") and v.get("state") != "REMOVED":
        print(verb, v["name"]); found = True
        if not dry:
            remove_placement_group(PlacementGroup(PlacementGroupID.from_hex(pg_id)))
container = f"fleet-gpu-{name}"
state = subprocess.run(["docker", "inspect", "-f", "{{.State.Status}}", container], capture_output=True, text=True).stdout.strip()
if state == "exited":
    print(verb, "container", container); found = True
    if not dry:
        subprocess.run(["docker", "rm", container], check=True)
if not found:
    print(f"nothing to clean for {name}")
PY
}

status() {
  if [ "${FLEET_CTL_ON_NODE:-0}" = 1 ]; then
    echo "== node $(hostname): agent $(systemctl is-active "$NODE_UNIT" || true), ray $(pgrep -x raylet >/dev/null && echo running || echo NOT RUNNING)"
    docker ps --format '{{.Names}} {{.Status}}' | grep -E '^fleet-(gpu|app|job)-' || true
    return
  fi
  echo "== control host: scheduler $(systemctl is-active "$SCHEDULER_UNIT" || true), mcp $(systemctl is-active "$MCP_UNIT" || true), healthz $(curl -fs http://127.0.0.1:9191/healthz || echo FAILING)"
  "$PREFIX/bin/fleetctl" node || true
  "$PREFIX/bin/fleetctl" app || true
  if [ -n "$NODE_SSH" ]; then on_node status; fi
}

case "${1:-}/${2:-}" in
  status/) status;;
  scheduler/upgrade) scheduler_upgrade "${3:-}";;
  scheduler/rollback) scheduler_rollback;;
  node/upgrade) node_upgrade "${3:-}" "${4:-}" "${5:-}";;
  node/rollback) if remote; then on_node node rollback; else node_rollback; fi;;
  app/cleanup) if remote; then on_node app cleanup "${3:-}"; else app_cleanup "${3:-}"; fi;;
  *) sed -n '2,/^set -euo/p' "$SELF" | sed '$d;s/^# \{0,1\}//'; exit 2;;
esac
