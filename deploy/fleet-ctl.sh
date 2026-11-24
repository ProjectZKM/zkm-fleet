#!/bin/bash
# Upgrade, roll back and inspect Fleet: fleet-ctl.sh [-n] COMMAND
#
#   status                        control services, health, nodes and apps (and the node, if configured)
#   openshell up                  check the OpenShell gateway's files and policy_gateway.openshell, pin its
#                                 image, prepare its volumes, restart the scheduler, start the gateway and
#                                 add the openshell profile to compose.env
#   scheduler upgrade [TAG]       pull the release's scheduler image from GHCR, recreate it with Docker
#                                 Compose, install its fleetctl and re-render gateway.yaml (default: latest
#                                 release). A rollback refuses after a database schema change unless --force
#   scheduler rollback            restore the image setting, fleetctl and compose files the last upgrade replaced
#   node upgrade [TAG]            install a release's fleet-node, bridge, fleet-proof-app and launch scripts
#   node upgrade FLEET_NODE BRIDGE_DIR [FLEET_PROOF_APP] [DEPLOY_DIR]   the same from local files
#   node rollback                 restore what the last node upgrade replaced
#   app cleanup APP-0             remove the placement group and container a failed app start left
#
# -n prints what would change and changes nothing. Settings come from the environment or
# ${FLEET_CTL_ENV:-~/.config/zkm-fleet/ctl.env}: FLEET_PREFIX (install dir), FLEET_NODE_SSH
# (ssh host of the node; node commands then run there), FLEET_NODE_PREFIX (its install
# dir, default ~/app/fleet) and FLEET_RELEASE_REPO (default ProjectZKM/zkm-fleet).
# FLEET_NODE_UNIT names the node's systemd unit; FLEET_CTL_HEALTH_WAIT how many seconds to wait for
# the scheduler to start listening (default 300). Every upgrade backs up what it replaces first.
# Node upgrades and rollbacks require systemd; see docs/maintenance.md for cron nodes.
set -euo pipefail
SELF=$(readlink -f "$0")
ENV_FILE=${FLEET_CTL_ENV:-$HOME/.config/zkm-fleet/ctl.env}
[ ! -f "$ENV_FILE" ] || . "$ENV_FILE"
PREFIX=${FLEET_PREFIX:-$(dirname "$(dirname "$SELF")")}
NODE_SSH=${FLEET_NODE_SSH:-}; NODE_PREFIX=${FLEET_NODE_PREFIX:-\$HOME/app/fleet}
NODE_UNIT=${FLEET_NODE_UNIT:-zkm-fleet-node.service}
NODE_SCRIPTS=(node-start.sh ray-start.sh fleet-watchdog.sh shim-start.sh ray-watch.sh egress-start.sh install-node-services.sh)
[ -n "${FLEET_TOKEN_FILE:-}" ] || [ ! -f "$PREFIX/etc/admin.token" ] || export FLEET_TOKEN_FILE=$PREFIX/etc/admin.token
DRY=0; [ "${1:-}" != -n ] || { DRY=1; shift; }
die() { echo "fleet-ctl: $*" >&2; exit 1; }
run() { if [ "$DRY" = 1 ]; then echo "+ $*"; else "$@"; fi; }
# Each upgrade downloads and unpacks its release into a temporary directory; it is removed
# on exit (success or failure), so a fleet host's small disk never leaks ~400MB per upgrade.
STAGE=""
cleanup_stage() { [ -z "$STAGE" ] || rm -rf "$STAGE"; return 0; }
trap cleanup_stage EXIT
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
  STAGE=$stage
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
on_node() { # this script, run on the node over ssh, with the node settings given here
  local flag= vars="FLEET_CTL_ON_NODE=1 FLEET_PREFIX=$NODE_PREFIX" v; [ "$DRY" != 1 ] || flag=-n
  for v in FLEET_ETC FLEET_ENV FLEET_NODE_UNIT; do
    [ -z "${!v:-}" ] || vars+=" $v=$(printf '%q' "${!v}")"
  done
  ssh "$NODE_SSH" "$vars bash -s -- $flag $(printf '%q ' "$@")" < "$SELF"
}

# The release's compose file, then this site's changes to it, if any: upgrades replace only the release's.
COMPOSE=(docker compose --env-file "$PREFIX/compose.env" -p zkm-fleet -f "$PREFIX/deploy/docker-compose.yml")
[ ! -f "$PREFIX/deploy/docker-compose.override.yml" ] || COMPOSE+=(-f "$PREFIX/deploy/docker-compose.override.yml")
compose() { run "${COMPOSE[@]}" "$@"; }
# Naming a profiled service starts it, so recreate MCP only where install.sh enabled it.
control_services() { echo scheduler; grep -q '^COMPOSE_PROFILES=.*mcp' "$PREFIX/compose.env" 2>/dev/null && echo mcp || true; }
scheduler_container() { # STATUS RESTARTS of the scheduler's container, or nothing
  local id; id=$("${COMPOSE[@]}" ps -aq scheduler 2>/dev/null) || return 0
  [ -z "$id" ] || docker inspect -f '{{.State.Status}} {{.RestartCount}}' "$id" 2>/dev/null || true
}
# A scheduler reads every job and command record it keeps before it listens, which takes a
# minute or more on a busy control host. Wait up to FLEET_CTL_HEALTH_WAIT seconds for it, but
# give up at once if its container exits or restarts: that is a crash, not a slow start.
scheduler_healthy() {
  [ "$DRY" = 1 ] && return 0
  local wait=${FLEET_CTL_HEALTH_WAIT:-300} start=$SECONDS first now said=
  first=$(scheduler_container)
  until curl -fs -o /dev/null http://127.0.0.1:9191/healthz; do
    now=$(scheduler_container)
    if [ -n "$first" ] && { [ "${now%% *}" != running ] || [ "${now#* }" != "${first#* }" ]; }; then
      echo "fleet-ctl: the scheduler container stopped (${now:-gone}); see docker logs zkm-fleet-scheduler-1" >&2
      return 1
    fi
    [ $((SECONDS - start)) -lt "$wait" ] || return 1
    [ -n "$said" ] || { echo "waiting up to ${wait}s for the scheduler to load its records and listen"; said=1; }
    sleep 2
  done
}
compose_env_get() { # KEY [FILE]: the value compose.env (or FILE) gives KEY, or nothing
  local file=${2:-$PREFIX/compose.env}
  [ -f "$file" ] || return 0
  sed -n "s/^\(export \)\{0,1\}$1=//p" "$file" | tail -1 | sed "s/^[\"']\(.*\)[\"']\$/\1/"
}
set_compose_env() { # KEY VALUE: replace KEY's line in compose.env, keeping every other line
  [ "$DRY" = 1 ] && { echo "+ set $1=$2 in $PREFIX/compose.env"; return; }
  { [ ! -f "$PREFIX/compose.env" ] || grep -v "^\(export \)\{0,1\}$1=" "$PREFIX/compose.env" || true; echo "$1=$2"; } > "$PREFIX/compose.env.new"
  [ ! -f "$PREFIX/compose.env" ] || chmod --reference="$PREFIX/compose.env" "$PREFIX/compose.env.new"
  mv "$PREFIX/compose.env.new" "$PREFIX/compose.env"
}
set_scheduler_image() { set_compose_env FLEET_SCHEDULER_IMAGE "$1"; } # compose.env names the image both services run
control_etc() { local etc; etc=$(compose_env_get FLEET_ETC); echo "${etc:-/etc/zkm-fleet}"; } # as Compose mounts it
# The schema the database is at, as the scheduler records it (fleet_schema in
# crates/fleet-scheduler/src/store.rs), or nothing if Postgres cannot say. A site whose
# database_url points elsewhere is asked with psql from the Compose Postgres image; the URL
# carries the password, so it travels in the environment, never in argv.
database_schema() {
  local v url image query='SELECT max(version) FROM fleet_schema'
  if ! v=$("${COMPOSE[@]}" exec -T postgres psql -U fleet -d fleet -tAc "$query" 2>/dev/null); then
    url=$(cat "$(control_etc)/database_url" 2>/dev/null) || return 0
    case "$url" in *@postgres:*|*@postgres/*|"") return 0;; esac  # the bundled one, and it did not answer
    image=$("${COMPOSE[@]}" config --format json postgres 2>/dev/null \
      | python3 -c 'import json, sys; print(json.load(sys.stdin)["services"]["postgres"]["image"])' 2>/dev/null) || return 0
    v=$(DATABASE_URL=$url docker run --rm --network host -e DATABASE_URL -e PGCONNECT_TIMEOUT=10 \
      --entrypoint sh "$image" -c 'exec psql "$DATABASE_URL" -tAc "$0"' "$query" 2>/dev/null) || return 0
  fi
  v=${v//[[:space:]]/}
  if [[ $v =~ ^[0-9]+$ ]]; then echo "$v"; fi
}
record_schema() { # BACKUP: the schema a rollback to BACKUP must still find
  if [ "$DRY" = 1 ]; then echo "+ record the database schema version in $1/schema_version"; return; fi
  local v; v=$(database_schema)
  if [ -n "$v" ]; then echo "$v" > "$1/schema_version"
  else echo "fleet-ctl: warning: cannot read the database schema version; a rollback to $1 will need --force" >&2; fi
}
# A new scheduler migrates the database when it starts, and an older one refuses a schema it does not know.
rollback_schema_check() { # BACKUP [--force]
  local b=$1 force=${2:-} was now why
  was=$(cat "$b/schema_version" 2>/dev/null || true)
  if [ "$DRY" = 1 ]; then echo "+ check that the database schema is still ${was:-(not recorded)}"; return; fi
  now=$(database_schema)
  if [ -n "$was" ] && [ "$now" = "$was" ]; then return 0; fi
  if [ -z "$was" ]; then why="$b records no database schema version"
  elif [ -z "$now" ]; then why="cannot read the database schema version (it was $was before the upgrade)"
  else why="the database is at schema $now, but was at schema $was before the upgrade"; fi
  if [ "$force" = --force ]; then echo "fleet-ctl: warning: $why; rolling back anyway (--force)" >&2; return 0; fi
  die "$why, and the previous scheduler may not start on a migrated database. Restore the Postgres and data_dir backups taken before the upgrade instead (see docs/maintenance.md), or pass --force to roll back anyway; nothing changed"
}
# The live Envoy config is rendered from the template the way install.sh renders it. A
# site that manages its own (no gateway.yaml), or edited the rendered one, keeps it.
gateway_config_refresh() { # BACKUP: holds the previous gateway.yaml.in
  local live ip; live=$(control_etc)/gateway.yaml
  ip=$(compose_env_get FLEET_SCHEDULER_IP); ip=${ip:-172.28.250.10}
  if [ ! -f "$live" ]; then
    echo "note: no $live, so the gateway's config is site-managed and was left alone; compare it with $PREFIX/deploy/gateway.yaml.in"
    return 0
  fi
  if [ "$DRY" = 1 ]; then
    echo "+ sed s/@SCHEDULER_IP@/$ip/g $PREFIX/deploy/gateway.yaml.in > $live   # unless $live was edited"
    echo "+ ${COMPOSE[*]} up -d --no-deps --force-recreate gateway   # if $live changed"
    return 0
  fi
  sed "s/@SCHEDULER_IP@/$ip/g" "$PREFIX/deploy/gateway.yaml.in" > "$live.new"
  if cmp -s "$live.new" "$live"; then rm -f "$live.new"; echo "gateway config unchanged"; return 0; fi
  # Replace only what the previous template rendered; an edited file is the site's.
  if ! sed "s/@SCHEDULER_IP@/$ip/g" "$1/gateway.yaml.in" 2>/dev/null | cmp -s - "$live"; then
    chmod --reference="$live" "$live.new"
    mv "$live.new" "$live.release"
    echo "fleet-ctl: $live differs from what the previous release rendered, so it was left alone and the gateway was not restarted; merge the new release's render, $live.release, into it and recreate the gateway" >&2
    return 0
  fi
  chmod --reference="$live" "$live.new"
  mv "$live.new" "$live"
  compose up -d --no-deps --force-recreate gateway || die "the gateway did not restart with the new $live; run: fleet-ctl.sh scheduler rollback"
}
# The scheduler and MCP run from the image in Docker Compose; only fleetctl is a host binary.
scheduler_upgrade() {
  local source=${1:-$RELEASE_VERSION} b; b=$PREFIX/backup/control-$(stamp)
  local stage; stage=$(mktemp -d)
  prepare_release "$source" "$stage"
  local image="ghcr.io/${RELEASE_REPO,,}-scheduler:${RELEASE_VERSION_NUMBER}"
  [ "$DRY" = 1 ] || [ -x "$RELEASE_ROOT/bin/fleetctl" ] || die "release package has no fleetctl"
  # Pull first so a missing image changes nothing.
  run docker pull "$image"
  run mkdir -p "$b"
  record_schema "$b"
  run cp -a "$PREFIX/bin/fleetctl" "$PREFIX/deploy/docker-compose.yml" "$PREFIX/deploy/gateway.yaml.in" "$PREFIX/compose.env" "$b/"
  local live; live=$(control_etc)/gateway.yaml
  [ ! -f "$live" ] || run cp -a "$live" "$b/gateway.yaml"
  [ ! -d "$PREFIX/deploy/trustee" ] || run cp -a "$PREFIX/deploy/trustee" "$b/"
  run install -m 755 "$RELEASE_ROOT/bin/fleetctl" "$PREFIX/bin/fleetctl"
  run cp "$RELEASE_ROOT/deploy/docker-compose.yml" "$RELEASE_ROOT/deploy/gateway.yaml.in" "$PREFIX/deploy/"
  if [ "$DRY" = 1 ] || [ -d "$RELEASE_ROOT/deploy/trustee" ]; then
    run mkdir -p "$PREFIX/deploy/trustee"
    run cp -a "$RELEASE_ROOT/deploy/trustee/." "$PREFIX/deploy/trustee/"
  fi
  set_scheduler_image "$image"
  # The scheduler finishes in-flight calls before it exits, so recreating can take ~90s.
  compose up -d --no-deps --force-recreate $(control_services)
  scheduler_healthy || die "scheduler is not healthy; run: fleet-ctl.sh scheduler rollback"
  # Envoy reads its config only at startup, and the new template may route new services.
  gateway_config_refresh "$b"
  echo "control plane upgraded to $RELEASE_TAG; previous fleetctl and compose files in $b"
}
scheduler_rollback() { # [--force]
  local force=${1:-}
  [ -z "$force" ] || [ "$force" = --force ] || die "usage: scheduler rollback [--force]"
  local b; b=$(latest_backup control); [ -n "$b" ] || die "no backup in $PREFIX/backup"
  rollback_schema_check "$b" "$force"
  run install -m 755 "$b/fleetctl" "$PREFIX/bin/fleetctl"
  run cp "$b/docker-compose.yml" "$b/gateway.yaml.in" "$PREFIX/deploy/"
  # The upgrade changed only the image; settings added since (OpenShell's pin and profile) stay.
  local image; image=$(compose_env_get FLEET_SCHEDULER_IMAGE "$b/compose.env")
  if [ -n "$image" ]; then set_scheduler_image "$image"; else run cp "$b/compose.env" "$PREFIX/compose.env"; fi
  if [ -d "$b/trustee" ]; then
    run mkdir -p "$PREFIX/deploy/trustee"
    run cp -a "$b/trustee/." "$PREFIX/deploy/trustee/"
  fi
  local services live; services=$(control_services); live=$(control_etc)/gateway.yaml
  if [ -f "$b/gateway.yaml" ] && ! cmp -s "$b/gateway.yaml" "$live"; then
    run cp -a "$b/gateway.yaml" "$live"
    services="$services gateway"
  fi
  compose up -d --no-deps --force-recreate $services
  scheduler_healthy || die "scheduler is not healthy after rollback"
  echo "restored $b"
}
# The access-policy gateway needs the scheduler's compute socket, which only
# policy_gateway.openshell creates, so it is off until this turns it on.
OPENSHELL_FILES=(gateway.toml server.pem server.key ca.pem jwt.key jwt.pub jwt.kid)
openshell_up() {
  local etc dir uid gid f problems=() image repo digest
  etc=$(control_etc); dir=$etc/openshell
  uid=$(compose_env_get FLEET_UID); uid=${uid:-10001}
  gid=$(compose_env_get FLEET_GID); gid=${gid:-10001}
  # 1. Its PKI, launch-signing keys and config, private to the UID both containers run as.
  if [ ! -d "$dir" ]; then
    problems+=("$dir does not exist")
  else
    [ "$(stat -c %a "$dir")" = 700 ] || problems+=("$dir must be mode 0700")
    [ "$(stat -c %u:%g "$dir")" = "$uid:$gid" ] || problems+=("$dir must be owned by $uid:$gid (FLEET_UID:FLEET_GID)")
    for f in "${OPENSHELL_FILES[@]}"; do
      if [ ! -f "$dir/$f" ] || [ ! -s "$dir/$f" ]; then problems+=("$dir/$f is missing or empty"); continue; fi
      [ "$(stat -c %u:%g "$dir/$f")" = "$uid:$gid" ] || problems+=("$dir/$f must be owned by $uid:$gid (FLEET_UID:FLEET_GID)")
      case "$f" in *.key) [ "$(stat -c %a "$dir/$f")" = 600 ] || problems+=("$dir/$f is a private key and must be mode 0600");; esac
    done
  fi
  if [ ${#problems[@]} -gt 0 ]; then
    printf 'fleet-ctl: %s\n' "${problems[@]}" >&2
    die "prepare the OpenShell gateway's files as docs/installation.md describes; nothing changed"
  fi
  # 2. Without policy_gateway.openshell the scheduler serves no compute socket, and the gateway exits.
  openshell_configured "$etc/fleet.yaml" || die "$etc/fleet.yaml has no policy_gateway.openshell block, so the scheduler would serve no compute socket for the gateway; configure it (docs/installation.md) first; nothing changed"
  # 3. Pin the image by digest: a revision tag can be pushed again.
  image=$("${COMPOSE[@]}" --profile openshell config --format json openshell-gateway \
    | python3 -c 'import json, sys; print(json.load(sys.stdin)["services"]["openshell-gateway"]["image"])') \
    || die "cannot read the openshell-gateway image from the Compose files; nothing changed"
  if [[ $image == *@sha256:* ]]; then
    echo "OpenShell gateway image is pinned: $image"
  else
    repo=${image%%@*}; [[ ${repo##*/} != *:* ]] || repo=${repo%:*}
    if ! run docker pull "$image"; then
      # A gateway built here (deploy/openshell/Dockerfile) is in no registry, so it has no
      # digest to pin: use the local image as is. Anything else must come from its registry.
      local_digests=$(docker image inspect --format '{{range .RepoDigests}}{{println .}}{{end}}' "$image" 2>/dev/null) \
        || die "cannot pull $image and it is not a local image; nothing changed"
      if grep -q "^$repo@sha256:" <<<"$local_digests"; then
        die "cannot pull $image from its registry; nothing changed"
      fi
      echo "fleet-ctl: $image is a local build with no registry digest; using it unpinned (push it to a registry to pin it by digest)" >&2
    elif [ "$DRY" = 1 ]; then
      echo "+ set FLEET_OPENSHELL_IMAGE=$repo@sha256:<registry digest of $image> in $PREFIX/compose.env"
    else
      digest=$(docker image inspect --format '{{range .RepoDigests}}{{println .}}{{end}}' "$image" \
        | awk -v want="$repo@sha256:" 'index($0, want) == 1 { print; exit }')
      [ -n "$digest" ] || die "$image has no registry digest; set FLEET_OPENSHELL_IMAGE=$repo@sha256:... in $PREFIX/compose.env"
      set_compose_env FLEET_OPENSHELL_IMAGE "$digest"
    fi
  fi
  # 4. The volumes are nocopy, so they start out owned by root. Safe to repeat.
  compose --profile openshell run --rm --no-deps --user 0:0 \
    --cap-add CHOWN --cap-add FOWNER --cap-add DAC_OVERRIDE \
    --entrypoint install openshell-gateway -d -m 700 \
    -o "$uid" -g "$gid" /run/zkm-fleet/openshell /var/lib/openshell \
    || die "cannot prepare the OpenShell volumes"
  # 5. The scheduler reads policy_gateway only at startup, and Compose cannot see a change
  # to the bind-mounted fleet.yaml, so recreate it.
  compose up -d --no-deps --force-recreate scheduler || die "cannot recreate the scheduler"
  scheduler_healthy || die "the scheduler is not healthy with policy_gateway.openshell (its compute_socket must be /run/zkm-fleet/openshell/compute.sock); see docker logs zkm-fleet-scheduler-1"
  # 6. Then the gateway, which connects to that socket as it starts.
  compose --profile openshell up -d --no-deps --wait openshell-gateway \
    || die "the OpenShell gateway is not healthy; see docker logs zkm-fleet-openshell-gateway-1. COMPOSE_PROFILES was left unchanged"
  # 7. Later full `up -d` runs include it.
  add_compose_profile openshell
  echo "OpenShell gateway is up; COMPOSE_PROFILES in $PREFIX/compose.env includes openshell"
}
add_compose_profile() {
  local current; current=$(compose_env_get COMPOSE_PROFILES)
  case ",$current," in *",$1,"*) return 0;; esac
  set_compose_env COMPOSE_PROFILES "${current:+$current,}$1"
}
openshell_configured() { # FLEET_YAML: has a policy_gateway.openshell mapping (block style, as installed)
  [ -f "$1" ] || return 1
  awk '
    function indent(s) { match(s, /^ */); return RLENGTH }
    done || /^[[:space:]]*(#.*)?$/ { next }
    pending { found = indent($0) > key; done = 1; next }
    /^policy_gateway:[[:space:]]*(#.*)?$/ { block = 1; child = -1; next }
    block && indent($0) == 0 { block = 0 }
    block {
      if (child < 0) child = indent($0)
      if (indent($0) == child && $0 ~ /^ *openshell:([[:space:]]|$)/) {
        v = $0; sub(/^ *openshell:[[:space:]]*/, "", v); sub(/^#.*$/, "", v); sub(/[[:space:]]+#.*$/, "", v)
        if (v == "") { pending = 1; key = child } else { found = (v ~ /^\{.*[^[:space:]{}].*\}$/); done = 1 }
      }
    }
    END { exit found ? 0 : 1 }
  ' "$1"
}

node_upgrade() { # node_upgrade FLEET_NODE BRIDGE_DIR [FLEET_PROOF_APP] [DEPLOY_DIR], or node upgrade RELEASE_TAG
  local bin=${1:-} bridge=${2:-} proof_app=${3:-} scripts=${4:-}
  # A release tag is a bare name. A path names local files, even ones a dry run never
  # shipped to the node.
  if [ -n "$bin" ] && [ ! -f "$bin" ] && [[ $bin != */* ]]; then
    local stage; stage=$(mktemp -d)
    prepare_release "$bin" "$stage"
    bin=$RELEASE_ROOT/bin/fleet-node
    bridge=$RELEASE_ROOT/bridge
    proof_app=$RELEASE_ROOT/bin/fleet-proof-app
    scripts=$RELEASE_ROOT/deploy
  fi
  # A dry run never ships or validates downloaded files.
  if [ "$DRY" != 1 ]; then
    [ -f "$bin" ] && [ -d "$bridge/fleet_bridge" ] || die "usage: node upgrade FLEET_NODE_BINARY DIR_CONTAINING_fleet_bridge [FLEET_PROOF_APP_BINARY] [DIR_CONTAINING_the_launch_scripts] or node upgrade RELEASE_TAG"
    [ -z "$proof_app" ] || [ -f "$proof_app" ] || die "no fleet-proof-app binary $proof_app"
    [ -z "$scripts" ] || launch_scripts_dir "$scripts" || die "no launch scripts in $scripts"
  fi
  if remote; then # ship them, then upgrade on the node
    local stage; stage=/tmp/fleet-ctl-$(stamp)
    local args=() name=
    [ "$DRY" = 1 ] || name=$(remote_node_name)
    run ssh "$NODE_SSH" "mkdir -p $stage/bridge"
    run scp -q "$bin" "$NODE_SSH:$stage/fleet-node"
    run scp -q -r "$bridge/fleet_bridge" "$NODE_SSH:$stage/bridge/"
    args+=("$stage/fleet-node" "$stage/bridge")
    if [ -n "$proof_app" ]; then
      run scp -q "$proof_app" "$NODE_SSH:$stage/fleet-proof-app"
      args+=("$stage/fleet-proof-app")
    fi
    if [ -n "$scripts" ]; then
      run ssh "$NODE_SSH" "mkdir -p $stage/scripts"
      local files=(); local s; for s in "${NODE_SCRIPTS[@]}"; do files+=("$scripts/$s"); done
      run scp -q "${files[@]}" "$NODE_SSH:$stage/scripts/"
      # The runtime binaries travel next to fleet-node, where the node's upgrade stages them.
      for s in trusted-shim-runtime trusted-router-tunnel; do
        [ ! -f "$(dirname "$bin")/$s" ] || run scp -q "$(dirname "$bin")/$s" "$NODE_SSH:$stage/$s"
      done
      # Keep the third argument reserved for the optional proof-app binary.
      [ -n "$proof_app" ] || args+=("")
      args+=("$stage/scripts")
    fi
    on_node node upgrade "${args[@]}"
    local restarted; restarted=$(date +%s.%N)
    run ssh "$NODE_SSH" "rm -rf $stage"
    [ "$DRY" = 1 ] || node_reported "$name" "$restarted" || die "node did not reconnect; run: fleet-ctl.sh node rollback"
    return
  fi
  local b; b=$PREFIX/backup/node-$(stamp)
  # Check the new bridge imports in the node's own venv before the agent is touched.
  [ "$DRY" = 1 ] || (cd "$bridge" && "$PREFIX/venv/bin/python" -c "import fleet_bridge.node") || die "the new bridge does not import; nothing changed"
  node_config_check "$bridge"
  node_runtime_check "$bridge"
  node_stage_runtime "$bin" "$scripts"
  run mkdir -p "$b"; run cp -a "$PREFIX/bin/fleet-node" "$PREFIX/bridge" "$b/"
  [ -z "$proof_app" ] || [ ! -f "$PREFIX/bin/fleet-proof-app" ] || run cp -a "$PREFIX/bin/fleet-proof-app" "$b/"
  if [ -n "$scripts" ]; then
    run mkdir -p "$b/scripts"
    local s; for s in "${NODE_SCRIPTS[@]}"; do
      [ ! -f "$PREFIX/deploy/$s" ] || run cp -a "$PREFIX/deploy/$s" "$b/scripts/"
    done
  fi
  run install -m 755 "$bin" "$PREFIX/bin/fleet-node.new"
  run sudo systemctl stop "$NODE_UNIT"
  run mv "$PREFIX/bin/fleet-node.new" "$PREFIX/bin/fleet-node"
  run cp -a "$bridge/fleet_bridge/." "$PREFIX/bridge/fleet_bridge/"
  if [ -n "$scripts" ]; then
    run mkdir -p "$PREFIX/deploy"
    local s; for s in "${NODE_SCRIPTS[@]}"; do
      run install -m 755 "$scripts/$s" "$PREFIX/deploy/$s"
    done
  fi
  local restarted; restarted=$(date +%s.%N)
  run sudo systemctl start "$NODE_UNIT"
  [ "$DRY" = 1 ] || node_reconnected "$restarted" || die "node did not reconnect; run: fleet-ctl.sh node rollback"
  if [ -n "$proof_app" ]; then
    run install -m 755 "$proof_app" "$PREFIX/bin/fleet-proof-app.new"
    run mv "$PREFIX/bin/fleet-proof-app.new" "$PREFIX/bin/fleet-proof-app"
    # Existing endpoint processes keep their old executable mapping. Restarting
    # proof apps is a separate operator action, never part of a node upgrade.
  fi
  echo "node upgraded; previous files in $b"
}
launch_scripts_dir() { # the complete node/companion deployment bundle
  local s; for s in "${NODE_SCRIPTS[@]}"; do
    [ -f "$1/$s" ] || return 1
  done
  return 0
}
node_connected() { # this invocation's agent, including operator wrapper processes
  local invocation
  for _ in $(seq 45); do
    invocation=$(systemctl show --property=InvocationID --value "$NODE_UNIT")
    if [[ "$invocation" =~ ^[0-9a-f]{32}$ ]] && systemctl is-active --quiet "$NODE_UNIT" \
      && journalctl -u "$NODE_UNIT" "_SYSTEMD_INVOCATION_ID=$invocation" --no-pager | grep -q "node connected to control plane"; then return 0; fi
    sleep 2
  done; return 1
}
node_reconnected() { # RESTARTED: the agent logged its connection and the scheduler took its report
  node_connected || return 1
  # Over ssh, the control host asks the scheduler itself.
  [ "${FLEET_CTL_ON_NODE:-0}" = 1 ] || node_reported "$(node_name)" "$1"
}
scheduler_access() { # fleetctl and the admin token file it would use; sets ADMIN_TOKEN
  [ -x "$PREFIX/bin/fleetctl" ] || return 1
  if [ -n "${FLEET_TOKEN_FILE:-}" ]; then ADMIN_TOKEN=$FLEET_TOKEN_FILE; [ -r "$ADMIN_TOKEN" ]; return; fi
  # FLEET_ETC may name the node's directory; the control host's own is the one compose.env names.
  local d; for d in ${FLEET_ETC:+"$FLEET_ETC"} "$(control_etc)" "$HOME/.config/zkm-fleet" /etc/zkm-fleet; do
    ADMIN_TOKEN=$d/admin.token; [ ! -r "$ADMIN_TOKEN" ] || return 0
  done
  return 1
}
# The agent logs that it connected as soon as its channel opens, before the scheduler accepts a
# report. Proof that it works is the node READY with a report newer than RESTARTED (date +%s.%N).
node_reported() { # NAME RESTARTED
  local name=$1 state deadline=$((SECONDS + 90)) left
  if ! [[ $name =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*$ ]] || ! scheduler_access; then
    echo "fleet-ctl: warning: cannot ask the scheduler about ${name:-the node} (this needs $PREFIX/bin/fleetctl, an admin token and the node's FLEET_NODE_NAME); only the agent's log was checked, so confirm it is READY in: fleetctl node" >&2
    return 0
  fi
  echo "waiting up to 90s for the scheduler to report $name READY"
  # One deadline for the whole wait: a scheduler that stops answering costs 90s, not 90s per try.
  for _ in $(seq 45); do
    left=$((deadline - SECONDS)); [ "$left" -gt 0 ] || break
    state=$(python3 - "$PREFIX/bin/fleetctl" "$name" "$ADMIN_TOKEN" "$2" "$left" <<'PY'
import json, os, subprocess, sys, time
fleetctl, name, token, restarted = sys.argv[1], sys.argv[2], sys.argv[3], float(sys.argv[4])
asked = time.time()
try:
    out = subprocess.run([fleetctl, "--json", "node", name], capture_output=True, text=True,
                         timeout=min(30, int(sys.argv[5])),
                         env={**os.environ, "FLEET_TOKEN_FILE": token})
    node = json.loads(out.stdout)
    # The scheduler measures the age when it answers, after `asked`: this never makes a report newer.
    fresh = asked - float(node["heartbeat_age_ms"]) / 1000 > restarted
except (subprocess.TimeoutExpired, ValueError, KeyError, TypeError):
    fresh = False
print("ready" if fresh and node.get("ready") is True else "draining" if fresh and node.get("draining") is True else "waiting")
PY
    ) || state=
    case "$state" in
      ready) echo "the scheduler reports $name READY"; return 0;;
      draining) echo "the scheduler took a report from $name after the restart; it is draining, so check READY once it is undrained"; return 0;;
    esac
    sleep 2
  done
  echo "fleet-ctl: the scheduler has no READY report from $name since the restart" >&2
  return 1
}
remote_node_name() { # the node's FLEET_NODE_NAME, asked over ssh; nothing if unknown
  local name; name=$(on_node node name 2>/dev/null | tail -n 1) || name=
  name=${name//$'\r'/}
  if [[ $name =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*$ ]]; then printf '%s\n' "$name"; fi
}
node_name() { # the name this node's agent reports under: FLEET_NODE_NAME in its watchdog.env
  node_locate
  [ ! -f "$NODE_ENV" ] || bash -c '. "$1"; printf "%s\n" "${FLEET_NODE_NAME:-}"' _ "$NODE_ENV"
}
node_rollback() {
  if remote; then
    local name=; [ "$DRY" = 1 ] || name=$(remote_node_name)
    on_node node rollback
    local restarted; restarted=$(date +%s.%N)
    [ "$DRY" = 1 ] || node_reported "$name" "$restarted" || die "restored node did not reconnect; runtime services were left running"
    return
  fi
  local b; b=$(latest_backup node); [ -n "$b" ] || die "no backup in $PREFIX/backup"
  node_runtime_check "$b/bridge"
  run sudo systemctl stop "$NODE_UNIT"
  run install -m 755 "$b/fleet-node" "$PREFIX/bin/fleet-node"
  run cp -a "$b/bridge/." "$PREFIX/bridge/"
  if [ -d "$b/scripts" ]; then
    run mkdir -p "$PREFIX/deploy"
    local s; for s in "${NODE_SCRIPTS[@]}"; do
      [ ! -f "$b/scripts/$s" ] || run install -m 755 "$b/scripts/$s" "$PREFIX/deploy/$s"
    done
  fi
  if [ -f "$b/fleet-proof-app" ]; then
    run install -m 755 "$b/fleet-proof-app" "$PREFIX/bin/fleet-proof-app"
  fi
  local restarted; restarted=$(date +%s.%N)
  run sudo systemctl start "$NODE_UNIT"
  [ "$DRY" = 1 ] || node_reconnected "$restarted" || die "restored node did not reconnect; runtime services were left running"
  echo "restored $b"
}

node_locate() { # NODE_ENV and NODE_ETC: the watchdog.env and configuration directory the agent reads
  local etc=${FLEET_ETC:-$PREFIX/etc} node_env=${FLEET_ENV:-}
  if [ -z "$node_env" ] && [ -z "${FLEET_ETC:-}" ]; then
    node_env=$(systemctl show --property=Environment --value "$NODE_UNIT" 2>/dev/null | python3 -c \
      'import shlex,sys; print(next((v.split("=",1)[1] for v in shlex.split(sys.stdin.read()) if v.startswith("FLEET_ENV=")),""))')
  fi
  node_env=${node_env:-$etc/watchdog.env}
  if [ -z "${FLEET_ETC:-}" ] && [ -f "$node_env" ]; then
    etc=$(bash -c '. "$1"; printf "%s" "${FLEET_ETC:-$2}"' _ "$node_env" "$etc")
  fi
  NODE_ENV=$node_env NODE_ETC=$etc
}
node_config_check() { # the candidate bridge loads apps.yaml as the new agent's reports will
  [ "$DRY" = 1 ] && return 0
  node_locate
  [ -f "$NODE_ETC/apps.yaml" ] || die "cannot locate apps.yaml; set FLEET_ETC before upgrading; node was not stopped"
  (cd "$1" && "$PREFIX/venv/bin/python" - "$NODE_ETC/apps.yaml" <<'PY'
import sys
from pathlib import Path
import yaml
from fleet_bridge.reconcile import load_apps
path = Path(sys.argv[1])
try:
    data = yaml.safe_load(path.read_text()) or {}
except (OSError, yaml.YAMLError) as error:
    sys.exit(f"fleet-ctl: cannot read {path}: {error}")
try:
    load_apps(data, validate_launch=False)
except Exception as error:
    sys.exit(f"fleet-ctl: the new bridge cannot load {path}: {error}")
PY
  ) || die "the new bridge cannot load apps.yaml; node was not stopped"
}
# A release's shim runtime and Chisel helper sit next to its fleet-node. The release's own
# install-node-services.sh stages them under libexec by content hash; nothing selects, switches
# or restarts them here.
node_stage_runtime() { # FLEET_NODE DEPLOY_DIR
  local shim; shim=$(dirname "$1")/trusted-shim-runtime
  [ -n "$2" ] || return 0
  if [ "$DRY" = 1 ]; then [ -f "$shim" ] || [ -n "${RELEASE_ROOT:-}" ] || return 0
  else
    [ -f "$shim" ] || return 0
    grep -q -- --stage-only "$2/install-node-services.sh" \
      || { echo "fleet-ctl: $2/install-node-services.sh cannot stage runtime binaries; none staged" >&2; return 0; }
  fi
  run bash "$2/install-node-services.sh" "$PREFIX" "" "" "$shim" --stage-only \
    || die "staging the runtime binaries failed; node was not stopped"
  [ "$DRY" = 1 ] || echo "staged the runtime binaries above; apps.yaml still selects the running shim"
}
node_runtime_check() {
  # Use the candidate adapter against the live daemon before stopping the node.
  # Older adapters lacking daemon support are incompatible, not a fallback.
  [ "$DRY" = 1 ] && return 0
  node_locate
  local etc=$NODE_ETC
  [ -f "$etc/apps.yaml" ] || die "cannot locate apps.yaml; set FLEET_ETC before upgrading; node was not stopped"
  (cd "$1" && "$PREFIX/venv/bin/python" - "$etc/apps.yaml" <<'PY'
import sys
from pathlib import Path
import yaml
data = yaml.safe_load(Path(sys.argv[1]).read_text()) or {}
if data.get('shim_daemon'):
    from fleet_bridge.shim import daemon_config, check_daemon
    check_daemon(daemon_config(Path(sys.argv[1])))
PY
  ) || die "runtime compatibility check failed; node was not stopped"
}

app_cleanup() {
  "$PREFIX/venv/bin/python" - "${1:?usage: app cleanup APP-0, e.g. ethproofs-v6-gpu-0}" "$DRY" <<'PY'
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
  echo "== control host: healthz $(curl -fs http://127.0.0.1:9191/healthz || echo FAILING)"
  "${COMPOSE[@]}" ps || true
  "$PREFIX/bin/fleetctl" node || true
  "$PREFIX/bin/fleetctl" app || true
  if [ -n "$NODE_SSH" ]; then on_node status; fi
}

case "${1:-}/${2:-}" in
  status/) status;;
  openshell/up) openshell_up;;
  scheduler/upgrade) scheduler_upgrade "${3:-}";;
  scheduler/rollback) scheduler_rollback "${3:-}";;
  node/upgrade) node_upgrade "${3:-}" "${4:-}" "${5:-}" "${6:-}";;
  node/rollback) node_rollback;;
  node/name) if remote; then on_node node name; else node_name; fi;;
  app/cleanup) if remote; then on_node app cleanup "${3:-}"; else app_cleanup "${3:-}"; fi;;
  *) sed -n '2,/^set -euo/p' "$SELF" | sed '$d;s/^# \{0,1\}//'; exit 2;;
esac
