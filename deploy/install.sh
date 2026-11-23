#!/bin/bash
# One control plane, independent local Ray instances on execution machines.
# control --public-url https://fleet.example:8905 [--tls-names stage,fleet.example] [--no-mcp]
# node --scheduler https://fleet.example:8905 --cluster-token FILE [--cuda 0,1]
# Common: --prefix DIR --etc DIR --bin DIR --workdir DIR --no-cron
# Node: --name NAME --cpus N --jobs-dir DIR --scheduler-ca FILE --with-shim
set -euo pipefail
ROLE=${1:-}; shift || true
PREFIX=/opt/zkm-fleet; ETC=/etc/zkm-fleet; BIN=./target/release; WORKDIR=""; JOBS_DIR=""
CUDA=""; CPUS=""; NAME=$(hostname); SCHEDULER=""; TOKEN=""; CA=""; PUBLIC_URL=""; TLS_NAMES=""; CRON=1; PROOF_ROOTS=""; MCP=1
WITH_SHIM=0
SUBNET=172.28.250.0/24; IP=$(hostname -I | awk '{print $1}')
GIVEN=""
while [ $# -gt 0 ]; do
  GIVEN="$GIVEN $1"
  case "$1" in
    --proof-storage-roots) PROOF_ROOTS=$2; shift 2;;
    --prefix) PREFIX=$2; shift 2;; --etc) ETC=$2; shift 2;; --bin) BIN=$2; shift 2;;
    --workdir) WORKDIR=$2; shift 2;; --jobs-dir) JOBS_DIR=$2; shift 2;;
    --cuda) CUDA=$2; shift 2;; --cpus) CPUS=$2; shift 2;; --name) NAME=$2; shift 2;;
    --scheduler) SCHEDULER=$2; shift 2;; --cluster-token) TOKEN=$2; shift 2;; --scheduler-ca) CA=$2; shift 2;;
    --public-url) PUBLIC_URL=$2; shift 2;; --tls-names) TLS_NAMES=$2; shift 2;;
    --subnet) SUBNET=$2; shift 2;; --ip) IP=$2; shift 2;; --no-cron) CRON=0; shift;;
    --no-mcp) MCP=0; shift;;
    --with-shim) WITH_SHIM=1; CRON=0; shift;;
    *) echo "unknown option $1" >&2; exit 2;;
  esac
done
case "$ROLE" in control|node) ;; *) sed -n '2,6p' "$0"; exit 2;; esac
SRC=$(cd "$(dirname "$0")/.." && pwd)
die(){ echo "[install] $*" >&2; exit 1; }
# Point a per-user convenience link at this installation, unless it already
# leads somewhere real: another live installation's token/CLI, or an operator's
# own file. Installing a second prefix (a rehearsal, a test) must not redirect
# the administrator's existing fleetctl. A missing or dangling link is (re)made.
link_home(){
  local target=$1 link=$2
  if [ -L "$link" ] && [ "$(readlink "$link")" = "$target" ]; then return; fi
  if [ -e "$link" ]; then
    echo "[install] keeping $link -> $(readlink -f "$link"); to use this installation instead: ln -sfn $target $link" >&2
    return
  fi
  ln -sfn "$target" "$link"
}
command -v docker >/dev/null && docker info >/dev/null 2>&1 || die "Docker must be installed and accessible"
# A re-run keeps the node's settings: an install-owned watchdog.env key changes only when its flag
# is given or the key is missing, and this run's paths and name follow the kept values.
given() { [[ " $GIVEN " = *" $1 "* ]]; }
if [ "$ROLE" = node ] && [ -f "$ETC/watchdog.env" ]; then
  KEEP=(); for pair in FLEET_HOME:--prefix FLEET_WORKDIR:--workdir FLEET_JOBS_DIR:--jobs-dir FLEET_NODE_NAME:--name; do
    given "${pair#*:}" || KEEP+=("${pair%%:*}")
  done
  if [ "${#KEEP[@]}" -gt 0 ]; then
    command -v python3 >/dev/null || die "Python >= 3.10 is required on execution machines"
    KEPT=$(mktemp)
    python3 - "$ETC/watchdog.env" "${KEEP[@]}" > "$KEPT" <<'PY' || { rm -f "$KEPT"; die "pass that setting's flag, or fix $ETC/watchdog.env"; }
import re, shlex, sys
path, keys = sys.argv[1], sys.argv[2:]
values = {}
for line in open(path):  # read, never sourced; the last assignment wins, as in the shell
    m = re.match(r'(?:export )?([A-Za-z_][A-Za-z0-9_]*)=(.*)$', line.rstrip('\n'))
    if m and m.group(1) in keys:
        values[m.group(1)] = m.group(2)
for key, raw in values.items():
    try:
        words = shlex.split(raw, comments=True)
    except ValueError:
        words = None
    if words is None or len(words) > 1 or '$' in raw or '`' in raw:
        sys.exit(f'[install] {key} in {path} is not a plain value this installer can keep')
    sys.stdout.write(f'{key}\0{words[0] if words else ""}\0')
PY
    while IFS= read -r -d '' key && IFS= read -r -d '' value; do
      case "$key" in
        FLEET_HOME) PREFIX=$value;; FLEET_WORKDIR) WORKDIR=$value;;
        FLEET_JOBS_DIR) JOBS_DIR=$value;; FLEET_NODE_NAME) NAME=$value;;
      esac
    done < "$KEPT"
    rm -f "$KEPT"
  fi
fi
[ -n "$WORKDIR" ] || WORKDIR="$PREFIX/work"
[ -n "$JOBS_DIR" ] || JOBS_DIR="$WORKDIR/jobs"
mkdir -p "$PREFIX/bin" "$ETC" "$WORKDIR/logs"
chmod 750 "$ETC"
if [ "$ROLE" = control ]; then
  docker compose version >/dev/null || die "Docker compose v2 is required"
  # Check everything before replacing deployment files or starting any services.
  # The OpenShell gateway is optional: fleet-ctl.sh openshell up checks its files.
  command -v openssl >/dev/null || die "openssl is required on the control host (it generates the database password)"
  command -v python3 >/dev/null || die "python3 is required on the control host (fleet-ctl.sh uses it)"
  # Postgres refuses an empty password.
  [ ! -e "$ETC/pg_password" ] || [ -s "$ETC/pg_password" ] || die "$ETC/pg_password is empty. If Postgres was never initialised (its volume is new or empty), remove $ETC/pg_password and $ETC/database_url and run the installer again; otherwise write the database's password into $ETC/pg_password and $ETC/database_url"
  [ -x "$BIN/fleetctl" ] || die "build fleetctl first, or pass --bin"
  install -m 755 "$BIN/fleetctl" "$PREFIX/bin/"
else
  [ -n "$SCHEDULER" ] && [ -f "$TOKEN" ] || die "--scheduler HTTPS_URL and --cluster-token FILE are required"
  [[ "$NAME" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*$ ]] || die "invalid --name"
  command -v python3 >/dev/null || die "Python >= 3.10 is required on execution machines"
  python3 -c 'import sys; sys.exit(sys.version_info < (3,10))' || die "Python >= 3.10 is required"
  [ -x "$BIN/fleet-node" ] || die "build fleet-node for this machine first"
  if [ "$WITH_SHIM" = 1 ]; then
    [ -x "$BIN/trusted-shim-runtime" ] || die "--with-shim requires the bundled trusted-shim-runtime"
    command -v systemctl >/dev/null || die "--with-shim requires systemd"
    # Validate before replacing binaries, Python code or watchdog ownership.
    # Retain this lock until the service installer completes its handover.
    exec 9>"$WORKDIR/node-agent.lock"
    flock -n 9 || die "control agent is running; use fleet-ctl node upgrade, or stop only the control agent before first supervisor conversion"
    bash "$SRC/deploy/install-node-services.sh" "$PREFIX" "$ETC" "$WORKDIR" "$BIN/trusted-shim-runtime" --check
  fi
  [ -z "$PROOF_ROOTS" ] || [ -x "$BIN/fleet-ziren-node" ] || [ -x "$PREFIX/bin/fleet-ziren-node" ] \
    || die "--proof-storage-roots requires the optional fleet-ziren-node storage handler"
  install -m 755 "$BIN/fleet-node" "$PREFIX/bin/fleet-node.new"
  mv "$PREFIX/bin/fleet-node.new" "$PREFIX/bin/fleet-node"
  # Only SDK proof actors and their storage handler need the native prover.
  # Ordinary apps, batches and integrated workers carry their own application code.
  for b in fleet-ziren-node fleet-proof-app; do
    if [ -x "$BIN/$b" ]; then install -m 755 "$BIN/$b" "$PREFIX/bin/"; fi
  done
  if [ -n "$CUDA" ]; then
    command -v nvidia-smi >/dev/null || die "NVIDIA driver is required"
    docker info 2>/dev/null | grep -qi nvidia || die "configure nvidia-container-toolkit first"
  fi
fi
# Copy execution code only to execution machines. The control plane has no Ray dependency.
mkdir -p "$PREFIX/deploy"
if [ "$ROLE" = control ]; then
  cp "$SRC"/deploy/{docker-compose.yml,gateway.yaml.in} "$PREFIX/deploy/"
  install -m 755 "$SRC/deploy/fleet-ctl.sh" "$PREFIX/deploy/fleet-ctl.sh"
  mkdir -p "$PREFIX/deploy/trustee"
  cp "$SRC/deploy/trustee/as.json" "$PREFIX/deploy/trustee/as.json"
  FLEETCTL="$PREFIX/bin/fleetctl"
  [ -f "$ETC/cluster.token" ] || "$FLEETCTL" init token --out "$ETC/cluster.token" >/dev/null
  if [ ! -f "$ETC/pg_password" ]; then # written whole or not at all
    PG_STAGE=$(mktemp "$ETC/pg_password.XXXXXX")
    if ! openssl rand -hex 24 > "$PG_STAGE" || [ ! -s "$PG_STAGE" ]; then
      rm -f "$PG_STAGE"; die "openssl could not generate the database password"
    fi
    mv "$PG_STAGE" "$ETC/pg_password"
  fi
  [ -f "$ETC/database_url" ] || (umask 077; printf 'postgres://fleet:%s@postgres:5432/fleet\n' "$(cat "$ETC/pg_password")" > "$ETC/database_url")
  [ -n "$PUBLIC_URL" ] || PUBLIC_URL="https://$IP:8905"
  [ -f "$ETC/tls/ca.pem" ] || "$FLEETCTL" init tls --dir "$ETC/tls" --names "${TLS_NAMES:-stage,$(hostname),$IP}" >/dev/null
  [ -f "$ETC/fleet.yaml" ] || cat > "$ETC/fleet.yaml" <<EOF
control_addr: 0.0.0.0:51001
control_tls:
  cert_path: /etc/zkm-fleet/tls/server.pem
  key_path: /etc/zkm-fleet/tls/server.key
http_addr: 0.0.0.0:9191
database_url_file: /etc/zkm-fleet/database_url
data_dir: /var/lib/zkm-fleet
cluster_token_file: /etc/zkm-fleet/cluster.token
EOF
  MCP_AUTHORITY=${PUBLIC_URL#https://}; MCP_AUTHORITY=${MCP_AUTHORITY%%/*}
  SCHED_IP="${SUBNET%.*}.10"
  sed "s/@SCHEDULER_IP@/$SCHED_IP/g" "$PREFIX/deploy/gateway.yaml.in" > "$ETC/gateway.yaml"
  mkdir -p "$PREFIX/data" "$HOME/.config/zkm-fleet" "$HOME/.local/bin"
  link_home "$ETC/admin.token" "$HOME/.config/zkm-fleet/admin.token"
  link_home "$FLEETCTL" "$HOME/.local/bin/fleetctl"
  IMG=${FLEET_SCHEDULER_IMAGE:-zkm-fleet-scheduler:local}
  # Rewrite only the keys this installer owns, and keep every other line (FLEET_OPENSHELL_*
  # and other site settings). Of COMPOSE_PROFILES it manages only mcp; profiles enabled
  # elsewhere (openshell, trustee) stay.
  PROFILES=()
  if [ -f "$PREFIX/compose.env" ]; then
    IFS=, read -ra OLD_PROFILES <<< "$(sed -n 's/^\(export \)\{0,1\}COMPOSE_PROFILES=//p' "$PREFIX/compose.env" | tail -1 | tr -d "\"' ")"
    for p in "${OLD_PROFILES[@]}"; do [ -z "$p" ] || [ "$p" = mcp ] || PROFILES+=("$p"); done
  fi
  [ "$MCP" = 0 ] || PROFILES+=(mcp)
  {
    if [ -f "$PREFIX/compose.env" ]; then
      awk '!/^(export )?(FLEET_ETC|FLEET_DATA|FLEET_UID|FLEET_GID|FLEET_SUBNET|FLEET_SCHEDULER_IP|FLEET_MCP_ALLOWED_HOSTS|COMPOSE_PROFILES|FLEET_SCHEDULER_IMAGE)=/' "$PREFIX/compose.env"
    fi
    cat <<EOF
FLEET_ETC=$ETC
FLEET_DATA=$PREFIX/data
FLEET_UID=$(id -u)
FLEET_GID=$(id -g)
FLEET_SUBNET=$SUBNET
FLEET_SCHEDULER_IP=$SCHED_IP
FLEET_MCP_ALLOWED_HOSTS=$MCP_AUTHORITY,localhost:8906,127.0.0.1:8906
COMPOSE_PROFILES=$(IFS=,; echo "${PROFILES[*]}")
FLEET_SCHEDULER_IMAGE=$IMG
EOF
  } > "$PREFIX/compose.env.new"
  [ ! -f "$PREFIX/compose.env" ] || chmod --reference="$PREFIX/compose.env" "$PREFIX/compose.env.new"
  mv "$PREFIX/compose.env.new" "$PREFIX/compose.env"
  # The release's compose file, then this site's changes to it, as fleet-ctl.sh runs them.
  COMPOSE=(docker compose --env-file "$PREFIX/compose.env" -p zkm-fleet -f "$PREFIX/deploy/docker-compose.yml")
  [ ! -f "$PREFIX/deploy/docker-compose.override.yml" ] || COMPOSE+=(-f "$PREFIX/deploy/docker-compose.override.yml")
  if ! docker image inspect "$IMG" >/dev/null 2>&1; then
    if [ -n "${FLEET_SCHEDULER_IMAGE:-}" ]; then
      docker pull "$IMG" || die "cannot pull $IMG"
    else # a source checkout builds its own image
      [ -d "$SRC/crates" ] || die "set FLEET_SCHEDULER_IMAGE or install from a full checkout"
      docker build -f "$SRC/deploy/Dockerfile.scheduler" -t "$IMG" "$SRC"
    fi
  fi
  "${COMPOSE[@]}" up -d
  for _ in $(seq 1 30); do curl -sf http://127.0.0.1:9191/healthz >/dev/null && break; sleep 2; done
  curl -sf http://127.0.0.1:9191/healthz >/dev/null || die "scheduler health check failed"
  # The first administrator's token goes into the database; its secret lands in the data
  # directory (the configuration is mounted read-only) and moves beside the other secrets.
  if [ ! -f "$ETC/admin.token" ]; then
    "${COMPOSE[@]}" exec -T scheduler \
      fleet-scheduler -c /etc/zkm-fleet/fleet.yaml issue-admin --label install --out /var/lib/zkm-fleet/admin.token >/dev/null
    (umask 077; mv "$PREFIX/data/admin.token" "$ETC/admin.token")
  fi
  echo "[install] control plane ready; fleetctl connects to http://127.0.0.1:9191"
  echo "[install] HTTPS gateway: $PUBLIC_URL; MCP: $PUBLIC_URL/mcp"
  case ",$(IFS=,; echo "${PROFILES[*]}")," in
    *,openshell,*) ;;
    *) echo "[install] OpenShell gateway: off; run $PREFIX/deploy/fleet-ctl.sh openshell up once policy_gateway.openshell is configured";;
  esac
else
  cp "$SRC"/deploy/{fleet-watchdog.sh,ray-start.sh,node-start.sh} "$PREFIX/deploy/"
  cp "$SRC"/deploy/{shim-start.sh,ray-watch.sh,install-node-services.sh} "$PREFIX/deploy/"
  # The node holds the cluster token, never the administrator or user tokens.
  [ "$(readlink -f "$TOKEN")" = "$(readlink -m "$ETC/cluster.token")" ] || install -m 600 "$TOKEN" "$ETC/cluster.token"
  if [ -n "$CA" ]; then
    [ "$(readlink -f "$CA")" = "$(readlink -m "$ETC/scheduler-ca.pem")" ] || install -m 644 "$CA" "$ETC/scheduler-ca.pem"
    CA="$ETC/scheduler-ca.pem"
  fi
  mkdir -p "$PREFIX/bridge"
  cp "$SRC/bridge/pyproject.toml" "$PREFIX/bridge/"
  cp -r "$SRC/bridge/fleet_bridge" "$PREFIX/bridge/"
  if [ ! -x "$PREFIX/venv/bin/python" ]; then
    python3 -m venv "$PREFIX/venv"
  fi
  "$PREFIX/venv/bin/pip" install --timeout 120 --retries 10 "$SRC/bridge"
  [ -f "$ETC/apps.yaml" ] || cat > "$ETC/apps.yaml" <<EOF
scheduler: $SCHEDULER
cluster_token_file: $ETC/cluster.token
node_binary: $PREFIX/bin/fleet-ziren-node
log_dir: $WORKDIR/logs
apps: [] # configured proof servers and persistent apps for this machine
EOF
  # A different --scheduler reaches apps.yaml too; only its scheduler line changes.
  (cd "$PREFIX/bridge" && "$PREFIX/venv/bin/python" - "$ETC/apps.yaml" "$SCHEDULER" <<'PY'
import json, re, sys, yaml
from pathlib import Path
path, url = Path(sys.argv[1]), sys.argv[2]
refuse = f'[install] cannot change scheduler in {path} safely; set it to {url} there and rerun'
text = path.read_text()
try:
    data = yaml.safe_load(text) or {}
except yaml.YAMLError:
    sys.exit(refuse)
if not isinstance(data, dict):
    sys.exit(refuse)
if data.get('scheduler') == url:
    sys.exit(0)
lines = text.splitlines(keepends=True)
own = [i for i, line in enumerate(lines) if re.match(r'scheduler\s*:', line)]
new = None
if 'scheduler' not in data and not own:
    new = f'scheduler: {json.dumps(url)}\n' + text
elif len(own) == 1:
    # Keep a comment after a plain value; quoted or multi-line values fail the check below.
    m = re.match(r'scheduler\s*:\s*(?:[^\'"\s#][^#]*?)?(\s+#.*)?\s*$', lines[own[0]].rstrip('\r\n'))
    comment = (m.group(1) or '') if m else ''
    lines[own[0]] = f'scheduler: {json.dumps(url)}{comment}\n'
    new = ''.join(lines)
try:
    safe = new is not None and yaml.safe_load(new) == {**data, 'scheduler': url}
except yaml.YAMLError:
    safe = False
if not safe:
    sys.exit(refuse)
from fleet_bridge.node import _replace
_replace(path, new)
PY
  ) || die "apps.yaml and watchdog.env still name the previous scheduler"
  if [ -n "$CA" ] && ! grep -q '^scheduler_ca_file:' "$ETC/apps.yaml"; then
    printf '\nscheduler_ca_file: %s\n' "$CA" >> "$ETC/apps.yaml"
  fi
  [ -f "$ETC/batches.yaml" ] || echo 'batches: {} # initial operator-managed batches' > "$ETC/batches.yaml"
  mkdir -p "$JOBS_DIR"
  if [ -n "$PROOF_ROOTS" ]; then
    [ -f "$PROOF_ROOTS" ] || die "--proof-storage-roots must name a file with one approved root per line"
    if [ "$PROOF_ROOTS" != "$ETC/proof-storage-roots" ]; then
      install -m 640 "$PROOF_ROOTS" "$ETC/proof-storage-roots"
    fi
  fi
  WATCHDOG_STAGE=$(mktemp "$ETC/watchdog.env.XXXXXX")
  # Rewrite an install-owned setting only when its flag was given or it is missing, retaining
  # custom settings and the independent service-supervision flag. Never source operator shell here.
  REWRITE=(); NEW=()
  owned() { # KEY FLAG VALUE
    if given "$2" || ! grep -Eq "^(export )?$1=" "$ETC/watchdog.env" 2>/dev/null; then
      REWRITE+=("$1"); NEW+=("$(printf '%s=%q' "$1" "$3")")
    fi
  }
  owned FLEET_HOME --prefix "$PREFIX"; owned FLEET_ETC --etc "$ETC"
  owned FLEET_LOG --workdir "$WORKDIR/logs"; owned FLEET_WORKDIR --workdir "$WORKDIR"
  owned FLEET_SCHEDULER --scheduler "$SCHEDULER"; owned FLEET_NODE_NAME --name "$NAME"
  owned FLEET_JOBS_DIR --jobs-dir "$JOBS_DIR"; owned FLEET_SCHEDULER_CA --scheduler-ca "$CA"
  [ ! -f "$ETC/proof-storage-roots" ] || owned FLEET_PROOF_STORAGE_ROOTS_FILE --proof-storage-roots "$ETC/proof-storage-roots"
  owned RAY_NUM_CPUS --cpus "$CPUS"; owned CUDA_VISIBLE_DEVICES --cuda "$CUDA"
  {
    if [ -f "$ETC/watchdog.env" ]; then
      awk -v keys=" ${REWRITE[*]} " '{ k = $0; sub(/^export /, "", k); sub(/=.*/, "", k) }
        !(index($0, "=") && index(keys, " " k " "))' "$ETC/watchdog.env"
    fi
    [ "${#NEW[@]}" = 0 ] || printf '%s\n' "${NEW[@]}"
  } > "$WATCHDOG_STAGE"
  if [ -f "$ETC/watchdog.env" ]; then chmod --reference="$ETC/watchdog.env" "$WATCHDOG_STAGE"; fi
  mv "$WATCHDOG_STAGE" "$ETC/watchdog.env"
  if [ "$WITH_SHIM" = 1 ]; then
    bash "$PREFIX/deploy/install-node-services.sh" "$PREFIX" "$ETC" "$WORKDIR" "$BIN/trusted-shim-runtime"
  elif [ "$CRON" = 1 ]; then
    FLEET_ENV="$ETC/watchdog.env" "$PREFIX/deploy/fleet-watchdog.sh"
  else # the operator's supervisor starts them; a nohup agent here would hold its lock
    echo "[install] --no-cron: nothing was started. Supervise both $PREFIX/deploy/node-start.sh and Ray ($PREFIX/deploy/ray-start.sh) yourself, with the settings in $ETC/watchdog.env"
  fi
  if [ "$CRON" = 1 ]; then
    command -v crontab >/dev/null || die "install cron or supervise node-start.sh yourself with --no-cron"
    LINE="* * * * * FLEET_ENV=$ETC/watchdog.env $PREFIX/deploy/fleet-watchdog.sh >> $WORKDIR/logs/watchdog.log 2>&1"
    if ! crontab -l 2>/dev/null | grep -qF "$PREFIX/deploy/fleet-watchdog.sh"; then
      (crontab -l 2>/dev/null || true; echo "$LINE") | crontab -
    fi
  fi
  echo "[install] node $NAME uses local Ray and outbound HTTPS heartbeats to $SCHEDULER"
fi
