#!/bin/bash
# One control plane, independent local Ray instances on execution machines.
# control --public-url https://fleet.example:8905 [--tls-names stage,fleet.example] [--no-mcp]
# node --scheduler https://fleet.example:8905 --cluster-token FILE [--cuda 0,1]
# Common: --prefix DIR --etc DIR --bin DIR --workdir DIR --no-cron
# Node: --name NAME --cpus N --jobs-dir DIR --scheduler-ca FILE
set -euo pipefail
ROLE=${1:-}; shift || true
PREFIX=/opt/zkm-fleet; ETC=/etc/zkm-fleet; BIN=./target/release; WORKDIR=""; JOBS_DIR=""
CUDA=""; CPUS=""; NAME=$(hostname); SCHEDULER=""; TOKEN=""; CA=""; PUBLIC_URL=""; TLS_NAMES=""; CRON=1; PROOF_ROOTS=""; MCP=1
SUBNET=172.28.250.0/24; IP=$(hostname -I | awk '{print $1}')
while [ $# -gt 0 ]; do
  case "$1" in
    --proof-storage-roots) PROOF_ROOTS=$2; shift 2;;
    --prefix) PREFIX=$2; shift 2;; --etc) ETC=$2; shift 2;; --bin) BIN=$2; shift 2;;
    --workdir) WORKDIR=$2; shift 2;; --jobs-dir) JOBS_DIR=$2; shift 2;;
    --cuda) CUDA=$2; shift 2;; --cpus) CPUS=$2; shift 2;; --name) NAME=$2; shift 2;;
    --scheduler) SCHEDULER=$2; shift 2;; --cluster-token) TOKEN=$2; shift 2;; --scheduler-ca) CA=$2; shift 2;;
    --public-url) PUBLIC_URL=$2; shift 2;; --tls-names) TLS_NAMES=$2; shift 2;;
    --subnet) SUBNET=$2; shift 2;; --ip) IP=$2; shift 2;; --no-cron) CRON=0; shift;;
    --no-mcp) MCP=0; shift;;
    *) echo "unknown option $1" >&2; exit 2;;
  esac
done
case "$ROLE" in control|node) ;; *) sed -n '2,6p' "$0"; exit 2;; esac
SRC=$(cd "$(dirname "$0")/.." && pwd)
die(){ echo "[install] $*" >&2; exit 1; }
command -v docker >/dev/null && docker info >/dev/null 2>&1 || die "Docker must be installed and accessible"
[ -n "$WORKDIR" ] || WORKDIR="$PREFIX/work"
[ -n "$JOBS_DIR" ] || JOBS_DIR="$WORKDIR/jobs"
mkdir -p "$PREFIX/bin" "$ETC" "$WORKDIR/logs"
chmod 750 "$ETC"
if [ "$ROLE" = control ]; then
  docker compose version >/dev/null || die "Docker compose v2 is required"
  [ -x "$BIN/fleetctl" ] || die "build fleetctl first, or pass --bin"
  install -m 755 "$BIN/fleetctl" "$PREFIX/bin/"
else
  [ -n "$SCHEDULER" ] && [ -f "$TOKEN" ] || die "--scheduler HTTPS_URL and --cluster-token FILE are required"
  [[ "$NAME" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*$ ]] || die "invalid --name"
  command -v python3 >/dev/null || die "Python >= 3.10 is required on execution machines"
  python3 -c 'import sys; sys.exit(sys.version_info < (3,10))' || die "Python >= 3.10 is required"
  [ -x "$BIN/fleet-node" ] || die "build fleet-node for this machine first"
  [ -z "$PROOF_ROOTS" ] || [ -x "$BIN/fleet-ziren-node" ] || [ -x "$PREFIX/bin/fleet-ziren-node" ] \
    || die "--proof-storage-roots requires the optional fleet-ziren-node storage handler"
  install -m 755 "$BIN/fleet-node" "$PREFIX/bin/"
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
  FLEETCTL="$PREFIX/bin/fleetctl"
  [ -f "$ETC/cluster.token" ] || "$FLEETCTL" init token --out "$ETC/cluster.token" >/dev/null
  [ -f "$ETC/pg_password" ] || (umask 077; openssl rand -hex 24 > "$ETC/pg_password")
  [ -f "$ETC/database_url" ] || (umask 077; printf 'postgres://fleet:%s@postgres:5432/fleet\n' "$(cat "$ETC/pg_password")" > "$ETC/database_url")
  [ -n "$PUBLIC_URL" ] || PUBLIC_URL="https://$IP:8905"
  [ -f "$ETC/tls/ca.pem" ] || "$FLEETCTL" init tls --dir "$ETC/tls" --names "${TLS_NAMES:-stage,$(hostname),$IP}" >/dev/null
  [ -f "$ETC/fleet.yaml" ] || cat > "$ETC/fleet.yaml" <<EOF
stage_addr: 0.0.0.0:51000
control_addr: 0.0.0.0:51001
control_tls:
  cert_path: /etc/zkm-fleet/tls/server.pem
  key_path: /etc/zkm-fleet/tls/server.key
http_addr: 0.0.0.0:9191
public_url: $PUBLIC_URL
database_url_file: /etc/zkm-fleet/database_url
data_dir: /var/lib/zkm-fleet
cluster_token_file: /etc/zkm-fleet/cluster.token
EOF
  MCP_AUTHORITY=${PUBLIC_URL#https://}; MCP_AUTHORITY=${MCP_AUTHORITY%%/*}
  SCHED_IP="${SUBNET%.*}.10"
  sed "s/@SCHEDULER_IP@/$SCHED_IP/g" "$PREFIX/deploy/gateway.yaml.in" > "$ETC/gateway.yaml"
  mkdir -p "$PREFIX/data" "$HOME/.config/zkm-fleet" "$HOME/.local/bin"
  ln -sf "$ETC/admin.token" "$HOME/.config/zkm-fleet/admin.token"
  ln -sf "$FLEETCTL" "$HOME/.local/bin/fleetctl"
  IMG=${FLEET_SCHEDULER_IMAGE:-zkm-fleet-scheduler:local}
  cat > "$PREFIX/compose.env" <<EOF
FLEET_ETC=$ETC
FLEET_DATA=$PREFIX/data
FLEET_UID=$(id -u)
FLEET_GID=$(id -g)
FLEET_SUBNET=$SUBNET
FLEET_SCHEDULER_IP=$SCHED_IP
FLEET_MCP_ALLOWED_HOSTS=$MCP_AUTHORITY,localhost:8906,127.0.0.1:8906
COMPOSE_PROFILES=$([ "$MCP" = 1 ] && echo mcp || true)
FLEET_SCHEDULER_IMAGE=$IMG
EOF
  if ! docker image inspect "$IMG" >/dev/null 2>&1; then
    if [ -n "${FLEET_SCHEDULER_IMAGE:-}" ]; then
      docker pull "$IMG" || die "cannot pull $IMG"
    else # a source checkout builds its own image
      [ -d "$SRC/crates" ] || die "set FLEET_SCHEDULER_IMAGE or install from a full checkout"
      docker build -f "$SRC/deploy/Dockerfile.scheduler" -t "$IMG" "$SRC"
    fi
  fi
  docker compose --env-file "$PREFIX/compose.env" -p zkm-fleet -f "$PREFIX/deploy/docker-compose.yml" up -d
  for _ in $(seq 1 30); do curl -sf http://127.0.0.1:9191/healthz >/dev/null && break; sleep 2; done
  curl -sf http://127.0.0.1:9191/healthz >/dev/null || die "scheduler health check failed"
  # The first administrator's token goes into the database; its secret lands in the data
  # directory (the configuration is mounted read-only) and moves beside the other secrets.
  if [ ! -f "$ETC/admin.token" ]; then
    docker compose --env-file "$PREFIX/compose.env" -p zkm-fleet -f "$PREFIX/deploy/docker-compose.yml" exec -T scheduler \
      fleet-scheduler -c /etc/zkm-fleet/fleet.yaml issue-admin --label install --out /var/lib/zkm-fleet/admin.token >/dev/null
    (umask 077; mv "$PREFIX/data/admin.token" "$ETC/admin.token")
  fi
  echo "[install] control plane ready; fleetctl connects to http://127.0.0.1:9191"
  echo "[install] HTTPS gateway: $PUBLIC_URL; MCP: $PUBLIC_URL/mcp"
else
  cp "$SRC"/deploy/{fleet-watchdog.sh,ray-start.sh,node-start.sh} "$PREFIX/deploy/"
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
  {
    printf 'FLEET_HOME=%q\nFLEET_ETC=%q\nFLEET_LOG=%q\nFLEET_WORKDIR=%q\n' "$PREFIX" "$ETC" "$WORKDIR/logs" "$WORKDIR"
    printf 'FLEET_SCHEDULER=%q\nFLEET_NODE_NAME=%q\nFLEET_JOBS_DIR=%q\nFLEET_SCHEDULER_CA=%q\n' "$SCHEDULER" "$NAME" "$JOBS_DIR" "$CA"
    [ ! -f "$ETC/proof-storage-roots" ] || printf 'FLEET_PROOF_STORAGE_ROOTS_FILE=%q\n' "$ETC/proof-storage-roots"
    printf 'RAY_NUM_CPUS=%q\nCUDA_VISIBLE_DEVICES=%q\n' "$CPUS" "$CUDA"
  } > "$ETC/watchdog.env"
  FLEET_ENV="$ETC/watchdog.env" "$PREFIX/deploy/fleet-watchdog.sh"
  if [ "$CRON" = 1 ]; then
    command -v crontab >/dev/null || die "install cron or supervise node-start.sh yourself with --no-cron"
    LINE="* * * * * FLEET_ENV=$ETC/watchdog.env $PREFIX/deploy/fleet-watchdog.sh >> $WORKDIR/logs/watchdog.log 2>&1"
    if ! crontab -l 2>/dev/null | grep -qF "$PREFIX/deploy/fleet-watchdog.sh"; then
      (crontab -l 2>/dev/null || true; echo "$LINE") | crontab -
    fi
  fi
  echo "[install] node $NAME uses local Ray and outbound HTTPS heartbeats to $SCHEDULER"
fi
