#!/bin/bash
# zkm-fleet installer: one command per machine, safe to re-run.
#
#   deploy/install.sh control [options]          scheduler + Postgres (compose), Ray head, prover actors
#   deploy/install.sh node --head IP [options]   Ray worker on a GPU / CPU machine
#
# Options:
#   --prefix DIR          install directory           (default /opt/zkm-fleet)
#   --etc DIR             config and secrets          (default /etc/zkm-fleet)
#   --bin DIR             where fleet-node and fleetctl were built (default ./target/release)
#   --cuda LIST           cards Ray may hand out on this machine, e.g. 0,1,2,3 (default: none)
#   --cpus N              CPUs Ray may hand out here (default: all; leave room on shared machines)
#   --workdir DIR         this node's workspace: Ray session files, spilled objects, node logs
#                         (default <prefix>/work; keep it off a root disk that must stay free)
#   --head IP             Ray head address (node role)
#   --cluster-token FILE  the control machine's cluster.token (node role)
#   --ip IP               this machine's cluster address (default: first address of `hostname -I`)
#   --public-url URL      how clients reach the scheduler's http port (control; default http://<ip>:9191)
#   --tls-names LIST      names on the stage certificate (control; default stage,<hostname>)
#   --no-tls              no TLS on the stage API: only when it is bound to loopback (control)
#   --bind-stage IP       interface for the stage API (control; default 0.0.0.0)
#   --bind-http IP        interface for public values and /admin (control; default 0.0.0.0)
#   --no-cron             do not install the cron watchdog (containers, tests)
#
# Without root, pass --prefix and --etc under $HOME: Ray is kept alive by a cron watchdog.
set -euo pipefail

ROLE=${1:-}; shift || true
PREFIX=/opt/zkm-fleet; ETC=/etc/zkm-fleet; BIN=./target/release; CUDA=""; CPUS=""; WORKDIR=""; HEAD=""; TOKEN=""; IP=""
PUBLIC_URL=""; TLS_NAMES=""; TLS=1; BIND_STAGE=0.0.0.0; BIND_HTTP=0.0.0.0; CRON=1
RAY_VERSION=2.59.0
while [ $# -gt 0 ]; do
  case "$1" in
    --prefix) PREFIX=$2; shift 2 ;;
    --etc) ETC=$2; shift 2 ;;
    --bin) BIN=$2; shift 2 ;;
    --cuda) CUDA=$2; shift 2 ;;
    --cpus) CPUS=$2; shift 2 ;;
    --workdir) WORKDIR=$2; shift 2 ;;
    --head) HEAD=$2; shift 2 ;;
    --cluster-token) TOKEN=$2; shift 2 ;;
    --ip) IP=$2; shift 2 ;;
    --public-url) PUBLIC_URL=$2; shift 2 ;;
    --tls-names) TLS_NAMES=$2; shift 2 ;;
    --no-tls) TLS=0; shift ;;
    --bind-stage) BIND_STAGE=$2; shift 2 ;;
    --bind-http) BIND_HTTP=$2; shift 2 ;;
    --no-cron) CRON=0; shift ;;
    *) echo "unknown option $1"; exit 2 ;;
  esac
done
case "$ROLE" in control|node) ;; *) sed -n '2,25p' "$0"; exit 2 ;; esac

SRC=$(cd "$(dirname "$0")/.." && pwd)
say(){ echo "[install] $*"; }
die(){ echo "[install] ERROR: $*" >&2; exit 1; }

# ------------------------------------------------------------------ checks
if [ "$ROLE" = control ] || [ -n "$CUDA" ]; then
  command -v docker > /dev/null || die "docker is not installed"
  docker info > /dev/null 2>&1 || die "this user cannot talk to docker (add it to the docker group)"
fi
command -v python3 > /dev/null || die "python3 is not installed"
python3 -c 'import sys; sys.exit(sys.version_info < (3, 10))' || die "python3 >= 3.10 is required"
python3 -m venv --help > /dev/null 2>&1 || die "python3-venv is not installed"
if [ -n "$CUDA" ]; then
  docker info 2>/dev/null | grep -qi nvidia || die "the NVIDIA container runtime is not configured (nvidia-container-toolkit)"
  command -v nvidia-smi > /dev/null || die "nvidia-smi not found"
fi
[ "$ROLE" = control ] && { docker compose version > /dev/null 2>&1 || die "docker compose v2 is not installed"; }
[ "$ROLE" = node ] && { [ -n "$HEAD" ] || die "--head is required for a node"; [ -f "$TOKEN" ] || die "--cluster-token FILE is required for a node"; }
for b in fleet-node fleetctl; do [ -x "$BIN/$b" ] || die "$BIN/$b not found: build first (cargo build --release --bins), or pass --bin"; done
[ -n "$IP" ] || IP=$(hostname -I | awk '{print $1}')
[ -n "$WORKDIR" ] || WORKDIR="$PREFIX/work"
mkdir -p "$PREFIX" "$ETC" 2>/dev/null || die "cannot create $PREFIX / $ETC: run as root, or pass --prefix and --etc under \$HOME"
chmod 750 "$ETC"

# ----------------------------------------------------------- files and venv
say "installing into $PREFIX (config in $ETC)"
mkdir -p "$PREFIX/bin" "$WORKDIR/logs"
install -m 755 "$BIN/fleet-node" "$BIN/fleetctl" "$PREFIX/bin/"
rm -rf "$PREFIX/bridge" "$PREFIX/deploy"
cp -r "$SRC/bridge" "$SRC/deploy" "$PREFIX/"
if ! "$PREFIX/venv/bin/python" -c "import ray, yaml, sys; sys.exit(ray.__version__ != '$RAY_VERSION')" 2>/dev/null; then
  say "installing Ray $RAY_VERSION"
  python3 -m venv "$PREFIX/venv"
  "$PREFIX/venv/bin/pip" install -q --timeout 120 --retries 10 "ray==$RAY_VERSION" pyyaml
fi
FLEETCTL="$PREFIX/bin/fleetctl"
# fleetctl on PATH: /usr/local/bin with root, ~/.local/bin without.
if [ -w /usr/local/bin ]; then LINKDIR=/usr/local/bin; else LINKDIR="$HOME/.local/bin"; mkdir -p "$LINKDIR"; fi
ln -sf "$FLEETCTL" "$LINKDIR/fleetctl"

# ------------------------------------------------------------------ secrets
if [ "$ROLE" = control ]; then
  [ -f "$ETC/cluster.token" ] || "$FLEETCTL" token new --out "$ETC/cluster.token" > /dev/null
  [ -f "$ETC/admin.token" ] || "$FLEETCTL" token new --out "$ETC/admin.token" > /dev/null
  if [ ! -f "$ETC/pg_password" ]; then
    (umask 077; head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n' > "$ETC/pg_password")
  fi
  (umask 077; echo "postgres://fleet:$(cat "$ETC/pg_password")@postgres:5432/fleet" > "$ETC/database_url")
  if [ "$TLS" = 1 ] && [ ! -f "$ETC/tls/ca.pem" ]; then
    "$FLEETCTL" tls init --dir "$ETC/tls" --names "${TLS_NAMES:-stage,$(hostname)}" > /dev/null
  fi
else
  install -m 600 "$TOKEN" "$ETC/cluster.token"
fi

# With a non-default config directory, let fleetctl find the admin token without flags.
if [ "$ROLE" = control ] && [ "$ETC" != /etc/zkm-fleet ]; then
  mkdir -p "$HOME/.config/zkm-fleet"
  ln -sf "$ETC/admin.token" "$HOME/.config/zkm-fleet/admin.token"
fi

# ---------------------------------------------------------------- scheduler
if [ "$ROLE" = control ]; then
  [ -n "$PUBLIC_URL" ] || PUBLIC_URL="http://$IP:9191"
  if [ ! -f "$ETC/fleet.yaml" ]; then
    say "writing $ETC/fleet.yaml (edit tenants there)"
    {
      echo "stage_addr: 0.0.0.0:51000"
      echo "control_addr: 0.0.0.0:51001"
      echo "http_addr: 0.0.0.0:9191"
      echo "public_url: $PUBLIC_URL"
      echo "database_url_file: /etc/zkm-fleet/database_url"
      echo "data_dir: /var/lib/zkm-fleet"
      echo "require_signature: true"
      echo "cluster_token_file: /etc/zkm-fleet/cluster.token"
      echo "admin_token_file: /etc/zkm-fleet/admin.token"
      if [ "$TLS" = 1 ]; then
        echo "tls:"
        echo "  cert_path: /etc/zkm-fleet/tls/server.pem"
        echo "  key_path: /etc/zkm-fleet/tls/server.key"
        echo "  client_ca_path: /etc/zkm-fleet/tls/ca.pem"
      fi
      echo "tenants:"
      echo "  - name: ethproofs"
      echo "    class: latency"
      echo "    anchor: exclusive"
      echo "    backends: [cuda]"
      echo "  - name: goat"
      echo "    class: batch"
      echo "    anchor: shared"
    } > "$ETC/fleet.yaml"
  fi
  [ "$TLS" = 0 ] && [ "$BIND_STAGE" != 127.0.0.1 ] && die "--no-tls requires --bind-stage 127.0.0.1"
  mkdir -p "$PREFIX/data"
  cat > "$PREFIX/compose.env" <<EOF
FLEET_ETC=$ETC
FLEET_DATA=$PREFIX/data
FLEET_UID=$(id -u)
FLEET_GID=$(id -g)
STAGE_BIND=$BIND_STAGE
CONTROL_BIND=$IP
HTTP_BIND=$BIND_HTTP
EOF
  say "starting the scheduler and Postgres (docker compose)"
  docker compose --env-file "$PREFIX/compose.env" -p zkm-fleet -f "$PREFIX/deploy/docker-compose.yml" up -d
  for _ in $(seq 1 60); do curl -sf "http://127.0.0.1:9191/healthz" > /dev/null 2>&1 && break; sleep 2; done
  curl -sf "http://127.0.0.1:9191/healthz" > /dev/null || die "scheduler did not come up: docker compose -p zkm-fleet logs scheduler"
fi

# --------------------------------------------------------------------- Ray
cat > "$ETC/watchdog.env" <<EOF
FLEET_HOME=$PREFIX
FLEET_ETC=$ETC
FLEET_LOG=$WORKDIR/logs
FLEET_WORKDIR=$WORKDIR
RAY_NUM_CPUS=$CPUS
RAY_ROLE=$([ "$ROLE" = control ] && echo head || echo worker)
RAY_HEAD=$HEAD
RAY_NODE_IP=$IP
CUDA_VISIBLE_DEVICES=$CUDA
EOF
say "starting Ray ($( [ "$ROLE" = control ] && echo head || echo "worker of $HEAD"), cards: ${CUDA:-none}, cpus: ${CPUS:-all}, workspace $WORKDIR)"
FLEET_ENV="$ETC/watchdog.env" "$PREFIX/deploy/fleet-watchdog.sh" || true
pgrep -x raylet > /dev/null || die "Ray did not start on this machine: see $WORKDIR/logs/ray.log"
LINE="* * * * * FLEET_ENV=$ETC/watchdog.env $PREFIX/deploy/fleet-watchdog.sh >> $WORKDIR/logs/watchdog.log 2>&1"
if [ "$CRON" = 1 ] && ! crontab -l 2>/dev/null | grep -qF "$PREFIX/deploy/fleet-watchdog.sh"; then
  command -v crontab > /dev/null || die "crontab not found (install cron, or pass --no-cron and keep Ray up another way)"
  (crontab -l 2>/dev/null; echo "$LINE") | crontab -
  say "cron watchdog installed (keeps Ray up after crashes and reboots)"
fi

# ------------------------------------------------------------------ actors
if [ "$ROLE" = control ]; then
  if [ ! -f "$ETC/services.yaml" ]; then
    {
      echo "scheduler: http://$IP:51001"
      echo "cluster_token_file: $ETC/cluster.token"
      echo "node_binary: $PREFIX/bin/fleet-node"
      echo "log_dir: $WORKDIR/logs"
      echo "services: []            # add ziren-gpu / ziren-cpu services, then: $PREFIX/deploy/apply.sh"
    } > "$ETC/services.yaml"
  fi
  cat > "$PREFIX/deploy/apply.sh" <<EOF
#!/bin/bash
# Makes the running prover actors match $ETC/services.yaml.
cd "$PREFIX/bridge" && exec "$PREFIX/venv/bin/python" -m fleet_bridge apply --services "$ETC/services.yaml" --address 127.0.0.1:6379
EOF
  chmod +x "$PREFIX/deploy/apply.sh"
  "$PREFIX/deploy/apply.sh"
fi

say "done"
if [ "$ROLE" = control ]; then
  cat <<EOF

  status:      fleetctl status          (fleetctl is in $LINKDIR)
  provers:     edit $ETC/services.yaml, then $PREFIX/deploy/apply.sh
  a client:    fleetctl credential issue --tenant ethproofs --label poller --endpoint https://<host>:51000 \\
                 --tls-dir $ETC/tls --out ethproofs.env
  a new node:  copy $ETC/cluster.token to it, then
               deploy/install.sh node --head $IP --cluster-token cluster.token --cuda 0,1,2,3
EOF
fi
