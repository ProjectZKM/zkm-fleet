# Installing zkm-fleet on a new cluster

## Roles

```
control machine                         GPU machines (one or more)              clients
┌──────────────────────────────┐        ┌────────────────────────────────┐      ┌──────────────────┐
│ docker compose:              │        │ Ray worker (or the Ray head)   │      │ SDK network      │
│   fleet-scheduler + Postgres │◀──51001─┤ ZirenGpuProver actors:         │      │ prover, signed + │
│ Ray head + fleet bridge      │──31000─▶│   fleet-node ── GPU server     │      │ mTLS             │
└──────────────────────────────┘        │   (ziren-gpu image, by digest) │      └────────┬─────────┘
          ▲  51000 (stage, mTLS), 9191 (public values, /admin) ─────────────────────────┘
```

A single machine can hold every role (that is how the first GPU box runs).

## 1. Prerequisites

| machine | needs |
|---|---|
| all | Linux x86_64, Docker 24+, Python 3.10+, `git`; a `fleet` user in the `docker` group |
| GPU | NVIDIA driver ≥ 570 (CUDA 12.8), `nvidia-container-toolkit` (`docker run --runtime=nvidia` works), `/dev/shm` sized for Ray's object store plus the GPU server (≥ 256 GB on an 8-card machine) |
| GPU | read access to `ghcr.io/projectzkm/ziren-gpu` (private): a token with `read:packages`, then `docker login ghcr.io` as the `fleet` user |
| build host | Rust (the repository pins the toolchain), `protoc`, Go 1.22+ (the SDK's Groth16 wrapper is a Go library) |

## 2. Binaries

```bash
git clone https://github.com/ProjectZKM/zkm-fleet && cd zkm-fleet
```

`fleet-node` links the Ziren SDK and must be built against the GPU machines' glibc. Build it in a container of the machines' OS (Ubuntu 22.04 here) and copy it to `/opt/zkm-fleet/bin` on every GPU / CPU machine:

```bash
docker build -t fleet-build:2204 - <<'EOF'
FROM ubuntu:22.04
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      build-essential pkg-config libssl-dev protobuf-compiler clang git curl ca-certificates
RUN curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain none
RUN curl -sSL https://go.dev/dl/go1.22.5.linux-amd64.tar.gz | tar -C /usr/local -xz
ENV PATH=/root/.cargo/bin:/usr/local/go/bin:$PATH
EOF
docker run --rm -v "$PWD":/src -w /src fleet-build:2204 cargo build --release --locked --bins
sudo install -D -m 755 target/release/{fleet-node,fleetctl} -t /opt/zkm-fleet/bin
```

The scheduler is built by its compose file (step 4); `fleetctl` is also inside that image.

## 3. Secrets (control machine)

```bash
sudo install -d -m 750 -o fleet /etc/zkm-fleet
cd /etc/zkm-fleet
fleetctl token new --out cluster.token                 # scheduler <-> nodes, both directions
fleetctl token new --out admin.token                   # fleetctl / /admin
openssl rand -hex 24 > pg_password && chmod 600 pg_password
echo "postgres://fleet:$(cat pg_password)@postgres:5432/fleet" > database_url && chmod 600 database_url
fleetctl tls init --dir tls --names stage,fleet.example.com   # CA + stage server certificate
```

Copy `cluster.token` to every GPU / CPU machine (`/etc/zkm-fleet/cluster.token`, mode 600, owner `fleet`). Nothing else leaves the control machine.

## 4. Scheduler (docker compose)

```bash
sudo cp deploy/fleet.example.yaml /etc/zkm-fleet/fleet.yaml     # edit public_url and tenants
cd deploy
export FLEET_UID=$(id -u fleet) FLEET_GID=$(id -g fleet)       # the owner of /etc/zkm-fleet
CONTROL_BIND=10.0.0.1 docker compose up -d --build              # CONTROL_BIND = the cluster interface
curl -sf http://127.0.0.1:9191/healthz                          # ok
```

`restart: unless-stopped` brings it back after crashes and reboots. Jobs, credentials and proofs survive restarts (Postgres + the `fleet-data` volume). Operators use the `fleetctl` inside the container:

```bash
docker compose exec scheduler fleetctl status
```

## 5. Ray (every machine) and the bridge (control machine)

```bash
python3 -m venv /opt/zkm-fleet/venv
/opt/zkm-fleet/venv/bin/pip install ./bridge          # ray[default]==2.59.0, pyyaml
sudo cp -r deploy bridge /opt/zkm-fleet/
```

`/etc/zkm-fleet/ray.env` on each machine:

```bash
RAY_ROLE=head                      # worker on the other machines
RAY_HEAD=10.0.0.1                  # head address, for workers
CUDA_VISIBLE_DEVICES=0,1,2,3       # the cards Ray may hand out on this machine; leave others' cards out
RAY_OBJECT_STORE_BYTES=68719476736
```

```bash
sudo cp deploy/systemd/fleet-ray.service deploy/systemd/fleet-bridge.service /etc/systemd/system/
sudo systemctl enable --now fleet-ray                 # every machine
sudo cp deploy/services.example.yaml /etc/zkm-fleet/services.yaml   # edit; image by digest
sudo systemctl enable --now fleet-bridge              # control machine only
```

Without root, use `deploy/fleet-watchdog.sh` from cron instead (same settings in `/etc/zkm-fleet/watchdog.env`).

The bridge creates one placement group and one detached actor per service replica. A `ziren-gpu` actor starts the GPU server container on the cards Ray granted it (loopback port from `gpu_port_range`, its own `/dev/shm`), then `fleet-node`, which registers with the scheduler. Editing `services.yaml` (for example a new image digest) replaces the affected actors on the next pass.

```bash
fleetctl status        # every node "yes" under READY
```

## 6. Tenants and clients

Tenants (class, anchoring, allowed backends, queue cap) are declared in `fleet.yaml`. Issue each client its credential:

```bash
fleetctl credential issue --tenant ethproofs --label poller \
    --endpoint https://fleet.example.com:51000 --tls-dir /etc/zkm-fleet/tls --out ethproofs.env
fleetctl elf put guest.elf                          # once per program, if clients send only elf_id
```

Hand `ethproofs.env` (and the client certificate it points to) to the client over a secure channel. The client loads it and keeps calling the SDK unchanged:

```bash
set -a; . ./ethproofs.env; set +a                   # ZKM_PROVER=network, ENDPOINT, ZKM_PRIVATE_KEY, TLS paths
```

Revoke with `fleetctl credential revoke <address>`.

## 7. Firewall

| port | from |
|---|---|
| 51000 stage, 9191 http | clients |
| 51001 control | cluster machines |
| 31000-31099 fleet-node | the control machine |
| Ray 6379, 6380, 6381, 10001, 8080, 52365, 10002-19999 | cluster machines |
| 8265 Ray dashboard | operators only (VPN): it can submit jobs |
| 30100-30199 GPU servers | nobody (loopback) |

## 8. Verify

```bash
fleetctl status
# a real proof through the SDK, from any client machine:
cargo run --release -p fleet-node --example sdk_submit -- <guest.elf> 20 compressed
fleetctl job <proof_id>
```

## 9. Operations

| task | how |
|---|---|
| new GPU server version | push the image, put the new digest in `services.yaml`; the bridge replaces the actors |
| take a node out | `fleetctl node drain <name>` (finishes its job), later `undrain` |
| scheduler upgrade | `git pull && docker compose up -d --build` |
| logs | `docker compose logs scheduler`; node logs in the services file's `log_dir`; GPU servers: `docker logs fleet-gpu-<name>` |
| remove a machine | drain its nodes, `ray stop` on it, remove its replicas from `services.yaml` |
