# zkm-fleet

One scheduler for the GPU / CPU workloads on our machines. Phase 1 runs Ziren proofs: EthProofs and GOAT submit through the Ziren SDK's network prover (`ZKM_PROVER=network`, unchanged), the scheduler places each job on a prover node, and the nodes run as Ray actors next to the GPU servers. Design and discussion: ProjectZKM/Projects#5.

```
SDK network prover ──mTLS──▶ gateway ──▶ fleet-scheduler ──cluster token──▶ fleet-node ──loopback──▶ GPU server container
  (EthProofs, GOAT)         :51000 stage                                     (Ray actor)              (ziren-gpu image)
                                         control :51001 ◀──register/heartbeat──┘
fleetctl ──admin token──▶   :9191 http (/admin, public values)
```

The scheduler, its Postgres and the gateway run under docker compose. Every machine runs a Ray node; the prover actors are detached, so Ray restarts them on failure, and `apply` reconciles them with `services.yaml` when it changes.

Restarting or upgrading the scheduler does not fail the proofs in flight:
- **gateway:** the SDK gives up on the first broken call, so the gateway (Envoy) terminates mTLS and retries calls until the scheduler is back. A resubmitted `proof_id` is never queued twice.
- **jobs:** the scheduler keeps them in Postgres.
- **nodes:** a proof keeps running on its node; the restarted scheduler sends the job back to that node, which hands over the result.

## Install

Build once (`fleet-node` links the Ziren SDK: build it for the machines' OS, e.g. in an `ubuntu:22.04` container with Rust, `protoc` and Go 1.22):

```bash
cargo build --release --bins
```

Prerequisites on every machine: Docker (with `docker compose` on the control machine), Python ≥ 3.10 with `venv`; on GPU machines the NVIDIA driver ≥ 570 and `nvidia-container-toolkit`, and `docker login ghcr.io` with a `read:packages` token for the private GPU image.

Then one command per machine:

```bash
# control machine: secrets, CA, scheduler + Postgres (compose), Ray head, prover actors
deploy/install.sh control --cuda 0,1,2,3

# every other machine (copy the control machine's cluster.token first)
deploy/install.sh node --head 10.0.0.1 --cluster-token cluster.token --cuda 0,1,2,3
```

`--cuda` lists the cards Ray may hand out on that machine; leave out cards other systems use. Without root, add `--prefix ~/zkm-fleet --etc ~/zkm-fleet/etc`. The installer is idempotent: it keeps existing secrets and configs, installs a cron watchdog that keeps Ray up, and prints the next steps.

## Operate

`install.sh` puts `fleetctl` on the PATH; on the control machine it finds the admin token by itself (elsewhere set `FLEET_URL` and `FLEET_ADMIN_TOKEN_FILE`).

```bash
fleetctl status                                       # queue, nodes, anchors
```

| task | how |
|---|---|
| add / change provers | edit `/etc/zkm-fleet/services.yaml` (see `deploy/services.example.yaml`), then `/opt/zkm-fleet/deploy/apply.sh`; a changed spec (e.g. a new image digest) replaces that actor |
| tenants | `/etc/zkm-fleet/fleet.yaml` (class, anchoring, allowed backends, queue cap), then `docker compose -p zkm-fleet restart scheduler` |
| a client | `fleetctl credential issue --tenant ethproofs --label poller --endpoint https://<host>:51000 --tls-dir /etc/zkm-fleet/tls --out ethproofs.env`; the client runs `set -a; . ./ethproofs.env; set +a` and calls the SDK as before |
| revoke | `fleetctl credential revoke <address>` |
| a program sent by `elf_id` only | `fleetctl elf put guest.elf` once |
| take a node out | `fleetctl node drain <name>` (finishes its job), later `undrain` |
| one job | `fleetctl job <proof_id>` |
| logs | `docker compose -p zkm-fleet logs scheduler`; node logs in `/opt/zkm-fleet/logs`; GPU servers: `docker logs fleet-gpu-<name>` |
| upgrade the scheduler | `git pull`, rebuild, `docker compose -p zkm-fleet up -d --build scheduler`; clients and running proofs ride through it |

## Credentials

| what | held by | issued by |
|---|---|---|
| tenant signing key (`ZKM_PRIVATE_KEY`) | the client | `fleetctl credential issue`: generated on the operator's machine, only the address is registered, revocable |
| client certificate (`SSL_CERT_PATH` / `SSL_KEY_PATH`) | the client | the same command with `--tls-dir`. The SDK only enables TLS when it has a client certificate, so the stage API runs mutual TLS |
| stage server certificate + CA | gateway; clients get `CA_CERT_PATH` | `install.sh` (`fleetctl tls init`) |
| cluster token | scheduler and every node, both directions | `install.sh` (`fleetctl token new`) |
| admin token | operators | `install.sh` (`fleetctl token new`) |

What the SDK's scheme leaves open: the request signature covers `proof_id&seg_size` only, not the ELF or the input, so mutual TLS is what protects the payload; a proof id is single-use (a resubmission returns the existing job). `GetStatus` and the public-values URL carry no credential, as the SDK sends none there; proof ids are random UUIDs.

## Ports

| port | bind | who |
|---|---|---|
| 51000 stage (gRPC) | external, mTLS, on the gateway | SDK clients |
| 9191 http | external, on the gateway | public values (SDK), `/admin` (admin token), `/healthz` |
| 51001 control (gRPC) | cluster interface | nodes (cluster token) |
| 31000-31099 fleet-node | cluster | the scheduler (cluster token) |
| 30100-30199 GPU server | 127.0.0.1 | its own node only |
| Ray 6379, 6380, 6381, 8080, 52365, 10002-19999 | cluster | Ray |
| 172.28.250.0/24 | docker network on the control machine | scheduler (fixed at .10), Postgres, gateway; `install.sh --subnet` if it collides |
