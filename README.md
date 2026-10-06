# zkm-fleet

One scheduler for the GPU / CPU workloads on our machines. Phase 1 runs Ziren proofs: EthProofs and GOAT submit through the Ziren SDK's network prover (`ZKM_PROVER=network`, unchanged), the scheduler places each job on a prover node, and the nodes run as Ray actors next to the GPU servers. Design and discussion: ProjectZKM/Projects#5.

```
SDK network prover ──mTLS──▶ gateway ──▶ fleet-scheduler ──cluster token──▶ fleet-ziren-node ──loopback──▶ GPU server container
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

Build once (`fleet-ziren-node` links the Ziren SDK: build it for the machines' OS, e.g. in an `ubuntu:22.04` container with Rust, `protoc` and Go 1.22):

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
fleetctl status                                       # queue, nodes, anchors, runs going on
fleetctl ps                                           # proof jobs and runs, newest first (--active, --type proof|run, -n N)
fleetctl show <id>                                    # one proof job or run
```

| task | how |
|---|---|
| add / change provers | edit `/etc/zkm-fleet/services.yaml` (see `deploy/services.example.yaml`), then `/opt/zkm-fleet/deploy/apply.sh`; a changed spec (e.g. a new image digest) drains and replaces that actor; `python -m fleet_bridge apply --dry-run --services ...` only lists what an apply would start, stop or replace |
| a GPU app (ComfyUI, an inference server) | a `kind: app` service: a container on one card, serving on `port_base` + replica index, its data mounted at the same paths; `python -m fleet_bridge render --services … --actor <name>-0 --gpus <card>` prints the `docker run` to try it by hand |
| a one-shot GPU job (training) | a profile in `/etc/zkm-fleet/runs.yaml` (see `deploy/runs.example.yaml`), then from any machine that reaches the runs API: `fleetctl run submit --profile P --gpus 1 --cpus 16 --gpu-mem 20 --mount PATH:ro -- CMD` (or `/opt/zkm-fleet/deploy/run.sh submit ...` on the control machine). It queues until a card is free and has the memory, runs capped at its CPUs, writes to its own `{out}`, frees the card on exit. `--gpus 0.5` shares a card, `--nodes N` spreads it over N machines, `--after ID` / `--retry N` / `--restart-on-oom N` sequence and retry, `--code git:REPO@REF` (on the server) or a local `.tar.gz` (uploaded) pins the code. `fleetctl run list`, `status ID`, `logs ID -f`, `fetch ID --out F`, `cancel ID`, `purge ID` |
| the runs API | a `kind: runs-api` service (on the control machine, `127.0.0.1:9192` by default, the admin token); `fleetctl run` talks to it (`--runs-url` / `FLEET_RUNS_URL`). From another machine: `ssh -N -L 9191:127.0.0.1:9191 -L 9192:127.0.0.1:9192 <control machine>` and the admin token in `~/.config/zkm-fleet/admin.token` |
| jobs | `fleetctl job` (the newest; `--tenant T`, `-n N`), `fleetctl job <proof_id>` (one record) |
| nodes | `fleetctl node` (the list), `node drain <name>` (finishes its job), later `undrain` |
| output | every `fleetctl` command prints a formatted view; `--json` prints the data as JSON |
| tenants | `/etc/zkm-fleet/fleet.yaml` (class, anchoring, allowed backends, `nodes` (name prefixes that may run it, for a tenant tied to one prover configuration), queue cap), then `docker compose -p zkm-fleet restart scheduler` |
| a client | `fleetctl credential issue --tenant ethproofs --label poller --endpoint https://<host>:51000 --tls-dir /etc/zkm-fleet/tls --out ethproofs.env`; the client runs `set -a; . ./ethproofs.env; set +a` and calls the SDK as before |
| revoke | `fleetctl credential revoke <address>` |
| logs | `docker compose -p zkm-fleet logs scheduler`; node logs in `/opt/zkm-fleet/logs`; GPU servers: `docker logs fleet-gpu-<name>` |
| upgrade the scheduler | `git pull`, rebuild, `docker compose -p zkm-fleet up -d --build scheduler`; clients and running proofs ride through it |

## Credentials

| what | held by | issued by |
|---|---|---|
| tenant signing key (`ZKM_PRIVATE_KEY`) | the client | `fleetctl credential issue`: generated on the operator's machine, only the address is registered, revocable |
| client certificate (`SSL_CERT_PATH` / `SSL_KEY_PATH`) | the client | the same command with `--tls-dir`; its CN is the credential's address. The SDK only enables TLS when it has a client certificate, so the stage API runs mutual TLS |
| stage server certificate + CA | gateway; clients get `CA_CERT_PATH` | `install.sh` (`fleetctl init tls`) |
| cluster token | scheduler and every node, both directions | `install.sh` (`fleetctl init token`) |
| admin token | operators | `install.sh` (`fleetctl init token`) |

A proof request is the tenant's by its signature (`ZKM_PRIVATE_KEY`; the SDK signs every
`GenerateProof`). The SDK's `GetStatus` carries no signature, so with mutual TLS the
caller is known by its certificate instead: the gateway forwards the verified subject,
and with `client_cert_identity: true` (set by `install.sh` with TLS) the scheduler
answers `GetStatus` only for the tenant that submitted the proof, refuses a request
whose certificate and signature name different tenants, and a revoked credential's
certificate with it. The public values URL `GetStatus` returns carries a per-proof key.
Without TLS (a stage port bound to `127.0.0.1`), a proof id is the only thing that
opens its result.

What the SDK's scheme leaves open: the request signature covers `proof_id&seg_size` only, not the ELF or the input, so mutual TLS is what protects the payload; a proof id is single-use (a resubmission returns the existing job). `GetStatus` and the public-values URL carry no credential, as the SDK sends none there; proof ids are random UUIDs.

## Ports

| port | bind | who |
|---|---|---|
| 51000 stage (gRPC) | external, mTLS, on the gateway | SDK clients |
| 9191 http | external, on the gateway | public values (SDK), `/admin` (admin token), `/healthz` |
| 51001 control (gRPC) | cluster interface | nodes (cluster token) |
| 31000-31099 fleet-ziren-node | cluster | the scheduler (cluster token) |
| 30100-30199 GPU server | 127.0.0.1 | its own node only |
| app `port_base` + replica | where the app binds (ComfyUI: 127.0.0.1) | the app's own clients |
| Ray 6379, 6380, 6381, 8080, 52365, 10002-19999 | cluster | Ray |
| 172.28.250.0/24 | docker network on the control machine | scheduler (fixed at .10), Postgres, gateway; `install.sh --subnet` if it collides |
