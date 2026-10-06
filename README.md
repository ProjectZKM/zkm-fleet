# zkm-fleet

One scheduler for the GPU / CPU workloads on our machines. Ray owns the cards; three kinds of work share them:

- **Ziren proofs.** EthProofs and GOAT submit through the Ziren SDK's network prover (`ZKM_PROVER=network`, unchanged); the scheduler places each job on a prover node, and the nodes run as Ray actors next to their GPU servers.
- **One-shot runs** (training, batch jobs): a command in a container on the cards Ray grants it, queued until they are free, through the runs API (`fleetctl job submit`).
- **Long-running GPU apps** (ComfyUI, an inference server): a container per replica, kept up like the provers.

Design and discussion: ProjectZKM/Projects#5.

```
SDK network prover ──mTLS──▶ gateway ──▶ fleet-scheduler ──cluster token──▶ fleet-ziren-node ──loopback──▶ GPU server container
  (EthProofs, GOAT)         :51000 stage                                     (Ray actor)              (ziren-gpu image)
                                         control :51001 ◀──register/heartbeat──┘
fleetctl ──admin token──▶   :9191 http (/admin, public values)
         ──admin token──▶   :9192 runs API (Ray actor) ──▶ run containers on the granted cards
```

The scheduler, its Postgres and the gateway run under docker compose. Every machine runs a Ray node; the service actors (provers, apps, the runs API) are detached, so Ray restarts them on failure, and `apply` reconciles them with `services.yaml` when it changes.

**Apps and jobs.** An *app* is what can run: a proof app is a tenant's program (named `tenant/<first 8 hex of the ELF id>`), a run app is a profile in `runs.yaml` (image, user, GPU cap, the roots a run may mount). A *job* is one instance of an app: a proof job, or a run. **A job runs only for an existing app:** the scheduler refuses a proof whose program is not registered for its tenant, and the runs API refuses a run whose profile is not in `runs.yaml`. A proof app is registered with `fleetctl app add tenant/<elf id>` (the tenant is in `fleet.yaml` and holds a credential; the ELF itself arrives with the first job); a run app with `fleetctl app add NAME --image … --allow …` (or by editing `runs.yaml`). `fleetctl app` lists the apps with their job counts, `fleetctl job` the jobs of both types. Either kind can be disabled (new jobs refused, history kept) and deleted.

Restarting or upgrading the scheduler does not fail the proofs in flight:
- **gateway:** the SDK gives up on the first broken call, so the gateway (Envoy) terminates mTLS and retries calls until the scheduler is back. A resubmitted `proof_id` is never queued twice.
- **jobs:** the scheduler keeps them in Postgres.
- **nodes:** a proof keeps running on its node; the restarted scheduler sends the job back to that node, which hands over the result.

## Install

Build once (`fleet-ziren-node` links the Ziren SDK: build it for the machines' OS, e.g. in an `ubuntu:22.04` container with Rust, `protoc` and Go 1.22):

```bash
cargo build --release --bins
```

Prerequisites on every machine: Docker (with `docker compose` on the control machine), Python ≥ 3.10 with `venv`; on GPU machines the NVIDIA driver ≥ 570 and `nvidia-container-toolkit`. The GPU server image (`ghcr.io/projectzkm/ziren-gpu`) is public.

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
fleetctl app                                          # apps: proof programs (tenant/ELF, the node each is anchored on) and run profiles (runs.yaml), with their jobs
fleetctl app add <tenant>/<elf id>                    # a proof app: that tenant's program (or <tenant>/ --elf FILE, hashed here)
fleetctl app add <name> --image I --allow /data ...   # a run app (a runs.yaml profile): --user, --max-gpus, --gpu-mem, --shm-gb, --env K=V
fleetctl app disable <name> | enable <name>           # no new jobs (the queued and running ones finish) / jobs again
fleetctl app delete <name> --yes                      # an app, once none of its jobs is queued or running
fleetctl job                                          # jobs, the instances of apps: every active one, then the newest -n of each type (--active, --type proof|run, --app A, --tenant T)
fleetctl job <id>                                     # one proof job or run
fleetctl job submit --app <run app> --gpus 1 -- CMD   # a run (see below); job logs / cancel / purge / fetch act on runs
fleetctl node                                         # prover nodes: cards, machine, ready, reserved for, busy with
```

| task | how |
|---|---|
| add / change provers | edit `/etc/zkm-fleet/services.yaml` (see `deploy/services.example.yaml`), then `/opt/zkm-fleet/deploy/apply.sh`; a changed spec (e.g. a new image digest) drains and replaces that actor; `python -m fleet_bridge apply --dry-run --services ...` only lists what an apply would start, stop or replace |
| service kinds | `ziren-gpu` / `ziren-cpu` (a prover node, with its GPU server), `app` (below), `runs-api` (below), `hold` (`gpus: N`, no image: keeps the next free cards from the services listed after it while they start, so on one machine the order of the file decides which cards each gets) |
| a GPU app (ComfyUI, an inference server) | a `kind: app` service: a container on one card, serving on `port_base` + replica index, its data mounted at the same paths; `python -m fleet_bridge render --services … --actor <name>-0 --gpus <card>` prints the `docker run` to try it by hand |
| a one-shot GPU job (training) | a profile in `/etc/zkm-fleet/runs.yaml` (see `deploy/runs.example.yaml`), then from any machine that reaches the runs API: `fleetctl job submit --app P --gpus 1 --cpus 16 --gpu-mem 20 --mount PATH:ro -- CMD` (the run app P is a runs.yaml profile) (or `/opt/zkm-fleet/deploy/run.sh submit ...` on the control machine). It queues until a card is free and has the memory, runs capped at its CPUs, writes to its own `{out}`, frees the card on exit. `--gpus 0.5` shares a card, `--nodes N` spreads it over N machines, `--after ID` / `--retry N` / `--restart-on-oom N` sequence and retry, `--code git:REPO@REF` or a `.tar.gz` under the app's allowed roots, or a local `.tar.gz` (fleetctl uploads it), pins the code. `fleetctl job` (the list), `job ID`, `job logs ID -f`, `job fetch ID --out F`, `job cancel ID`, `job purge ID`. States: `waiting_dependency`, `queued`, `waiting_vram`, `running`, `retrying`, then `succeeded` / `failed` / `cancelled`, or `lost` (its actor is gone; `cancel` removes its container) |
| apps | `fleetctl app` (the list). `app add TENANT/ELF_ID` registers a proof app (`TENANT/ --elf FILE` hashes a local ELF); `app add NAME --image I --allow ROOT …` writes a profile into `runs.yaml` (the file's other text, comments included, stays as it was; the result is re-read before it is written). `app disable NAME` makes the app refuse new jobs while the queued and running ones finish (a proof app: the scheduler answers InvalidParameter; a run app: `disabled: true` in its profile); `app enable NAME` undoes it. `app delete NAME` says what it would delete, `--yes` deletes it once none of its jobs is queued or running: a proof app goes with its job records, inputs and proofs, its anchor and its ELF (unless another tenant uses it); a run app's profile is cut out of `runs.yaml` and its past runs stay. The lower-risk order is disable, wait for the jobs to end, delete |
| the runs API | a `kind: runs-api` service (on the control machine, `127.0.0.1:9192` by default, the admin token); `fleetctl job` talks to it for runs (`--runs-url` / `FLEET_RUNS_URL`). From another machine: `ssh -N -L 9191:127.0.0.1:9191 -L 9192:127.0.0.1:9192 <control machine>` and the admin token in `~/.config/zkm-fleet/admin.token` |
| nodes | `fleetctl node` (the list), `node drain <name>` (finishes its job), later `undrain` |
| output | every `fleetctl` command prints a formatted view; `--json` prints the data as JSON |
| tenants | `/etc/zkm-fleet/fleet.yaml` (class, anchoring, allowed backends, `nodes` (name prefixes that may run it, for a tenant tied to one prover configuration), queue cap), then `docker compose -p zkm-fleet restart scheduler` |
| a client | `fleetctl credential issue --tenant ethproofs --label poller --endpoint https://<host>:51000 --tls-dir /etc/zkm-fleet/tls --out ethproofs.env`; the client runs `set -a; . ./ethproofs.env; set +a` and calls the SDK as before |
| revoke | `fleetctl credential revoke <address>` |
| logs | `docker compose -p zkm-fleet logs scheduler`; prover nodes: `<workdir>/logs/<actor>.node.log` (`/opt/zkm-fleet/work/logs` by default); GPU servers: `docker logs fleet-gpu-<actor>`; apps: `docker logs fleet-app-<actor>`; runs: `fleetctl job logs ID` |
| upgrade the scheduler | `git pull`, rebuild, `docker compose -p zkm-fleet up -d --build scheduler`; clients and running proofs ride through it. On start the scheduler brings Postgres to its schema version (one transaction; `fleet_schema` records it); an older scheduler refuses a newer schema, so a downgrade needs the database from before |

## Data

| what | where |
|---|---|
| proof apps, proof jobs, credentials | Postgres (compose): `fleet_apps` (a tenant's registered programs, with when each was disabled), `fleet_jobs` (state, kind and a failed job's stage status by name, with CHECK constraints), `fleet_credentials`, `fleet_schema` (the schema version) |
| ELFs, a job's input, proof and public values | the scheduler's data dir: `elf/<id>`, `jobs/<proof id>/` |
| runs | the runs dir (`install.sh --runs-dir`; shared storage when there are several machines): `<run id>/` holds `spec.json`, `status.json`, the log, `out/` and `code/`; `.uploads/` holds uploaded code tarballs |

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

What the SDK's scheme leaves open: the request signature covers `proof_id&seg_size` only, not the ELF or the input, so mutual TLS is what protects the payload; a proof id is single-use (a resubmission returns the existing job). The SDK sends no credential with `GetStatus` (hence the certificate above) or when it fetches the public values (hence the key in their URL); proof ids are random UUIDs.

The admin API (`/admin` on 9191) and the runs API take the admin token as a bearer token. 9191 is plain HTTP: bind it to loopback (`install.sh --bind-http 127.0.0.1`) unless the network between operators and the control machine is trusted, and reach it through an SSH tunnel (then clients must reach the public values another way). The runs API listens on loopback by default.

## Ports

| port | bind | who |
|---|---|---|
| 51000 stage (gRPC) | external, mTLS, on the gateway | SDK clients |
| 9191 http | external by default (`--bind-http`), on the gateway | public values (SDK, per-proof key), `/admin` (admin token), `/healthz` |
| 9192 runs API | 127.0.0.1 (the service's `listen`) | `fleetctl job` / `app` for runs (admin token) |
| 51001 control (gRPC) | cluster interface | nodes (cluster token) |
| 31000-31099 fleet-ziren-node | cluster | the scheduler (cluster token) |
| 30100-30199 GPU server | 127.0.0.1 | its own node only |
| app `port_base` + replica | where the app binds (ComfyUI: 127.0.0.1) | the app's own clients |
| Ray 6379, 6380, 6381, 8080, 52365, 10002-19999 | cluster | Ray |
| 172.28.250.0/24 | docker network on the control machine | scheduler (fixed at .10), Postgres, gateway; `install.sh --subnet` if it collides |
