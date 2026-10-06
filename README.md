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

`install.sh` puts `fleetctl` on the PATH; on the control machine it finds the admin token by itself (elsewhere set `FLEET_URL`, `FLEET_RUNS_URL` and `FLEET_ADMIN_TOKEN_FILE`, through an SSH tunnel to 9191 and 9192). Every command prints a formatted view; `--json` prints the data.

```bash
fleetctl app | app NAME | app add … | app disable NAME | app enable NAME | app delete NAME --yes
fleetctl job | job ID | job submit --app A --gpus 1 -- CMD | job logs ID -f | job fetch ID --out F | job cancel ID | job purge ID
fleetctl node | node NAME | node drain NAME | node undrain NAME
fleetctl credential | credential ADDRESS | credential issue … | credential revoke ADDRESS
```

The how-tos are the skills in [`.claude/skills/`](.claude/skills): [fleet-apps](.claude/skills/fleet-apps/SKILL.md) (apps: a job runs only for an existing one; registering a new program before a client uses it), [fleet-jobs](.claude/skills/fleet-jobs/SKILL.md) (runs: submit, watch, fetch, clean up), [fleet-operate](.claude/skills/fleet-operate/SKILL.md) (services, nodes, adding a machine, tenants, credentials, logs, upgrades). Claude Code loads them when working in this repository; they read as documentation too.

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
