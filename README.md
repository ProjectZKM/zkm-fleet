# zkm-fleet

[![CI](https://github.com/ProjectZKM/zkm-fleet/actions/workflows/ci.yml/badge.svg)](https://github.com/ProjectZKM/zkm-fleet/actions/workflows/ci.yml)

One scheduler for the GPU / CPU workloads on our machines. Ray owns the cards; three kinds of work share them:

- **Ziren proofs.** EthProofs and GOAT submit through the Ziren SDK's network prover (`ZKM_PROVER=network`, unchanged); the scheduler places each job on a prover node, and the nodes run as Ray actors next to their GPU servers.
- **One-shot runs** (training, batch jobs): a command in a container on the cards Ray grants it, queued until they are free, through the runs API (`fleetctl job submit`).
- **Long-running GPU apps** (ComfyUI, an inference server): a container per replica, kept up like the provers.

![zkm-fleet architecture: clients, control machine, and Ray workloads](images/diagram.svg)

The scheduler, its Postgres and the gateway run under docker compose. Every machine runs a Ray node; the service actors (provers, apps, the runs API) are detached, so Ray restarts them on failure, and `apply` reconciles them with `services.yaml` when it changes.

**Who gets a card.** Jobs inherit their app’s priority at submission (default `0`, higher first). A waiting run or starting service can preempt eligible lower-priority runs only within its own namespace. Stopped runs queue again after confirmed cleanup, spending no retry. Equal-priority jobs do not preempt each other; pending jobs with equal priority use FIFO order.

**Resources in the code.** Four main resources describe ownership, access and work:

| Resource | Purpose |
|---|---|
| **Namespace** | Administrator-created ownership and billing boundary. |
| **App** | Belongs to one namespace; type is `proof` or `run`. |
| **Job** | An execution of an app, belonging to its namespace. |
| **Credential** | Belongs to one namespace, with explicit app grants and an optional tag. |

Nodes are infrastructure; allocations are internal reservation records. Tenants configure proof scheduling, quotas configure namespace limits, and tags are optional attributes for isolating work and applying additional limits. There is no Group resource. See [namespaces, access and quotas](#namespaces-access-and-quotas) for details.

**Apps and jobs.** An *app* is what can run: a proof app is a tenant's program (named `tenant/<first 8 hex of the ELF id>`), a run app is a profile in `runs.yaml` (image, user, GPU cap, the roots a run may mount). A *job* is one instance of an app: a proof job, or a run. **Every app belongs to an administrator-created namespace. A job runs only for an existing app:** the scheduler refuses a proof whose program is not registered for its tenant, and the runs API refuses a run whose profile is not in `runs.yaml`. A proof app is registered with `fleetctl --namespace NAME app add tenant/<elf id>` (the tenant is in `fleet.yaml` and holds a credential; the ELF itself arrives with the first job); a run app with `fleetctl --namespace NAME app add APP --image … --allow …` (or by editing `runs.yaml`). `fleetctl app` lists the apps with their job counts, `fleetctl job` the jobs of both types. Either kind can be disabled (new jobs refused, history kept) and deleted.

Restarting or upgrading the scheduler does not fail the proofs in flight:
- **gateway:** the SDK gives up on the first broken call, so the gateway (Envoy) terminates mTLS and retries calls until the scheduler is back. A resubmitted `proof_id` is never queued twice.
- **jobs:** the scheduler keeps them in Postgres.
- **nodes:** a proof keeps running on its node; the restarted scheduler sends the job back to that node, which hands over the result.

## Namespaces, access and quotas

A namespace is a resource and the accounting and billing unit. A credential names
one namespace and may optionally have a tag. Credentials with the same tag share
proofs, runs and uploads; different tags isolate that work. Untagged credentials
share the namespace's untagged work. Refreshing preserves the namespace and optional
tag, including access to earlier work. Proof tenants still define scheduling policy, and app
types remain `proof` and `run`.

### Identity and permissions

An administrator creates a namespace with an immutable ID and unique name such
as `alice`. Names start with a lowercase letter, contain lowercase letters,
digits or hyphens, and have at most 63 characters; `system` and `admin` are
reserved. Renaming and deleting namespaces are not currently supported.

A credential belongs to a namespace, with an optional tag. Several credentials
can share a tag, for example teammates, laptop keys and automation keys.
Members can access that tag's existing jobs and uploads; another tag in the
same namespace cannot. App grants remain explicit on each credential, and
submission requires the relevant app grant. Only administrators create
namespaces, issue credentials or change access.

User requests infer their namespace from the verified credential. A
`--namespace` selector must match that namespace; it cannot authorize access to
another tag or namespace. Administrators can select `--namespace NAME --tag TAG`
when listing jobs or looking up a client's proof ID. Fleet-wide records include
namespace IDs and tags and retain the submitting credential for audit.

Revoking one credential blocks that key. Suspending a namespace blocks every
credential in it. Resuming a namespace does not restore individually revoked
keys. Neither action cancels accepted work; an administrator stops jobs explicitly.
Existing tenant/profile resource limits also apply. Each namespace has a shared
GPU/CPU quota, described below.

### App ownership

Every app belongs to an existing namespace. Namespace creation and app registration
are administrator operations; user credentials cannot create either. Register the
app before granting it:

```bash
fleetctl namespace create alice --gpus 2 --cpus 16
fleetctl --namespace alice app add training --image APPROVED_IMAGE --allow /srv/training
fleetctl --namespace alice app add eth/FULL_ELF_SHA256
```

Omitting the namespace during app creation is refused; an unknown namespace is not
automatically created. Proof registrations use `(namespace, tenant, ELF ID)`, so
namespaces can register the same program independently. Disabling or deleting one
registration leaves the others and their jobs intact. Run profile names remain
unique within the runs API's configuration; each profile has one namespace.
A grant from another namespace cannot authorize submission. Admin run submissions
also inherit the app's namespace and participate in its quota.

The proof API uses `POST /admin/apps?namespace=alice&tenant=eth&elf_id=FULL_ELF_SHA256`.
The run API accepts `POST /apps?namespace=alice` with the profile JSON, or a
`namespace` field in the JSON. Names and immutable namespace IDs are accepted;
registrations persist the ID. A configured run profile or persistent `kind: app`
entry can also specify `namespace: alice`; its namespace is verified before launch.
Tags remain optional credential and job labels; apps do not require a tag.

### GPU and CPU quotas

A namespace quota caps **concurrent resource units**, shared by proofs and runs
across every tag and credential in that namespace. An optional tag can also cap its
total concurrent GPU/CPU units. Admission must fit both caps; tag caps do not
reserve a guaranteed share of fleet capacity. Neither quota has a duration, expiry, hourly
budget or periodic reset. A job can keep its admitted units indefinitely; normal
job lifecycle rules and existing application timeouts still apply.

```bash
fleetctl namespace create alice --gpus 2 --cpus 16
fleetctl namespace quota alice --gpus 4 --cpus 32
fleetctl namespace get alice       # quota and currently reserved units
fleetctl namespace quota alice     # clear both caps: unlimited (a write operation)
```

Each `quota` command replaces both caps. An omitted cap is unlimited; zero
prohibits allocation of that resource. For example `--gpus 0 --cpus 8` permits
CPU-only runs. Fractional GPU units are supported and retain the existing
per-profile VRAM requirements for shared cards. New namespaces
start with unlimited caps unless the administrator sets them.

Runs requesting more than either cap receive HTTP 422 before creation. A run
first obtains physical Ray capacity, then claims namespace and tag units.
If capacity or either quota is occupied, it stays `waiting_quota`; its detail
names the constraint. Blocked placements are removed and no quota is held while
waiting for physical capacity, so an unschedulable run cannot block proofs from
using idle prover services. Dependencies also wait without occupying quota.

The private runs API retries admission every five seconds. A persisted launch
intent, stable placement-group name and atomic Ray actor lookup/creation recover
an interrupted launch without starting a second actor. Existing executions are
accounted for after an upgrade without relaunching them. Active runs have a durable
index; terminal history is checked once during recovery and cleanup, rather than
scanned and released on every poll or submission.

An admitted run reserves its requested units throughout execution, VRAM waits
and retries. Finishing or cancelling releases its reservation after actors stop and the actual container is
confirmed stopped or absent. An unreachable machine or failed Docker inspection
keeps the reservation and history; cleanup is retried when the machine returns. Preemption releases it and requires quota admission again before restarting. A lost run retains its
reservation until cleanup is confirmed so an orphaned container cannot silently free the
namespace's allowance. Cancelling an unreachable run does not release its quota.
Actor cleanup may take a few seconds after a terminal status appears.

Proof placement charges the selected prover node's advertised GPU and CPU units.
A proof stays queued until an eligible node fits both remaining quotas. A timed
out or disconnected proof keeps its reservation and rejoins its original prover;
it cannot fail over to another prover while the original execution is uncertain.
The node's entire allocation must fit: this feature does not resize a prover or
scale an active proof. Configure appropriately sized prover nodes for smaller
user quotas. Idle prover services and administrator-managed persistent apps are
fleet infrastructure; this quota governs user proof/run jobs.

Reservations are stored in Postgres and recovered after a scheduler restart;
refreshing credentials never changes them. If the private quota service is
unavailable, new runs remain waiting. Reducing a cap lets already-admitted work
finish, including work recovered during an upgrade. Further admissions wait
until usage fits; a waiting run whose request exceeds a newly lowered cap fails
with an explanation. Operators can inspect reservations, including each tag, through
`GET /admin/namespaces/alice/allocations`.

### App priority and preemption

Administrators set app policy; users cannot supply a job priority:

```bash
fleetctl --namespace alice app add training --image python:3 --allow /srv/training --priority 10
fleetctl --namespace alice app priority training 20
fleetctl --namespace alice app priority eth/FULL_ELF_ID 100
```

Proof and run jobs retain the priority inherited when submitted, including across
retries, preemption and restart. Changing the app affects new submissions. Higher
priority jobs are considered first; equal priority pending jobs use FIFO order
(creation time, then ID). Priority does not bypass quotas, app grants, dependencies,
placement constraints or tenant anchoring. A blocked job can be skipped so work
that fits can proceed.

Automatic preemption selects only lower-priority runs in the requesting namespace
whose profile permits preemption and whose minimum runtime has elapsed. It prefers
the lowest priority victims and plans enough physical capacity and namespace/tag
quota before stopping any run. Equal or higher priority runs, runs in another
namespace and active proofs are protected. Proof priority orders queued proofs;
it does not resize active proofs or evict running prover services. Persistent apps
are also protected from automatic eviction. Their `priority` in `services.yaml`
controls which one-shot runs they may displace when starting; changing that value
alone does not recreate actors. The services file still controls startup/card order.

Stopping uses SIGTERM followed by SIGKILL after `stop_timeout_s`. Placement and
quota reservations remain until every container is confirmed stopped or absent.
An unreachable worker keeps its reservations, and the durable requeue intent is
recovered after restart. Runs resume with their original ID, output and inherited
priority, without spending a retry. All priorities default to `0`, so automatic
preemption requires explicitly assigning different priorities.

Ray provides placement groups and actor resource reservations. Fleet enforces
namespace ownership, optional tag isolation, quotas and preemption policy; a Ray
namespace alone does not enforce these limits. Current actors use the shared Ray
control namespace, while Fleet namespace IDs scope policy and accounting.

### Optional tags

```bash
fleetctl namespace create team --gpus 8 --cpus 64
fleetctl namespace quota team --tag research --gpus 2 --cpus 16
fleetctl namespace quota team --tag production --gpus 6 --cpus 48
fleetctl namespace get team                  # namespace quota, usage and tags
fleetctl namespace get team --tag research   # namespace totals and tag quota/usage
fleetctl credential issue --namespace team --tag research --tenant eth \
  --app training --endpoint http://127.0.0.1:51000 --out alice.env
fleetctl --namespace team --tag research job
```

Both research and production contribute to the team's shared quota. If production
occupies six GPU units and research occupies two, all eight units are in use and
neither can admit another GPU job. Jobs can retain admitted units indefinitely. Lowering
either quota lets admitted work finish and blocks further admissions as needed.

Tags use the same character and length rules as namespace names and are scoped
to their namespace. Tags are attributes, with no creation or registration step. Assign a tag when
issuing a credential with `--tag`, or set its optional cap on the namespace. A newly
used tag has no extra cap unless the administrator sets one. Omitting `--tag`
issues an untagged credential; it still consumes the namespace quota.

Membership cannot change for an existing key. Refresh copies the original tag,
independently of CLI selectors. Client bundles use `FLEET_TAG`, empty when no tag is assigned.
Users cannot select another tag or spoof it in a submission body. Untagged
credentials store `tag: null`; proof jobs store `tag: ""`. There is no mandatory default tag.

### Administrator workflow

```bash
fleetctl namespace create alice
fleetctl namespace list
fleetctl namespace get alice
fleetctl --namespace alice app add training --image APPROVED_IMAGE --allow /srv/training
fleetctl credential issue --namespace alice --tenant eth --app training \
  --endpoint http://127.0.0.1:51000 --out alice.env
fleetctl job --namespace alice

# Copies the namespace, optional tag, tenant, app grants and label to a new key/token.
fleetctl credential refresh OLD_ADDRESS \
  --endpoint http://127.0.0.1:51000 --out alice-next.env
# After the user verifies the replacement:
fleetctl credential revoke OLD_ADDRESS

fleetctl namespace suspend alice
fleetctl namespace resume alice
```

The endpoint above is unused for run-only access. For proofs, use the forwarded
HTTPS gateway and supply `--tls-dir` and `--domain-name` as described in
[user access](docs/user-access.md). `refresh` also accepts the alias `rotate`. It does
not revoke the old key automatically or overwrite an existing bundle.

Issuance requires an existing, active namespace. A typo cannot silently create a
new owner. Credential namespace and optional tag cannot be changed after issuance.

Administrator HTTP endpoints, on the existing private scheduler listener:

| Method and path | Action |
|---|---|
| `POST /admin/namespaces` with `{"name":"alice"}` | Create |
| `GET /admin/namespaces` | List |
| `GET /admin/namespaces/alice` | Inspect quota, total usage and tags |
| `PUT /admin/namespaces/alice/quota` with `{"gpus":2,"cpus":16}` | Replace caps |
| `GET /admin/namespaces/alice/allocations` | Inspect reservations across tags |
| `GET /admin/namespaces/alice?tag=research` | Inspect namespace totals and tag caps/usage |
| `PUT /admin/namespaces/alice/quota?tag=research` with `{"gpus":2,"cpus":16}` | Replace tag caps, recording a new tag if needed |
| `PUT /admin/namespaces/alice/suspended` | Suspend |
| `DELETE /admin/namespaces/alice/suspended` | Resume |

Credential registration accepts a namespace name or ID and an optional `tag`.
Job/app lists and job reads accept `?namespace=alice&tag=research`; the runs API supports the same selectors. Refresh
is a CLI operation using administrator credential lookup and registration.
No additional listener or public port is required.

### Job fields and storage

The SDK signs and receives its original proof ID. The scheduler resolves
`(namespace_id, optional_tag, client_proof_id)` to a unique internal ID, derived from their
SHA-256 hash for new jobs. Nodes, blob keys, retries and deduplication use that
internal ID. Different tags can submit the same client proof ID independently.
Resubmission within a tag returns the existing job without replacing input.

Run IDs and directories retain their existing flat layout, with namespace
and tag ownership recorded in each run spec. Namespace names are never host paths.
New uploaded archives live under
`runs/.uploads/namespaces/<namespace-id>/tags/<tag>/<content-hash>.tar.gz` for
tagged credentials, or `runs/.uploads/namespaces/<namespace-id>/untagged/<content-hash>.tar.gz`
for untagged credentials. Dependencies,
logs, outputs and mutations check both namespace and tag membership. A verified replacement
credential can reuse uploads and read accepted runs from earlier keys.

Proof job identity is required and immutable:

| Field | Meaning |
|---|---|
| `id` | Internal execution and storage identifier. |
| `owner` | Authenticated submitting address, retained for audit after credential refresh. |
| `namespace` | Immutable namespace ID controlling resource accounting and access. |
| `tag` | Isolation label; `""` means untagged. |
| `client_id` | Original SDK proof ID, unique within namespace and tag. |
| `tenant`, `elf_id`, `kind`, `created_ms` | Submission metadata retained throughout retries. |

`node` and `started_ms` can be absent while queued. `finished_ms` is absent
until the job succeeds or fails. Counters start at zero and failure history starts
empty. Rejected authenticated proof requests retain their submitting identity;
requests without an authenticated owner are refused without creating a job.
Credential tags and run tags remain optional (`null` when absent).

Public-values URLs for namespaced proofs are bound to the issuing credential.
Revoking that key or suspending its namespace invalidates its URLs. A replacement
credential can poll the same job to obtain a fresh URL.

Postgres creates the current schema directly. Reset databases created with an
incompatible development schema before starting this version; there is no identity
backfill or old group-name/path conversion. Operator-managed proof apps use the
reserved `system` namespace. User credentials cannot be issued in `system`.

Configured run profiles and persistent app entries default to `system`.
Administrators assign a user app's namespace explicitly; API registrations require
an existing namespace. Namespace quotas govern proof and one-shot run jobs;
persistent apps retain their configured replica resource reservations.

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
fleetctl job | job ID | job submit --app A --gpus 1 -- CMD | job logs ID -f | job fetch ID --out F | job cancel ID | job preempt ID | job purge ID
fleetctl node | node NAME | node drain NAME | node undrain NAME
fleetctl namespace create NAME --gpus N --cpus N | namespace quota NAME --gpus N --cpus N | namespace list | namespace get NAME | namespace suspend NAME | namespace resume NAME
fleetctl credential | credential ADDRESS | credential issue --namespace NAME … | credential refresh ADDRESS … | credential revoke ADDRESS
```

Submit from your laptop with `fleetctl job submit` or `POST /runs` through an SSH
tunnel. See [local CLI and API submission](docs/submit-jobs.md) for connection,
code uploads, and output retrieval.

Admin-issued user access for proofs and runs, restricted to approved apps and
the credential's namespace and optional tag, is supported. Credential refresh preserves jobs and uploads.
Namespaces are the resource accounting and billing unit; namespace and optional tag quotas
cap concurrent GPU/CPU units across proofs and runs, with no quota time limit.
See [user access](docs/user-access.md) for onboarding and current limits, and
[namespaces, access and quotas](#namespaces-access-and-quotas) for administration.

The skills in [`.claude/skills/`](.claude/skills) cover user-facing interfaces and resources: [fleet-apps](.claude/skills/fleet-apps/SKILL.md) for app registration and lifecycle, [fleet-jobs](.claude/skills/fleet-jobs/SKILL.md) for CLI/API submission and job management, and [fleet-resources](.claude/skills/fleet-resources/SKILL.md) for namespaces, quotas, tags and credentials. Administrator-only actions are identified explicitly.

Persistent GPU apps such as ComfyUI appear as `run` apps in `fleetctl app`.
`fleetctl app disable NAME` stops a persistent app and releases its GPUs;
`fleetctl app enable NAME` starts it again. For proof apps and one-shot run
profiles, disabling only blocks new jobs; existing jobs finish.

GPU counts are currently configured in `services.yaml` for provers and persistent
apps, or with `fleetctl job submit --gpus N` for one-shot runs. Active proofs cannot
resize their GPU allocation. Live proof scaling is tracked in
[issue #3](https://github.com/ProjectZKM/zkm-fleet/issues/3).

## Data

| what | where |
|---|---|
| proof apps, proof jobs, credentials | Postgres (compose): `fleet_apps` (program registrations per namespace and tenant, with when each was disabled), `fleet_jobs` (state, kind and a failed job's stage status by name, with CHECK constraints), `fleet_credentials`, `fleet_namespaces` (namespace and optional tag quota settings), `fleet_namespace_allocations`, `fleet_schema` (the schema version) |
| ELFs, a job's input, proof and public values | the scheduler's data dir: `elf/<id>`, `jobs/<proof id>/` |
| runs | the runs dir (`install.sh --runs-dir`; shared storage when there are several machines): `<run id>/` holds `spec.json`, `status.json`, the log, `out/` and `code/`; `.uploads/` holds uploaded code tarballs |

## Credentials

| what | held by | issued by |
|---|---|---|
| user API token (`FLEET_TOKEN_FILE`) | the user | `fleetctl credential issue --namespace NAME --app …`: only its hash is registered; approved apps and namespace jobs; revocable |
| tenant signing key (`ZKM_PRIVATE_KEY`) | the client | `fleetctl credential issue`: generated on the operator's machine, only the address is registered, revocable |
| client certificate (`SSL_CERT_PATH` / `SSL_KEY_PATH`) | the client | the same command with `--tls-dir`; its CN is the credential's address. The SDK only enables TLS when it has a client certificate, so the stage API runs mutual TLS |
| stage server certificate + CA | gateway; clients get `CA_CERT_PATH` | `install.sh` (`fleetctl init tls`) |
| cluster token | scheduler and every node, both directions | `install.sh` (`fleetctl init token`) |
| admin token | operators | `install.sh` (`fleetctl init token`) |

Proof clients use their signing key and mTLS certificate; scoped credentials are
limited to approved programs and their own jobs. Operator credentials without app
grants have tenant-level SDK access. Revocation applies to both proofs and runs.
See [user access](docs/user-access.md) for issuance and permissions.

The scheduler and runs API stay private. Users reach them through restricted SSH
forwards and use `FLEET_TOKEN_FILE`; operators use `FLEET_ADMIN_TOKEN_FILE`.
The runs API listens on loopback by default, and this feature adds no public port.

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
