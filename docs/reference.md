# Fleet reference

See [installation](installation.md), [operations](operations.md) and
[node maintenance](maintenance.md) for practical instructions.

## Code names

Fleet follows Kubernetes workload terminology while keeping its own small API:
a `Job` finishes a command, an `AppDeployment` holds the desired state of one app
entry on one node, and `WorkloadController` reconciles that state with node reports.
`JobSpec` describes execution; `JobStatus` records what happened. `ProofJob`
is the proof-specific record. Shared records and requests are generated from
[model.proto](../crates/fleet-proto/proto/fleet/model/v1/model.proto).

| Name | Responsibility |
| --- | --- |
| `Scheduler` | Places proofs on registered provers |
| `WorkloadController` | Admits and reconciles apps and batch jobs |
| `NodeExecutor` | Applies authorized commands through local Ray |
| `ProofFetcher` | Coordinates bounded proof downloads from storage nodes |
| `FleetClient` / `FleetctlClient` | HTTP client / MCP's CLI subprocess client |

These names follow Kubernetes' [workload](https://kubernetes.io/docs/concepts/workloads/controllers/)
and [controller](https://kubernetes.io/docs/concepts/architecture/controller/) concepts.
Fleet keeps `fleetctl app` and `fleetctl batch`; it needs no Kubernetes runtime.
Node execution configuration is `apps.yaml`; each entry runs an app's replicas
on that node. Batches use `batches.yaml`, and local job records use
`FLEET_JOBS_DIR`. This release replaces the previous execution names and paths
without compatibility aliases.

## Execution and proof protocols

Each execution machine runs an **independent local Ray instance** and `fleet-node`.
It initiates authenticated HTTPS gRPC heartbeats to the control plane. Responses
carry typed launch, stop and app commands; later heartbeats acknowledge commands
and report capacity and job state. Proof actors use their existing outbound proof
heartbeat. The scheduler makes admission, quota, priority and preemption decisions;
a small Python adapter performs local Ray/container operations. The control host
requires no Ray installation. Adding a remote machine requires the control-plane URL
and cluster token; machines do not join a shared Ray head or use SSH channels.

SDK-driven apps preserve the `GenerateProof`/`GetStatus` protocol:

1. The client submits to its **app-owned endpoint**, which stores the inputs and
   sends Fleet only references, SHA-256 hashes and job parameters.
2. Fleet admits and assigns the job through **node-initiated unary heartbeats**.
   The assigned proof actor reads inputs directly from app storage and proves them
   using its dedicated GPU server.
3. The node commits outputs to app storage and reports references, hashes, cycles
   and `prove_ms`. Fleet records the storage machine from the authenticated node.
4. The client polls its app endpoint, which validates completion references and
   reads the local proof and public values directly. Remote CLI/MCP fetches use
   an authorized ticket and the storage node's outbound HTTPS upload through Fleet.
   Fleet does not persist reference-job proof bytes.

There is no bidirectional RPC stream or control-plane connection to an inbound
node API. Integrated EthProofs owns the entire block pipeline: head polling,
witness preparation, loopback GPU proving, durable outputs and API reporting.
It needs no separate EthProofs client or SDK proof endpoint. Its saved cursor
advances one block at a time after output and report records are committed.

Each proof replica owns one GPU server and runs one proof at a time. Fleet places
jobs across eligible replicas; a single proof does not span servers. Every eligible
replica and the app endpoint must access the same app storage path. The initial
implementation uses filesystem references (`file://`), including owner-managed
shared mounts for replicas on different machines. Fleet retains admission, quotas,
placement, retries and execution leases.

Batch records and app policy are durable on the control host. Nodes retain their local
execution records and sync logs separately over HTTPS. App owners prepare batch
inputs and receive outputs in their storage; the batch command reads and writes there directly.
Lost connections retain
reservations; replay uses the same execution lease and confirmed cleanup releases
resources. Current batch jobs execute on one machine (`--nodes 1`); cross-machine batch jobs
are refused until separate coordination is implemented.

Persistent app ports remain private application endpoints. Fleet controls their
lifecycle and resource reservations; an authenticated application proxy is required
for external access to an app's own protocol.

**Priority and preemption.** Jobs inherit their app’s priority at submission (default `0`, higher first). A waiting batch job or starting app can preempt eligible lower-priority batch jobs only within its own namespace. Stopped batch jobs queue again after confirmed cleanup, spending no retry. Equal-priority jobs do not preempt each other; pending jobs with equal priority use FIFO order.

The Python adapter performs local placement, container startup, retries and cleanup
for commands authorized by Rust. It holds the cluster token for proof heartbeats;
it has no administrator API client, standalone submission CLI or policy editor.
Rust resolves dependencies, enforces quotas, selects preemption victims and drains
apps before sending node commands. Prepare code, datasets and models in the
workload image or owner-managed storage before submission.

The implementation keeps these boundaries:

| Component | Responsibility | Deployment dependencies |
|---|---|---|
| `fleet-scheduler` | Admission, namespace policy, quotas, placement and durable control records | Postgres; no Ray or prover SDK |
| `fleet-node` | Outbound machine heartbeats, command delivery and log synchronization | Local Python adapter; no database, signer recovery or prover SDK |
| `bridge/fleet_bridge` | Local resource grants, actors, containers and confirmed cleanup | Ray and PyYAML |
| `fleet-proof-app` | Optional SDK protocol adapter beside app-owned storage | Fleet HTTPS API; no database or prover SDK |
| `fleet-ziren-node` | Optional SDK-driven proof execution and retained artifact fetching | Ziren runtime; installed only for workloads that use it |
| App worker | Application preparation, proving calls, outputs, checkpoints and reporting | Dependencies packaged by its app owner |
| `fleetctl` / `fleet-mcp` | Clients of the same authenticated Fleet API | MCP is optional and invokes `fleetctl` |

Protocol types, artifact validation and SDK step mapping are shared in
`fleet-proto`. Fleet health checks process and container availability; it does
not interpret an app's checkpoint format. Inspect application progress and
failures through its logs. Apps and batches share ownership and container
configuration, while retaining their distinct persistent and finite lifecycles.

Shared data records and requests are defined in
[`model.proto`](../crates/fleet-proto/proto/fleet/model/v1/model.proto), which generates
the Rust types for jobs, apps, namespaces, tokens, resources, scaling and node reports.
The scheduler and machine agent exchange a typed protobuf report. App-owned settings
and execution details use lossless JSON extension fields; the local Python adapter
needs no protobuf package. Rust owns runtime handles and scheduling behavior.
The report (`NodeReport`) carries the node's `apps.yaml` entries, each with its observed
state, in `apps` (field 9). Machine report version 3 requires updating the scheduler,
agent and adapter together; version 2 named that field `services`.

Further simplification should move batch and app command persistence behind
the same store interface as proof records, and separate retained artifact fetching
from the native prover package. Both require recovery and compatibility checks;
the current storage formats and GOAT execution path are preserved.

**Resources in the code.** Namespaces, workloads, execution records and tokens describe ownership, access and work:

| Resource | Purpose |
|---|---|
| **Namespace** | Administrator-created ownership and billing boundary. |
| **App** | Long-running container in one namespace; a proof app's container is a Ziren prover of its named program. |
| **Batch** | Reusable finite workload configuration in one namespace; each run is a batch job. |
| **Job** | Execution record for a proof request or batch command, with namespace and tag ownership. |
| **Token** | Belongs to one namespace, with permissions, an optional tag and app grants; an administrator's token has none. |

Nodes are infrastructure; allocations are internal reservation records. A proof app's `proof` settings set proof scheduling, quotas configure namespace limits, and tags are optional attributes for isolating work and applying additional limits. There is no Group resource. See [namespaces, access and quotas](#namespaces-access-and-quotas) for details.

**Apps, batches and jobs.** `fleetctl app` lists all long-running workloads together. `fleetctl --json app` returns one array of app records, each with `type: app`. Inspect and manage an app by name, without a kind selector. An app is a container; a proof app is an app whose container is a Ziren prover of a named program. Create either with `fleetctl --namespace NS app add NAME --spec FILE.json`, which posts a `RegisterAppRequest` (`model.proto`) to `POST /apps`: `name`, optional `namespace` and `priority` (default 0), the file as `spec`, and a prover's `proof`. The spec holds the keys of the app's `apps.yaml` entry: `kind` (`app` by default, `ziren-gpu` by default for a prover, or `ziren-cpu`), `image` (required), `replicas`, `gpus`, `cpus`, `shm_gb`, `memory_gb`, `env`, `volumes`, `ready_timeout_s` and `stop_timeout_s`; a `kind: app` container also `command`, `port_base` (required; replica i serves on `port_base` + i), `health_path`, `workdir` and `user`; a prover also `proof_storage_root`, `backend` and `rust_log`. Unknown or mistyped settings are refused. A prover also takes its program, `--elf-id ID` or `--elf FILE`, and its placement policy, sent as `proof` (`elf_id`, `class`, `anchor`, `backends`, `zkm_version`, `max_queued`): `--class latency` places its jobs on the biggest free prover first, `batch` (the default) on the smallest; `--anchor` is `none` (default), `shared` or `exclusive`; repeat `--backend` (`cuda`, `cpu`, `mock`; default any); `--zkm-version` and `--max-queued` (default 64) are optional. Only a prover takes a program, and proof policy without one is refused. A proof runs only on a prover bound to its app.

An app is created disabled and on no node; the first `app enable` places it, and users never choose the node. Candidates are live, ready nodes that are not draining and whose total capacity holds the GPUs and CPUs of all its replicas. The scheduler prefers a node where they are free now and, among those, the one with the fewest free GPUs; otherwise the one with the most free GPUs, where the app waits for room. With no node big enough, enabling answers 503. The scheduler sends that node a `create` (`NODE_OPERATION_APP`), which writes the entry into its `apps.yaml` disabled, waits up to 120 seconds for the node to report it, then enables it and admits it against the namespace's quota; the control plane then manages enable/disable, priority and scaling. A failed create clears the placement, so the next enable places the app again; if the node has not reported the entry within the wait, enabling answers 503, and enabling again waits for the same create. An app that a node's `apps.yaml` already runs, such as an integrated worker's prover bound with `app: NAME`, is not placed again. An app the scheduler placed that waits for room on its node moves to a node that has the room now (the tightest fit, as above): that node gets a `create` with the entry as the old node has it, disabled, its `machine` set to the new node's. Once the new node reports the entry, the old node removes its own (its configuration and what is left of its containers and processes, never the app's data) and the app starts on the new node. A node whose create fails is not asked again for that app, which keeps waiting where it is. The scheduler copies nothing of what an app keeps on a node's disks: an app that moves takes care of its own storage. A namespace token with manage permission creates the namespace's apps; `user`, `volumes` and `proof_storage_root` (the host's user ids and filesystem) require administrator credentials, as `user` and `allow` do for batches. App names are unique across the fleet and shared with batches (409 when taken): a node's entry named like a batch is ignored.

App and job tables both show `TAG` and `STATE`. An app's state describes its replicas:
`configured`, `starting`, `running`, `draining`, `disabled`, `offline` or `failed`;
a proof app without provers is `registered`.
The app table shows total configured GPUs and CPUs without a replica column;
replica settings and job counts remain in app details. An app with node entries using
multiple tags shows their tags together. Batches show `enabled` or
`disabled`; their execution states appear in the job table.

`fleetctl batch` lists the batches, as `fleetctl app` lists apps, with each batch's active jobs and last job; `fleetctl batch NAME` shows one batch and its newest jobs, and `fleetctl job --type batch` lists the finite executions themselves. Register a reusable image and resource policy with `fleetctl --namespace NAMESPACE batch add NAME --image IMAGE`, and submit commands with `fleetctl batch submit NAME --gpus N --cpus N -- COMMAND`. Job actions take the job ID: `fleetctl batch logs`, `cancel`, `preempt` and `purge` find its batch, and `--batch NAME` skips that lookup. Batches can also be initially advertised from node `batches.yaml`. A batch may run for hours or days; its finite command lifecycle distinguishes it from an app.

Apps and batch workloads share the scheduler’s configuration and lifecycle model: name, namespace, image, user, environment, mount policy, priority, and enable/disable state. A batch is that reusable configuration; each submission creates a batch job. Apps keep replicas running; batch workloads execute a command per submission. `batch submit --name` continues to name an individual job.

Every app and batch belongs to an administrator-created namespace. Proof requests require a registered proof app, which the scheduler finds by the job's namespace and ELF ID; batch submissions require an existing batch. Jobs inherit their workload's namespace, tag and priority. `fleetctl job` remains a combined view of proof and batch execution records, including proof output fetching; `--app NAME` selects one app's or batch's jobs and `--type app|batch` one kind.

App listings contain registered workloads. Rejected proof requests remain job
records and do not create an app entry.

Restarting the scheduler preserves proof jobs, namespaces and quota reservations
in Postgres. Batch records, app deployment records and commands are durable files in
the control host's `data_dir`; back up that directory together with Postgres.
Nodes continue locally, reconnect through heartbeats, and retry
completion metadata until acknowledged. App endpoints and the machine storage handler remain independent of
prover replicas. Disabling a registered app releases its GPU server while retained
proofs remain fetchable through the machine agent.
App owners control retention of their input and output files.

`fleetctl job fetch JOB_ID --out DIR` downloads proof JSON and public values (the
destination must not exist). Fleet authorizes the request,
delivers a short-lived fetch ticket in the machine's next ordinary heartbeat,
and relays the node's HTTPS response in bounded memory. `fleet-ziren-node fetch-artifact`
verifies committed files against the job, execution lease and
hashes; it initializes no prover or GPU. The control host saves no proof files.
Nodes need only outbound connections. Fetching waits for a heartbeat; an offline
storage machine returns a retryable error.
Batch logs remain available through Fleet. Each node retains its batch workspace in
its own `FLEET_JOBS_DIR`; no shared mount is required for one-machine batch jobs.

### App-owned proof endpoint

Run an endpoint beside the app's storage, independently of its GPU replicas:

```bash
fleet-proof-app --scheduler https://scheduler.example.com:8905 \
  --namespace NAMESPACE_ID --elf-id FULL_ELF_SHA256 \
  --storage-root /srv/goat --elf-file /srv/goat/program.elf \
  --token-file /etc/my-app/user.token \
  --stage-addr 127.0.0.1:51012 --http-addr 127.0.0.1:51013
```

Set `proof_storage_root: /srv/goat` on each matching `ziren-gpu` prover, in its app's
spec or its `apps.yaml` entry. Point the SDK's `ENDPOINT` to `http://127.0.0.1:51012` for a local
client. Keep its signing key; older SDKs without bearer headers can use the
endpoint's configured namespace user `--token-file`. The endpoint reads that file
on each request, so token rotation and revocation still apply. Supplied authorization
is checked directly, without falling back to the configured token. This binary binds
loopback only; external access requires an authenticated HTTPS app gateway.
Repeat `--elf-id` and `--elf-file` for multiple allowlisted programs in the same
namespace. Each program is a separate Fleet app; the endpoint finds its name from
`GET /apps` with the caller's token and submits to `/apps/NAME/jobs`.
GOAT uses its app-owned SDK endpoint on loopback ports `51012`/`51013`.
Integrated EthProofs calls its local GPU server directly and requires no SDK endpoint.
For other SDK-driven apps, use a separate storage root, scoped token and pair of
loopback ports for each endpoint. Set the pipeline’s SDK `ENDPOINT` to its local
stage port and its prover's `proof_storage_root` to the matching directory.
Changing the endpoint does not enable a disabled Fleet app.
Successful SDK status calls read the proof directly from this app's local output
storage using Fleet's authenticated completion references. Public values use the
same local path. Both reads check the file size, SHA-256 hash, and confinement to
the configured output directory; no output fetch heartbeat or control-host relay
is needed. The proof worker wakes its control loop immediately when execution
finishes so completion metadata does not wait for the periodic heartbeat.
Fleet CLI/MCP clients keep their control-plane URL.
Pass `--scheduler-ca-file ca.pem` when Fleet uses an operator-issued CA.

On the machine agent, configure `--proof-node-binary /path/to/fleet-ziren-node`
and repeat `--proof-storage-root /srv/goat` for approved storage roots.
For `deploy/node-start.sh`, set `FLEET_PROOF_STORAGE_ROOTS_FILE` to an administrator-owned
file with one absolute root per line; `deploy/install.sh node --proof-storage-roots FILE`
installs this configuration. Keep these roots configured when GPU actors stop.
The machine agent executes storage-only fetches separately from its heartbeats and Ray operations.

Fleet stores no proof inputs or outputs. A remote fetch through Fleet transfers its
requested bytes through the control host. The app endpoint logs input storage and metadata admission time
separately from the node's `prove_ms`.

**Proving time is execution time.** For GPU proofs, `prove_ms` comes from the
GPU server; input/output transfers, queueing, setup and result fetching are
excluded. CPU/mock provers measure the local proof call. Wall time is recorded
separately for diagnostics and must not be reported as proving time. A GPU server
that omits execution timing fails the job instead of substituting RPC elapsed time.
The SDK response carries this value in `GetStatus.proving_time`. Reporting clients
must preserve it (for example, through `take_prove_ms()`); an SDK that only logs
it needs a timing accessor. Integrated EthProofs takes execution timing directly
from its GPU response and excludes preparation, queueing and reporting.

## Namespaces, access and quotas

A namespace is a resource and the accounting and billing unit. A token names
one namespace and may optionally have a tag. Tokens with the same tag share
proofs and batch jobs; different tags isolate that work. Untagged tokens
share the namespace's untagged work. Refreshing preserves the namespace and optional
tag, including access to earlier work. Each proof app's registration defines its scheduling policy.
The workload types are `app` and `batch`.

### Token access and permissions

An administrator creates a namespace with an immutable ID and unique name such
as `alice`. Names start with a lowercase letter, contain lowercase letters,
digits or hyphens, and have at most 63 characters; `system` and `admin` are
reserved. Renaming and deleting namespaces are not currently supported.

A token belongs to a namespace, with an optional tag. Several tokens
can share a tag, for example teammates, laptop keys and automation keys.
New tags use `KEY=VALUE`, such as `scope=default` or `team=research`. The key
starts with a letter; each part contains up to 63 letters, digits, dots,
underscores or hyphens. Tags match exactly: `team=research` and `team=production`
are separate scopes. Apps and batches use the same tag rules. Bare tags are rejected.
Members can access that tag's existing jobs; another tag in the
same namespace cannot. A namespace token holds some of the permissions `read`,
`submit` and `manage` (default `read` and `submit`); a token without `manage`
needs explicit `--app` grants, by app or batch name. An administrator's token
holds `admin` alone, has no namespace and may do everything in every namespace.
Only administrators create namespaces, issue tokens or change access.

Proof status checks the app grant before waiting or revealing any job state.
The SDK can still poll a rejection recorded for its own signer, including a
submission refused because the app was not granted; other signers cannot read
that rejection without the app grant.

User requests infer their namespace from the verified token. A
`--namespace` selector must match that namespace; it cannot authorize access to
another tag or namespace. Administrators can select `--namespace NAME --tag TAG`
(`?namespace=NAME&tag=TAG` in the API) when listing jobs or looking up a client's proof ID. Fleet-wide records include
namespace IDs and tags and retain the submitting token for audit.

Revoking one token blocks that key. Suspending a namespace blocks every
token in it. Resuming a namespace does not restore individually revoked
keys. Neither action cancels accepted work; an administrator stops jobs explicitly.
Each proof app's `max_queued` and each batch's resource limits also apply. Each namespace has a shared
GPU/CPU quota, described below.

### Workload ownership

Every app belongs to an existing namespace. An administrator creates the namespace
and issues its owner token for the built-in `scope=default` tag. The owner can then
create apps and batches, manage their metadata and submit jobs
inside that namespace and tag:

```bash
fleetctl namespace create alice --gpus 2 --cpus 16 \
  --endpoint http://127.0.0.1:51012 --out alice.env
fleetctl --token-file alice.token --namespace alice --tag scope=default batch add training --image IMAGE
fleetctl --token-file alice.token --namespace alice --tag scope=default app add eth --spec prover.json --elf-id FULL_ELF_SHA256
```

Owner tokens hold `read`, `submit` and `manage`. Metadata changes still enforce
the token's namespace, tag and any explicit app grants; changing a proof app also
needs the token's tag to cover every node entry running it. Tokens without `manage`
can submit and inspect permitted jobs and read granted scaling status; they
cannot change workload metadata.

| Action | Required access |
|---|---|
| Namespace creation, token issuance, namespace/tag quotas and node administration | `admin` |
| Namespace listing and quota/usage inspection | `read` for its own namespace and tag; administrators can inspect all |
| App creation, removal, priority and enable/disable | `manage` in the namespace, with any explicit app grants |
| Batch creation, priority, enable/disable and removal | `manage` for the namespace, tag and batch |
| Batch host mount roots and runtime user | `admin` |
| App `user`, `volumes` and `proof_storage_root` at creation | `admin` |
| App image, environment, command, user, workdir and volumes changes | `admin` |
| App GPU/CPU and replica changes | `manage` for the app; namespace/tag quotas and node capacity still apply |
| Job and log inspection | `read` within its scope |
| Job submission and batch job cancellation | `submit` within its scope |
| Batch job preemption | `admin` |

Batch image, environment and resource policy are supplied when
registering a batch and changed with `fleetctl batch update NAME --image ...`
(`PUT /batches/NAME/settings`). Accepted jobs keep the settings they were
submitted with; new jobs use the new ones. Only administrators may set batch
`allow` roots or `user`, at creation or on update. Owner-created batches start
without host mounts and use the image's default user until an administrator
configures one. Apps start from their creation spec or a node's
`apps.yaml`; owners manage their lifecycle, priority, allocation and
settings through the control plane. `fleetctl app update NAME --image ... --env K=V`
(`PUT /apps/NAME/settings`) changes an app's server or prover image,
environment, command, user, workdir, volumes, shm/memory size, health path or
timeouts. Changing image, environment, command, user, workdir or volumes requires
administrator credentials because apps can reach host networking, storage
and prover credentials. Scoped managers retain resource-size, health-path and
timeout updates. Mixed updates containing an administrator-only field are
rejected without applying any fields.

Execution nodes independently reject batch mounts overlapping Fleet state,
device, kernel, runtime or container-daemon directories, and reject direct
socket, FIFO and device mounts, even if an older accepted policy allows them.
Administrators must approve storage dedicated to the namespace; an approved
directory grants access to its contents.

The scheduler is the source of truth for an app's configuration and tells each
node with one app-level node operation, `NODE_OPERATION_APP` (7): it addresses an app
(`{"app": {namespace, namespace_name, name}}`: its namespace by id and name, plus the
app's name; the node finds its `apps.yaml` entries that run it), a batch
(`{"batch": {name}}`) or the one entry a change of an app stopped (`{"entry": {name}}`),
and either removes it (`remove`), writes its configuration into `apps.yaml` or
`batches.yaml` (`set`): enabled state, resources and settings, or, for an app being
placed, adds its entry disabled (`create`, with the entry). Enabling,
disabling, scaling and updating an app all run the same way: its replicas drain
and stop, its node writes the new configuration, and they start again unless the app
is now disabled. `NODE_OPERATION_SET_APP` (4), with `{"app": entry, "disabled",
"scale_id", "scale_phase"}` (or `"scale_cleanup"`), only starts or stops an entry's
replicas as configured, for that sequence and when a node's replicas stop
matching it. The scheduler records each entry on each node as an `AppDeployment`
keyed `{node}-{entry name}`; its reservation's allocation id is `deployment:{key}`.
`fleetctl app logs` asks the node for an entry's log with a `LogRequest` naming that
`entry`. Priority is a scheduling parameter within the namespace and stays in
the scheduler.

Owner metadata routes use the same authenticated API as the CLI. Every app has
the same routes:

| Workload | Routes |
|---|---|
| Apps | `GET /apps`, `POST /apps` (create an app from its container `spec`, a prover also with its `proof`), `GET` / `DELETE /apps/NAME`, `PUT /apps/NAME/priority` with `{"priority":N}`, `PUT` / `DELETE /apps/NAME/disabled`, `PUT /apps/NAME/settings`, `GET /apps/NAME/logs` |
| Allocation and status | `PUT` / `GET /apps/NAME/scale` |
| Proof app jobs | `GET /apps/NAME/jobs?limit=N&active=true`, `POST /apps/NAME/jobs` (app-storage references), `GET /apps/NAME/jobs/ID`, `GET /apps/NAME/jobs/ID/output/proof.json` and `.../output/public_values.bin` |
| Batches | `GET` / `POST /batches`, `GET` / `DELETE /batches/NAME`, `PUT /batches/NAME/priority`, `PUT /batches/NAME/settings`, `PUT` / `DELETE /batches/NAME/disabled` |
| Batch jobs | `GET` / `POST /batches/NAME/jobs`, `DELETE /batches/NAME/jobs?older_than=DAYS`, `GET` / `DELETE /batches/NAME/jobs/ID`, `POST /batches/NAME/jobs/ID/cancel`, `POST /batches/NAME/jobs/ID/preempt`, `GET /batches/NAME/jobs/ID/logs` |

Every API route needs a token permission. `GET /namespaces[/NAME]` (own
namespace), `GET /apps...` and `GET /batches...` need `read`. `POST /apps/NAME/jobs`,
`POST /batches/NAME/jobs` and `POST /batches/NAME/jobs/ID/cancel` need `submit`;
`.../preempt` needs `admin`. Any other `/apps` or `/batches` change (create,
delete, priority, disabled, scale, settings, purge) needs `manage`; the settings
handler additionally requires `admin` for the execution fields listed above. Everything
else (status, namespace creation, quotas and suspension, allocations, the
preemption plan, tokens and node drain) needs `admin`. `/healthz` and
`/{proof_id}/wrap/public_values.bin` need no token, `/access` takes any valid
token and `/internal/*` the cluster token. An administrator selects a namespace
with `?namespace=`. Disabling a batch blocks new submissions and lets
accepted jobs continue; disabling an app drains its replicas before releasing
their reservations. A deleted batch stays listed with state `deleted`, and readable,
while it still has job records; its jobs keep their own lifecycle until purged.

App and batch creation infer the namespace from a verified user token;
administrator requests must supply it. An unknown namespace is not automatically
created. App names are unique across the fleet; each program has one proof app
per namespace, so namespaces can prove the same program independently. Disabling or deleting one
app leaves the others and their jobs intact. Batch names remain unique within the control plane; each workload has one namespace.
App and batch grants are explicit; a grant from another namespace cannot authorize submission. Admin batch submissions
also inherit the app's namespace and participate in its quota.

The app creation API uses `POST /apps?namespace=alice` with
`{"name":"eth","spec":{"image":"IMAGE","gpus":1},"proof":{"elf_id":"FULL_ELF_SHA256"}}`,
optional `priority`, and in `proof` optional `class`, `anchor`, `backends`, `zkm_version`
and `max_queued`; a container app has no `proof` and a `spec` with `port_base`.
The control-plane batch API accepts `POST /batches?namespace=alice` with the batch JSON, or a
`namespace` field in the JSON. Names and immutable namespace IDs are accepted;
registrations persist the ID. A configured batch or `apps.yaml`
entry can also specify `namespace: alice`; its namespace is verified before launch.
The owner token is bound to `scope=default`; jobs inherit that tag. Batch owners
prepare inputs in storage and include the code and storage client in their image.
Administrators configure host mount roots and runtime users for batches. Submit batch jobs through
`fleetctl batch submit NAME` or `POST /batches/NAME/jobs`; job records name their
`batch`, and their reported type is `batch`. `GET /batches/NAME/jobs/ID` (`fleetctl job ID`)
also returns `request`, the submission as accepted: its name, command, gpus, cpus,
gpu_mem, mounts, env and nodes. Listings leave it out.

### GPU and CPU quotas

A namespace quota caps **concurrent GPU/CPU units** held by proof jobs, one-shot
batch jobs and app replicas assigned to that namespace. Reservations from
all tags and tokens share this cap. An optional tag can cap its own reservations.
Admission must fit both caps; tag caps do not
reserve a guaranteed share of fleet capacity. Neither quota has a duration, expiry,
hourly budget or periodic reset. Jobs and enabled apps can keep their reservations
indefinitely; normal job lifecycle rules and existing application timeouts still apply.

```bash
fleetctl namespace create alice --gpus 2 --cpus 16 \
  --endpoint http://127.0.0.1:51012 --out alice.env
fleetctl namespace quota alice --gpus 4 --cpus 32
fleetctl namespace get alice       # quota and currently reserved units
fleetctl namespace quota alice     # clear both caps: unlimited (a write operation)
```

Each `quota` command replaces both caps. An omitted cap is unlimited; zero
prohibits allocation of that resource. For example `--gpus 0 --cpus 8` permits
CPU-only batch jobs. Fractional GPU units are supported and retain the existing
per-batch VRAM requirements for shared cards. New namespaces
start with unlimited caps unless the administrator sets them.

Use the publicly reachable TLS endpoint in bundles shared with users; `127.0.0.1`
would refer to the user's own machine. Set `--server-ca` only when that endpoint
uses a private CA.

Batch jobs requesting more than either cap receive HTTP 422 before creation. The Rust
scheduler selects a ready machine with sufficient reported capacity, reserves
namespace and tag units, then sends a launch command. The node creates the local
Ray placement. If capacity or either quota is occupied, the batch job stays
`waiting_quota` without a reservation. Dependencies also wait without occupying
quota. Once assigned, a batch job retains its reservation until execution cleanup is
confirmed, including when the machine disconnects before acknowledging launch.

The Rust scheduler retries admission while capacity and quota are occupied.
Durable commands and execution leases recover an interrupted launch without
starting a second actor. Node heartbeats import existing local execution records
without relaunching them. Completed jobs remain readable through the control plane
when a node is disconnected.

An admitted batch job reserves its requested units throughout execution, VRAM waits
and retries. Finishing or cancelling releases its reservation after actors stop and the actual container is
confirmed stopped or absent. An unreachable machine or failed Docker inspection
keeps the reservation and history; cleanup is retried when the machine returns. Preemption releases it and requires quota admission again before restarting. A lost batch job retains its
reservation until cleanup is confirmed so an orphaned container cannot silently free the
namespace's allowance. Cancelling an unreachable batch job does not release its quota.
Actor cleanup may take a few seconds after a terminal status appears.

Proof placement charges the selected proof replica's GPU and CPU units.
A proof stays queued until an eligible replica fits both remaining quotas. A timed
out or disconnected proof keeps its reservation and rejoins its original prover;
it cannot fail over to another prover while the original execution is uncertain.
The replica's entire allocation must fit; an active proof keeps its allocation.
App owners can use `fleetctl app scale` to adjust the app's GPU/CPU units
between proofs to fit user quotas. App replicas reserve their configured GPU/CPU units
against the namespace and tag they are assigned to. Shared infrastructure
configured in the `system` namespace is accounted there.

Reservations are stored in Postgres and recovered after a scheduler restart;
refreshing tokens never changes them. If quota admission cannot be recorded,
new batch jobs remain waiting. Reducing a cap lets already-admitted work
finish, including work recovered during an upgrade. Further admissions wait
until usage fits; a waiting batch job whose request exceeds a newly lowered cap fails
with an explanation. Operators can inspect reservations, including each tag, through
`GET /namespaces/alice/allocations`.

### App priority and preemption

App owners set workload priority with their owner token. Individual job
submissions inherit this policy and cannot supply a separate priority:

```bash
fleetctl --token-file alice.token --namespace alice batch add training \
  --image python:3 --allow /srv/training --priority 10
fleetctl --token-file alice.token batch priority training 20
fleetctl --token-file alice.token --namespace alice app priority eth 100
```

Proof and batch jobs retain the priority inherited when submitted, including across
retries, preemption and restart. Changing the app affects new submissions. Higher
priority jobs are considered first; equal priority pending jobs use FIFO order
(creation time, then ID). Priority does not bypass quotas, app grants, dependencies,
placement constraints or app anchoring. A blocked job can be skipped so work
that fits can proceed.

Automatic preemption selects only lower-priority batch jobs in the requesting namespace
whose batch permits preemption and whose minimum runtime has elapsed. It prefers
the lowest priority victims and plans enough physical capacity and namespace/tag
quota before stopping any batch job. The Rust scheduler makes this plan from Ray-reported
capacity and batch candidates; the Python adapter executes the selected stop, and
the Rust scheduler requeues the job after confirmed cleanup. Equal or higher priority batch jobs, batch jobs in another
namespace and active proofs are protected. Proof priority orders queued proofs;
it does not resize active proofs or evict running provers. Persistent apps
are also protected from automatic eviction. Their app priority controls which batch jobs they may displace when starting;
changing priority alone does not recreate actors. An app's creation spec or
`apps.yaml` entry provides the initial execution settings.

Stopping uses SIGTERM followed by SIGKILL after `stop_timeout_s`. Placement and
quota reservations remain until every container is confirmed stopped or absent.
An unreachable worker keeps its reservations, and the durable requeue intent is
recovered after restart. Batch jobs resume with their original ID, output and inherited
priority, without spending a retry. All priorities default to `0`, so automatic
preemption requires explicitly assigning different priorities.

Ray provides placement groups and actor resource reservations. Fleet enforces
namespace ownership, optional tag isolation, quotas and preemption policy; a Ray
namespace alone does not enforce these limits. Actors on each machine use a local Ray
control namespace, while Fleet namespace IDs scope policy and accounting.

### Optional tags

```bash
fleetctl namespace create team --gpus 8 --cpus 64 \
  --endpoint http://127.0.0.1:51012 --out team.env
fleetctl namespace quota team --tag scope=research --gpus 2 --cpus 16
fleetctl namespace quota team --tag scope=production --gpus 6 --cpus 48
fleetctl namespace get team                  # namespace quota, usage and tags
fleetctl namespace get team --tag scope=research   # namespace totals and tag quota/usage
fleetctl token issue --namespace team --tag scope=research \
  --app training --endpoint http://127.0.0.1:51012 --out alice.env
fleetctl --namespace team --tag scope=research job
```

Both research and production contribute to the team's shared quota. If production
occupies six GPU units and research occupies two, all eight units are in use and
neither can admit another GPU job. Jobs can retain admitted units indefinitely. Lowering
either quota lets admitted work finish and blocks further admissions as needed.

Tags use the same character and length rules as namespace names and are scoped
to their namespace. The `scope=default` tag is created with each namespace. Additional
tags are introduced when an administrator assigns one to a token or sets its
optional cap. A newly used tag has no extra cap unless the administrator sets one.

Membership cannot change for an existing key. Refresh copies the original tag,
independently of CLI selectors. Namespace owner bundles use `FLEET_TAG=scope=default`.
Users cannot select another tag or spoof it in a submission body.

### Administrator workflow

```bash
fleetctl namespace create alice --gpus 2 --cpus 16 \
  --endpoint http://127.0.0.1:51012 --out alice.env
fleetctl namespace list
fleetctl namespace get alice
# Give alice.env, alice.token, and alice.ca.pem (if generated) to the owner securely.
FLEET_TOKEN_FILE=alice.token FLEET_NAMESPACE=alice FLEET_TAG=scope=default \
  fleetctl batch add training --image APPROVED_IMAGE

# Copies the namespace, optional tag, permissions, app grants and label to a new key/token.
fleetctl token refresh OLD_TOKEN_ID \
  --endpoint http://127.0.0.1:51012 --out alice-next.env
# After the user verifies the replacement:
fleetctl token revoke OLD_TOKEN_ID

fleetctl namespace suspend alice
fleetctl namespace resume alice
```

Namespace creation takes the SDK endpoint the owner's clients use, their
[app-owned proof endpoint](#app-owned-proof-endpoint), for the bundle's `ENDPOINT`. `refresh` does not revoke
the old key automatically or overwrite an existing bundle.

Issuance requires an existing, active namespace. A typo cannot silently create a
new owner. Token namespace and optional tag cannot be changed after issuance.

Namespace and access HTTP endpoints, on the existing private scheduler listener.
Writes require administrator credentials. Scoped tokens can read their own
namespace by name or immutable ID; lists return only that namespace. Shared
namespace quota and usage are visible, with tag caps and usage restricted to the
token's tag. Another namespace or tag selector is refused:

| Method and path | Action |
|---|---|
| `POST /namespaces` with `{"name":"alice"}` | Create |
| `GET /tokens` | List opaque token IDs, permissions and access scopes |
| `POST /tokens` | Issue an access token (administrator only) |
| `DELETE /tokens/TOKEN_ID` | Revoke a token |
| `GET /namespaces` | Scoped users: own namespace; administrators: all |
| `GET /namespaces/alice` | Scoped users: own namespace quota, total usage and own tag; administrators: all tags |
| `PUT /namespaces/alice/quota` with `{"gpus":2,"cpus":16}` | Replace caps |
| `GET /namespaces/alice/allocations` | Administrator-only: inspect reservations across tags |
| `GET /namespaces/alice?tag=scope=research` | Inspect namespace totals and tag caps/usage; scoped tokens must match that namespace/tag |
| `PUT /namespaces/alice/quota?tag=scope=research` with `{"gpus":2,"cpus":16}` | Replace tag caps, recording a new tag if needed |
| `PUT /namespaces/alice/suspended` | Suspend |
| `DELETE /namespaces/alice/suspended` | Resume |

Token registration accepts a namespace name or ID, an optional `tag` and its `permissions`. Tokens have opaque IDs; proof signer bindings are internal and are not exposed through the token API.
Job/app lists and job reads accept `?namespace=alice&tag=scope=research`; the batch API supports the same selectors. Refresh
is a CLI operation using administrator token lookup and registration.
No additional listener or public port is required.

### Job fields and storage

Every proof job comes from a namespace token (`Authorization: Bearer`) holding
`submit` and is signed by that token's key; the signature must recover to the
token's address. The scheduler finds the job's app by namespace and ELF ID.
`GetStatus` needs a token of that namespace with `read` or `submit`.
The SDK signs and receives its original proof ID. The scheduler resolves
`(namespace_id, optional_tag, client_proof_id)` to a unique internal ID, derived from their
SHA-256 hash for new jobs. Nodes, retries and deduplication use that
internal ID. Different tags can submit the same client proof ID independently.
Resubmission within a tag returns the existing job without replacing input. Reference submissions with conflicting inputs or proof parameters are rejected.

Batch job IDs and directories retain their existing flat layout, with namespace
and tag ownership recorded in each batch spec. Namespace names are never host paths.
App owners prepare batch inputs in shared or object storage and manage access to
that storage. The app reads inputs directly using its own storage client or a
mount permitted by the owner's batch. Fleet passes command arguments
and environment variables to the app. The app writes outputs and checkpoints directly to its owner's
storage, including any files staged in its local `{out}` workspace. Fleet does not
transfer batch inputs or outputs. Dependencies, logs and mutations check both namespace
and tag membership.
A verified replacement token can read accepted batch jobs from earlier keys.

For a batch command that uses `INPUT_URI` and `OUTPUT_URI` from its environment:

```bash
fleetctl --token-file alice.token batch submit training --gpus 1 \
  --env INPUT_URI=s3://alice-inputs/dataset \
  --env OUTPUT_URI=s3://alice-results/job-output -- python /app/train.py
```

Proof job identity is required and immutable:

| Field | Meaning |
|---|---|
| `id` | Internal execution and storage identifier. |
| `owner` | Internal proof signer audit identifier, retained when an access token is refreshed. |
| `namespace` | Immutable namespace ID controlling resource accounting and access. |
| `tag` | `key=value` isolation label; proof records use `""` for untagged work. |
| `client_id` | Original SDK proof ID, unique within namespace and tag. |
| `app`, `elf_id`, `kind`, `created_ms` | Submission metadata retained throughout retries; `app` is empty for a request refused before its app was known. |

`node` and `started_ms` can be absent while queued. `finished_ms` is absent
until the job succeeds or fails. Counters start at zero and failure history starts
empty. Rejected authenticated proof requests retain their submitting identity;
requests without an authenticated owner are refused without creating a job.
Token tags and batch tags remain optional (`null` when absent).

Public-values URLs carry the issuing token's ID and a key, and are bound to that token.
Revoking that key or suspending its namespace invalidates its URLs. A replacement
token can poll the same job to obtain a fresh URL.

Postgres creates schema 16 directly and checks its version at startup. A schema 14
or 15 database migrates automatically at start, in one transaction: each app takes a
name (its tenant's when that tenant had one app in the namespace, else
`TENANT-<first 8 hex of the ELF id>`) and the default placement policy; jobs name
their app; `TENANT/ELF` grants become app names; namespace-less tenant signer
tokens are removed. Other schemas require an explicit metadata reset.
Tags must use `key=value`. A reset removes job history and access registrations;
recreate namespaces and tokens, and preserve app-owned proofs and checkpoints.
Operator-managed apps use the reserved `system` namespace. User tokens
cannot be issued in `system`.

Configured batches and persistent app entries default to `system`.
Administrators assign a user app's namespace explicitly; API registrations require
an existing namespace. Namespace quotas govern proof and batch jobs as well as
app replica reservations. Disabling an app releases its reservation
after the actor stops.

### MCP clients

Share these connection details with external MCP client users:

- **Server:** ZKM Fleet
- **Transport:** Streamable HTTP over HTTPS
- **URL:** `https://34.215.238.232:8905/mcp`
- **Authentication:** HTTP Bearer token in the `Authorization` header

For clients with a custom-header field, set `Authorization` to `Bearer <user-token>`.
An administrator issues each user a namespace/tag-scoped token and delivers it
securely. Do not give external users the administrator token.

Initialization and `tools/list` are available without a token, so clients can
discover the available tools and input schemas before adding a token. `tools/call`
requires a current user token; the scheduler applies its namespace, tag and
app grants. Proof submissions must also be signed by the token's key. MCP has no
administrator token. Clients must support remote
Streamable HTTP and allow the Bearer header to be configured. Clients that only
support local `stdio` servers cannot connect directly to this endpoint.

The server lists long-running apps with `fleet_apps_list` and `fleet_app_get`, and
`fleet_app_create` creates one as `fleetctl app add` does: a `spec` object and optional
`priority`, plus a prover's `elf_id`, `class`, `anchor`, `backends`, `zkm_version` and
`max_queued`.
Batch tools are `fleet_batch_create`, `fleet_batches_list`,
`fleet_batch_get`, `fleet_batch_jobs_list`, `fleet_batch_job_get`,
`fleet_batch_job_submit`, `fleet_batch_job_logs` and `fleet_batch_job_cancel`. `fleet_app_logs` reads a visible app's log from its node. Batch job submission
uses a `batch` name and a command; the jobs list, logs and cancel tools take an optional `batch`. `fleet_jobs_list` remains a combined execution
view with `kind: "batch"` or `kind: "app"`. Batch creation requires a
workload-management grant. Its `priority` is owner-managed; nonempty `allow`
roots require an administrator.
`fleet_batches_list` and `fleet_batch_get` use `fleetctl batch` and `fleetctl batch NAME`; `fleet_batch_jobs_list` and `fleet_batch_job_get` use `fleetctl job --type batch` and `fleetctl job ID`.
`fleet_batch_job_submit` accepts a `mounts` array such as `["/data/models:ro"]`;
each path must be inside the batch's allowed roots. Existing metadata changes,
enabling and app scaling use the CLI or HTTP API; MCP currently exposes app and batch
creation and job operations. Proof jobs remain submitted through the Ziren network SDK;
MCP can list and inspect their records.

Tokens with `manage` manage app and batch
metadata within their namespace, tag and app grants:
creation, priority, enable/disable and removal. Owners can
scale their apps within namespace and tag quotas, and submit
batch jobs in their tag. SDK proof submissions are signed by the token's key.
Tokens without `manage` can use their granted apps but cannot change their metadata.
Administrators manage namespace and tag quotas, namespace access, token issuance,
batch host storage and runtime users, and app execution settings.
App removal refuses while any proof is queued or running and deletes that app's
terminal proof records and control-host artifacts. App-owned storage retention remains with the app owner. Administrators can also issue tokens limited
to approved apps and a selected namespace/tag. Token refresh preserves access to jobs.
Namespaces are the resource accounting and billing unit; namespace and optional tag quotas
cap concurrent GPU/CPU units across proofs, batch jobs, and app replicas, with no quota time limit. An app replica reserves its configured resources for as long as it is enabled; disabling or removing it releases that reservation after the actor stops.


Model servers, ComfyUI and provers all appear as `type: app` in `fleetctl app`; a record has no `kind`. An app record carries `name`, `namespace`, `tag`, `tags`, `state`, `detail`, `priority`, `disabled`, `resources`, `namespace_quota`, `tag_quota`, `jobs` (`total`, `active`, `succeeded`, `failed`, `last_at`), `spec` (its container settings, including `kind`, and `workers`: the nodes it runs on), and for a proof app `proof` (`elf_id`, its placement policy, `mean_prove_ms`, `anchored_on`, `created_at`, and `image`: the container image of the provers proving for it, a list while they differ). A proof app created before `spec` existed, proved by provers its nodes configure, has only `proof`. A batch record (`fleetctl batch`, `type: batch`) has the same envelope: its `state` is `enabled`, `disabled` or `deleted`, its `jobs` count the batch's job records, and its policy sits under `batch` (`image`, `user`, `max_gpus`, `default_gpu_mem_gb`, `shm_gb`, `allow`, `env`, `preemptible`, `min_runtime_s`, `stop_timeout_s`, `workers`). Batch jobs appear only under `fleetctl batch` and `fleetctl job --type batch`.
`fleetctl app disable NAME` stops a persistent app and releases its GPUs. A proof
app has its own dedicated Ziren GPU server: disable blocks new submissions,
drains its active proofs, then stops that server and releases its GPUs. Queued
proofs wait until the app is enabled again. Enabling an app starts its
replicas before accepting work. Disabling a batch only blocks new
batch jobs; accepted batch jobs finish.

Every entry in `apps.yaml` has an `app`. Each `ziren-gpu` entry must bind
its server to the one app it is dedicated to using `namespace`, `app: NAME` and
`elf_id: FULL_ELF_SHA256`; a `ziren-cpu` entry names its app the same way, with
`elf_id` optional. An entry the scheduler creates for an app takes the app's name.
Any other entry (`kind: app`) is its own app, takes no `elf_id`, and its
`app` defaults to the entry name. Do not configure a Ziren server for multiple
apps. Provers and workers receive `FLEET_APP`, `FLEET_APP_NAMESPACE` and
`FLEET_APP_ELF_ID`; `fleet-ziren-node` takes `--app` / `FLEET_APP`.
The spec or `apps.yaml` entry supplies initial resource settings; app owners then manage
allocations through the control plane with `fleetctl app scale`. Batch jobs request resources with
`fleetctl batch submit NAME --gpus N --cpus N`. Active proofs cannot resize
their GPU allocation. Resizing a proof already in progress is tracked in
[issue #3](https://github.com/ProjectZKM/zkm-fleet/issues/3).

## Data

| what | where |
|---|---|
| registered apps, proof jobs, tokens | Postgres (compose): `fleet_apps` (each namespace's named programs and their placement policy, with when each was disabled), `fleet_jobs` (state, kind and a failed job's stage status by name, with CHECK constraints), `fleet_tokens`, `fleet_namespaces` (namespace and optional tag quota settings), `fleet_namespace_allocations`, `fleet_schema` (the schema version) |
| SDK proof ELFs and inputs | App-owned `proof_storage_root`: `elf/<hash>` and `inputs/<hash>`. Fleet stores references, hashes and parameters. |
| SDK proof output | App-owned `outputs/<job id>/<execution lease>/`: proof, public values and a committed manifest. The local app endpoint reads them directly; remote Fleet fetches use an authorized heartbeat ticket and HTTPS relay. Disabling the GPU actor preserves fetching. |
| Integrated EthProofs data | Its app-owned storage root: `checkpoint.json`, `inputs/`, `outputs/<block>/`, `outbox/` and `reported/`; the app manages their format and recovery. |
| apps, app deployments, batches, jobs and command intents | `data_dir/workloads/{apps,deployments,jobs,commands}` on the control host; durable JSON records, including lease and cleanup fences |
| batch inputs, outputs and checkpoints | owner-managed shared or object storage, read and written directly by the batch command |
| batch logs | `data_dir/workloads/artifacts/<job-id>/<lease>/logs` on the control host; nodes retain execution files in `FLEET_JOBS_DIR/<job-id>/` (normally `work/jobs/`) |

## Tokens

| what | held by | issued by |
|---|---|---|
| user token (`FLEET_TOKEN_FILE`) | the namespace owner/client | namespace/token issuance; authorizes proof RPCs and HTTP APIs within its scope; revocable |
| SDK signing key (`ZKM_PRIVATE_KEY`) | the client | generated in the token bundle and bound internally to its token |
| server certificate + CA | gateway and control API; clients get `CA_CERT_PATH` when supplied | `install.sh` (`fleetctl init tls`) |
| cluster token | scheduler and every node; authenticates node-initiated requests | `install.sh` (`fleetctl init token`) |
| admin token | operators | `install.sh` (`fleet-scheduler issue-admin`); more with `fleetctl token admin` |

Administrator tokens live in the database. On the scheduler host,
`fleet-scheduler -c fleet.yaml issue-admin --label NAME --out FILE` writes an
administrator token's record into the database and its secret to FILE (0600,
never overwritten); a running scheduler reloads tokens every `heartbeat_secs`.
`install.sh` issues one into `etc/admin.token` after Compose is up. Issue more
with `fleetctl token admin --label NAME --out FILE`. An in-memory scheduler (no
`database_url`, for development and tests) writes its own to `data_dir/admin.token`
at start. `fleet.yaml` no longer takes `tenants`, `default_tenant`,
`require_signature` or `admin_token_file`; the scheduler refuses to start with
any of them and names what replaced it.

Proof clients use bundled SDK signing material and bearer authorization at their
app endpoint. Loopback clients use local HTTP; external clients require an authenticated HTTPS app gateway. The token API exposes only opaque IDs and access
scopes. Tokens with `manage` can create apps and manage their metadata;
tokens without it can use only granted apps and batches. Revocation applies to both proofs and
batch jobs.

The scheduler’s backend listeners stay private behind the HTTPS gateway. Node control gRPC uses TLS with the scheduler
certificate and cluster bearer token; use an HTTPS `scheduler` URL in `apps.yaml`.
For a private certificate authority, set `scheduler_ca_file` so prover containers trust it.
The installer sets this to its generated public CA certificate; distribute that CA to
each execution machine.
The scheduler holds a PostgreSQL advisory lock for its lifetime, so only one process can
dispatch from a database at a time. Fleet does not use SSH channels between machines;
MCP exposes only user-scoped tools through the existing HTTPS gateway. Users and operators both use `FLEET_TOKEN_FILE`
(`--token-file`); without it `fleetctl` reads `admin.token` from `FLEET_ETC`,
`~/.config/zkm-fleet` or `/etc/zkm-fleet`. Batch API handlers are part of the Rust scheduler; there is no separate Python HTTP listener.
