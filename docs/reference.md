# Fleet reference

What Fleet runs and how, who may do what, its APIs and where it keeps data.
[Installation](installation.md), [operations](operations.md) and
[maintenance](maintenance.md) have the procedures.

## Architecture

Each execution machine runs its own local Ray instance and `fleet-node`. The agent
sends authenticated HTTPS gRPC heartbeats to the control plane: replies carry typed
launch, stop and app commands, and later heartbeats acknowledge them and report
capacity and job state. Nothing connects in to a node: there is no streaming RPC,
inbound node API, shared Ray head or SSH channel. Adding a machine needs only the
control-plane URL and the cluster token, and the control host runs no Ray.

The Rust scheduler decides admission, quotas, priority, dependencies and preemption,
and drains apps before it sends node commands. A small Python adapter on each node
carries out what Rust authorizes: local placement, container startup, retries and
cleanup. It holds the cluster token for proof heartbeats and has no administrator
client, submission CLI, policy editor or HTTP listener.

| Component | Does | Depends on |
|---|---|---|
| `fleet-scheduler` | Admission, namespace policy, quotas, placement, durable control records and optional proof-backed policy ledger | Postgres; policy SQLite and Ziren verifier; no Ray or prover SDK |
| `fleet-node` | Outbound heartbeats, command delivery and log sync | The local Python adapter; no database, signer recovery or prover SDK |
| `bridge/fleet_bridge` | Local resource grants, actors, containers and confirmed cleanup | Ray, PyYAML |
| `fleet-proof-app` | Optional SDK endpoint beside app-owned storage | Fleet HTTPS API; no database or prover SDK |
| `fleet-ziren-node` | Optional SDK-driven proving and retained-artifact fetching | Ziren runtime, installed only where used |
| App worker | The app's own preparation, proving calls, outputs, checkpoints and reporting | Whatever its owner packages |
| `fleetctl` / `fleet-mcp` | Clients of the same authenticated API | MCP is optional and runs `fleetctl` |
| `openshell-gateway` | Optional (`openshell` Compose profile): the unmodified OpenShell access gateway, with Fleet as its compute driver | The scheduler's `policy_gateway.openshell` compute socket; no Docker socket |
| `trustee-as` | Optional (`trustee` Compose profile): Trustee's attestation service for legacy prechecks | Its `trustee-data` volume; no TEE devices or Docker socket |

Fleet checks that processes and containers are up; it doesn't read an app's
checkpoints, so follow an app's progress in its logs. Prepare code, datasets and
models in the workload image or the owner's storage before submitting.

### Code names

Fleet borrows Kubernetes' [workload](https://kubernetes.io/docs/concepts/workloads/controllers/)
and [controller](https://kubernetes.io/docs/concepts/architecture/controller/) terms
without needing a Kubernetes runtime:

- `Job`: one run of a command or a proof. `JobSpec` says how to run it, `JobStatus`
  what happened, and `ProofJob` is a proof's record.
- `AppDeployment`: the desired state of one app entry on one node.
- `WorkloadController`: admits apps and batch jobs and reconciles deployments with
  node reports.
- `Scheduler`: places proofs on registered provers.
- `NodeExecutor`: applies authorized commands through local Ray.
- `ProofFetcher`: coordinates bounded proof downloads from storage nodes.
- `FleetClient` / `FleetctlClient`: the HTTP client, and MCP's client that runs
  `fleetctl`.

A node's apps are entries in its `apps.yaml`, each running one app's replicas there;
its batches are in `batches.yaml`, and its job records in `FLEET_JOBS_DIR`.

### Records and the node report

Shared records and requests are generated from
[`model.proto`](../crates/fleet-proto/proto/fleet/model/v1/model.proto): jobs, apps,
namespaces, tokens, resources, scaling and node reports. `fleet-proto` also holds the
protocol types, artifact validation and SDK step mapping. App-owned settings and
execution details travel as lossless JSON fields, so the Python adapter needs no
protobuf package.

A node's `NodeReport` carries its `apps.yaml` entries, with their observed state, in
`apps` (field 9). The report is version 3; changing it means updating the scheduler,
agent and adapter together. `agent_version` (field 12, optional) is the release the
agent was built for, set by `FLEET_VERSION` at build time (for example `0.1.5`);
`fleetctl node` shows `-` when it's missing.

### Node commands

The scheduler owns each app's configuration and sends it with one operation,
`NODE_OPERATION_APP` (7). It addresses an app (`{"app": {namespace, namespace_name,
name}}`: the node finds the `apps.yaml` entries that run it), a batch
(`{"batch": {name}}`) or the one entry a change stopped (`{"entry": {name}}`). It then
removes it (`remove`), writes its settings into `apps.yaml` or `batches.yaml`
(`set`: enabled state, resources and settings), or adds an app being placed, disabled
(`create`, with the entry). Enabling, disabling, scaling and updating an app run the
same way: its replicas drain and stop, the node writes the new configuration, and
they start again unless the app is now disabled.

`NODE_OPERATION_SET_APP` (4), with `{"app": entry, "disabled", "scale_id",
"scale_phase"}` (or `"scale_cleanup"`), only starts or stops an entry's replicas as
configured: in that sequence, and when a node's replicas stop matching it. Each entry
on each node is an `AppDeployment` keyed `{node}-{entry name}`, holding the
reservation `deployment:{key}`. `fleetctl app logs` asks for an entry's log with a
`LogRequest` naming that `entry`.

### Durability

Proof jobs, namespaces, tokens and quota reservations are in Postgres. Batch, app,
deployment and command records are durable files in the control host's `data_dir`;
back that up with Postgres. The scheduler holds a Postgres advisory lock while it
runs, so only one process dispatches from a database. A new database is created at
schema 20, and the scheduler checks the version at startup;
[maintenance](maintenance.md#database-schema-14-or-15-to-16) covers older ones.

Through a scheduler restart or a lost connection, nodes keep running locally,
reconnect through heartbeats and retry completion metadata until it's acknowledged.
A lost connection keeps its reservations: a replay uses the same execution lease, an
interrupted launch recovers without starting a second actor, and only confirmed
cleanup releases resources. Heartbeats import a node's existing execution records
without relaunching them, and finished jobs stay readable while their node is
offline.

## Workloads

Every app and batch belongs to a [namespace](#namespaces-tags-and-tokens). They share
one settings model (name, namespace, image, user, environment, mounts, priority and
enabled state) but not a lifecycle: an app keeps its replicas running, and a batch
runs one command per submission, for minutes or for days. App and batch names are
unique across the fleet and shared between them (409 when taken); a node entry named
like a batch is ignored.

### Apps

An app is a long-running container, such as a model server, ComfyUI or a Ziren
prover. A proof app is an app whose container is a Ziren prover of a named program.
`fleetctl app` lists every app (with `--json`, one array of records with `type: app`),
and each is managed by name.

**Creating.** `fleetctl --namespace NS app add NAME --spec FILE.json` posts a
`RegisterAppRequest` to `POST /apps`: `name`, optional `namespace` and `priority`
(default 0), the file as `spec`, and a prover's `proof`. The spec holds the keys of
the app's `apps.yaml` entry; unknown or mistyped settings are refused.

| Keys | For |
|---|---|
| `kind`: `app` by default, `ziren-gpu` by default for a prover, or `ziren-cpu` | Every app |
| `image` (required), `replicas`, `gpus`, `cpus`, `shm_gb`, `memory_gb`, `env`, `volumes`, `ready_timeout_s`, `stop_timeout_s` | Every app |
| `command`, `port_base` (required; replica i serves on `port_base` + i), `health_path`, `workdir`, `user` | A `kind: app` container |
| `proof_storage_root`, `backend`, `rust_log` | A prover |
| `cpu_limit`, `memory_limit_gb` | An app on driver `docker`, `tee` or `tee-simulation` |

**Reservations and hard limits.** `gpus`, `cpus` and `memory_gb` are what Ray
reserves for each replica: the scheduler places and admits an app by them, and on
the Docker driver nothing caps a container to them. An app may also set hard
limits for each replica's container: `cpu_limit` in CPUs (fractions allowed, at
least 0.01 and at least `cpus`) and `memory_limit_gb` in GiB (at least 6 MiB and at
least `memory_gb`). A limit the app doesn't set isn't applied. Both are refused for a native OpenShell app,
whose `cpus` and `memory_gb` are its limits. Quotas count reservations, not limits.

`cpus` is a positive whole number, except that a shim `kind: app` without GPUs may
reserve 0 or a fraction of a CPU when it sets `cpu_limit`, `memory_gb` and
`memory_limit_gb`: a replica that reserves less than a CPU is always capped. A
replica with `cpus: 0` counts no CPU toward its node's free CPUs, though its memory
counts, so it fits on a node whose CPUs are all reserved. It runs with a CPU weight
of 64 (Docker `--cpu-shares`; the default is 1024), so under contention it yields to
replicas that reserve CPUs, and its `cpu_limit` caps it either way. The scheduler
places an app with hard limits only on a node that reports `shim-limits-v1`, and one
with `cpus: 0` only on a node that also reports `shim-cpu-shares-v1`, its shim's
`runtime-cpu-shares-v1` feature. On a TEE node, `cpus: 0` also needs
`shim-tee-cpu-overcommit-v1` and `cpu_limit` must fit in the node's CPUs
([TEE runtimes](#node-configured-tee-runtimes)). A spec for such an app:

```json
{"image":"REGISTRY/AGENT@sha256:DIGEST","port_base":51000,"command":["agent","--port","{port}"],
 "gpus":0,"cpus":0,"cpu_limit":2,"memory_gb":1,"memory_limit_gb":2,
 "runtime":{"executor":"shim","driver":"docker"}}
```

A prover also takes its program, `--elf-id ID` or `--elf FILE`, and a placement
policy, sent as `proof`:

- `--class latency` places its jobs on the biggest free prover first, and `batch`
  (the default) on the smallest.
- `--anchor` is `none` (default), `shared` or `exclusive`.
- `--backend` is `cuda`, `cpu` or `mock`, repeatable (default any).
- `--zkm-version` is optional.
- `--max-queued` (default 64) caps the app's queued and running proofs; more are
  refused.

Only a prover takes a program, and proof policy without one is refused. A proof runs
only on a prover bound to its app. A namespace has one proof app per program, so
namespaces prove the same program independently, and disabling or deleting one leaves
the others and their jobs intact. In the API:
`POST /apps?namespace=alice` with
`{"name":"eth","spec":{"image":"IMAGE","gpus":1},"proof":{"elf_id":"FULL_ELF_SHA256"}}`.

**Placement.** An app is created disabled and on no node. Its first `app enable`
places it; users never choose the node. Candidates are live, ready, undrained nodes
whose total capacity holds the GPUs and CPUs of all its replicas. The scheduler
prefers a node where they're free now, and among those the one with the fewest free
GPUs; otherwise it takes the one with the most free GPUs, where the app waits for
room. With no node big enough, enabling answers 503.

The chosen node gets a `create`, which writes the entry into its `apps.yaml`,
disabled. Once the node reports it, within 120 seconds, the scheduler enables it and
admits it against the namespace quota. A failed create clears the placement, so the
next enable places the app again. If the node hasn't reported the entry in time,
enabling answers 503, and enabling again waits for the same create. An app that a
node's `apps.yaml` already runs, such as a prover entry bound with `app: NAME`,
isn't placed again.

**Moves.** Each start of an app the scheduler placed is placed again when it leaves
the queue: on its own node when that has room, else on the node that fits it best
now. That node gets a `create` with the app's entry, disabled. Once it reports the
entry, the old node removes its own (its configuration and leftover containers and
processes, never the app's data), which frees the room there, and the app starts on
the new node. A node whose create fails isn't picked again for that app. With no room
anywhere, the start preempts lower-priority work on its own node and keeps the room
that frees. A start whose entry is anchored with `machine` never moves. It waits for room on its node, preempting
lower-priority batch jobs of its namespace when it can. Fleet copies nothing an app
keeps on a node's disks: the app looks after its own storage. Anchor an app that keeps
data there, or runs from it, by setting `machine` in its entry in that node's
`apps.yaml` to the node's machine, as `fleetctl node` lists it.

**States.** An app is `configured`, `starting`, `running`, `draining`, `disabled`,
`offline` or `failed`; a proof app without provers is `registered`. The table shows
the total GPUs and CPUs its replicas reserve, and, where the app sets them, its
memory reservation (`MEMORY GB`) and hard limits (`CPU LIMIT`, `MEMORY LIMIT GB`),
every tag when an app's entries use several; the details show replica settings and
job counts.

**Enabling and disabling.** Enabling starts the replicas before the app takes work.
Disabling drains and stops them, then releases their reservation; durable app data
stays. Disabling a proof app blocks new submissions, drains its active proofs and
stops its GPU server; queued proofs wait for the next enable, and its retained proofs
stay fetchable.

**Changing.** `fleetctl app scale` sets an app's GPUs, CPUs and replicas, and a shim
app's `--cpu-limit` and `--memory-limit-gb`, within its quotas
([operations](operations.md#configure-resources)). They are the app's, not a
node's: the replica count covers all its nodes, and a node the count leaves none
removes the app. An active proof can't be
resized ([issue #3](https://github.com/ProjectZKM/zkm-fleet/issues/3)).
`fleetctl app update NAME` (`PUT /apps/NAME/settings`) changes the image,
environment, command, user, workdir, volumes, shm and memory sizes, health path or
timeouts. The image, environment, command, user, workdir and volumes need an
administrator, since an app can reach host networking, storage and prover
credentials; an update mixing them with other fields is refused whole. Changing
priority alone doesn't restart replicas. `fleetctl app update NAME ... --check`
(`PUT /apps/NAME/settings?dry_run=true`) answers what the update would, including a
refusal because the app's node lacks a capability the settings need, and changes
nothing: a way to check a move to another runtime before the app is drained for
it. What only the node checks when it starts a replica isn't covered. An app on
no node yet takes an update, or a check, in the app itself: the settings are
checked against the nodes there are, no node is told, and its first enable places
it with them.

A change tells the node only what it has to do. Disabling stops the replicas and
writes the configuration; enabling writes it and starts them; settings for a
disabled app are one configuration write; settings for a running app stop it,
write and start it again. Each step's result is answered with the next step, so a
change takes about as long as its stop and its start do.

**Deleting.** `fleetctl app delete NAME --yes` and `fleetctl batch delete NAME --yes`
work the same way. Without `--yes` they only say what would go. A delete is refused
while any of the app's or batch's work runs or may run, and the refusal says what that
is and how to stop it:
- An app's work is its replicas. Disable it first, and wait until every node that
  configures it has stopped them; an offline node holds the delete until it's back.
- A proof app's work also includes its queued or running proofs.
- A batch's work is its queued or running jobs. Cancel them or let them finish.

The app or batch then goes with everything Fleet holds of it:
- On the control host: its job records with their logs, and the commands about them.
  A proof app's job records and anchor go from Postgres.
- On each node: its configuration, with what the node holds of it. A batch's jobs lose
  their directories: record, logs and `out/`. An app's replicas' logs go, and so do its proof jobs' files in its proof
  storage: each job's output directory, and the inputs and ELF that no other job or
  app uses.

Volumes, batch mounts, env files, storage roots and images stay. So does whatever
holds or lies in a path that another app or batch on any node uses, and anything at
the top of the filesystem or around Fleet's own files; the node's result says what it
kept and why, and the scheduler logs it. A container's `workdir` is the container's
own: what it keeps on the host is in its volumes. Job history goes only with its
batch; there is no separate purge.

The record stays, hidden, until every node has removed it, so the name can't be
reused before then. A node that is offline removes it once it's back; deleting it
again retries a node that failed. A batch deleted by a release before this one kept
its job records: it stays listed as `deleted` until it is deleted again, which removes
them. A node with an older bridge removes only the configuration.

**Node entries.** Every `apps.yaml` entry has an `app`. A `ziren-gpu` entry binds its
server to the one app it serves with `namespace`, `app: NAME` and
`elf_id: FULL_ELF_SHA256`; a `ziren-cpu` entry does the same, with `elf_id` optional.
Don't bind one Ziren server to several apps. An entry the scheduler creates takes the
app's name. Any other entry (`kind: app`) is its own app, takes no `elf_id`, and its
`app` defaults to the entry name. Provers and workers get `FLEET_APP`,
`FLEET_APP_NAMESPACE` and `FLEET_APP_ELF_ID`; `fleet-ziren-node` takes `--app` /
`FLEET_APP`. An app's ports are private: reaching its own protocol from outside needs
an authenticated proxy.

**Records.** An app record has `name`, `namespace`, `tag`, `tags`, `state`, `detail`,
`priority`, `disabled`, `resources`, `namespace_quota`, `tag_quota`, `jobs` (`total`,
`active`, `succeeded`, `failed`, `last_at`) and `spec`: its container settings,
including `kind`, and `workers`, the nodes it runs on. A record has no top-level
`kind`. A proof app also has `proof`: `elf_id`, its placement policy,
`mean_prove_ms`, `anchored_on`, `created_at`, and `image`, its provers' image (a list
while they differ). A proof app created before `spec` existed, and proved by provers
its nodes configure, has only `proof`. A refused proof request stays a job record and
creates no app.

### Batches

A batch is a reusable image and resource policy; each submission runs one batch job.

```bash
fleetctl --namespace NAMESPACE batch add NAME --image IMAGE
fleetctl batch submit NAME --gpus N --cpus N -- COMMAND
fleetctl batch            # the batches, with their active and last jobs
fleetctl batch NAME       # one batch and its newest jobs
```

`fleetctl batch logs`, `cancel` and `preempt` take a job ID and find its
batch; `--batch NAME` skips that lookup. `batch submit --name` names the job. Nodes
can also advertise batches in their `batches.yaml`.

**Settings.** A new batch takes `name`, `namespace`, `image` (required) and `user`,
plus these settings, shown with their defaults: `max_gpus` 8, `default_gpu_mem_gb` 0,
`shm_gb` 16, `priority` 0, `allow` `[]`, `env` `{}`, `disabled` false, `preemptible`
true, `min_runtime_s` 300, `stop_timeout_s` 30 and `runtime` null. Other fields are
refused.
`fleetctl batch update NAME --image ...` (`PUT /batches/NAME/settings`) changes them,
except `priority` and `disabled`, which have their own commands;
accepted jobs keep the settings they were submitted with. Only administrators set
`allow` (the host paths jobs may mount), `user` and `runtime`, so an owner's batch
starts with no host mounts and the image's default user.

**Runtime.** A batch's jobs run through the shim's Docker driver: `runtime` is null
or `{"executor":"shim","driver":"docker"}`, which mean the same. The node's bridge
launches each attempt through the node's trusted-shim, as a run to completion on
that driver: the same image, command, environment, mounts, cards,
user, `shm_gb` and `stop_timeout_s`, and no network for a namespace's job. The bridge
keeps the VRAM wait, the retries and their budgets, and writes the container's output
to the job's log, so `fleetctl batch logs`, cancel, preempt and requeue work the same.
Each attempt is a new generation of the job's allocation, launched once the shim has
removed the one before it. The scheduler places a job only on a node that reports
`shim-batch-v1`, which a node does when its shim's Docker driver runs a container to
completion, reports its exit status and output, starts it without a network and stops
it with a long grace; until one does, the job waits with the detail `waiting for a node
whose shim runs batch jobs`. A node refuses the launch of a job whose runtime its shim
does not run.

**Records.** A batch record (`type: batch`) has an app record's envelope. Its `state`
is `enabled`, `disabled` or `deleted`, its `jobs` count the batch's job records, and
its policy is under `batch`: `image`, `user`, `max_gpus`, `default_gpu_mem_gb`,
`shm_gb`, `allow`, `env`, `preemptible`, `min_runtime_s`, `stop_timeout_s`, `runtime`
and `workers`.

In the API, `POST /batches?namespace=alice` takes the batch JSON, or the namespace as
its `namespace` field; a name or an ID is accepted, and the ID is stored. A batch or
`apps.yaml` entry configured on a node may name `namespace: alice`, which is checked
before launch. Disabling a batch blocks new submissions and lets accepted jobs
finish. Deleting a batch works like deleting an app ([Deleting](#apps)): it is
refused while any of its jobs is queued or running, and its jobs go with it.

**Submissions.** A job takes `command`, a non-empty list of strings, and optionally:

| Field | Meaning |
|---|---|
| `gpus`, `cpus` | Default 1 and 8. GPUs are at most the batch's `max_gpus`, and whole when more than one; CPUs are at least 1. |
| `gpu_mem` | GiB of free VRAM each granted card must have before the job starts. Defaults to the batch's `default_gpu_mem_gb`; 0 means the whole card, so a fractional GPU needs one. |
| `nodes` | Must be 1: a batch job runs on one machine |
| `machine` | A list with at most one machine to run on |
| `mount` | `PATH[:MODE]` entries, inside the batch's `allow` |
| `env`, `workdir`, `name` | Its environment, working directory and job name |
| `after` | A job that must succeed first; if it doesn't, this one fails |
| `retry` | How many times to rerun it after a failure (default 0) |
| `restart_on_oom` | How many times to rerun it after it runs out of memory, spent before `retry` (default 0) |
| `vram_wait` | Seconds to wait for its VRAM to be free (default 1800) |
| `log` | A log file of its own, for administrators only |

Other fields are refused. `GET /batches/NAME/jobs/ID` (`fleetctl job ID`) also
returns `request`, the submission as accepted; listings leave it out.

**Storage.** A batch job's inputs, outputs and checkpoints live in its owner's shared
or object storage. The command reads and writes them directly, with its own client or
a mount the batch allows; Fleet passes its arguments and environment and moves no
data. Each job gets a workspace on its node, `FLEET_JOBS_DIR/JOB_ID/`, with
`FLEET_OUT` as its output directory, and its logs sync to the control host, so a
one-machine job needs no shared mount. Job IDs and directories are flat; namespace
names never become host paths.

```bash
fleetctl --token-file alice.token batch submit training --gpus 1 \
  --env INPUT_URI=s3://alice-inputs/dataset \
  --env OUTPUT_URI=s3://alice-results/job-output -- python /app/train.py
```

Nodes refuse batch mounts that overlap Fleet state or device, kernel, runtime or
container-daemon directories, and direct socket, FIFO and device mounts, even under
an older accepted policy. An approved directory grants access to everything in it,
so approve only storage dedicated to the namespace.

### Jobs

`fleetctl job` lists what the scheduler queues: proofs submitted to a proof app
(`--type app`) and batch jobs (`--type batch`); `--app NAME` selects one app's or
batch's jobs. An app's own work in its replicas isn't a job, so a plain app, or a
proof app whose worker proves on its own GPU server, has none: `fleetctl app` and
`fleetctl app logs APP` show that work. A job takes its workload's namespace, tag and
priority, and its dependencies, logs and changes check both namespace and tag.

### Priority and preemption

A job takes its app's or batch's priority when it's submitted (default 0, higher
first) and keeps it through retries, preemption and restarts. A submission can't set
its own, and a workload's new priority applies to new jobs only. Equal priorities run
in submission order (creation time, then ID). Priority doesn't bypass quotas, grants,
dependencies, placement or anchoring, and a blocked job can be skipped so that work
that fits goes ahead.

A waiting batch job or a starting app may preempt batch jobs of its own namespace
that have a lower priority, belong to a batch that allows it, and have run for their
`min_runtime_s`. The scheduler takes the lowest priorities first, and plans enough
capacity and namespace and tag quota before it stops anything. Equal and higher
priorities, other namespaces, active proofs and running apps are never preempted;
proof priority only orders queued proofs. Every priority defaults to 0, so preemption
needs priorities set explicitly.

A stop sends SIGTERM, then SIGKILL after `stop_timeout_s`; disabling or deleting an
app waits that long for its containers. Reservations last until
every container is confirmed stopped or gone; an unreachable node keeps them, and the
requeue survives a scheduler restart. A preempted job queues again with its ID,
output and priority, without spending a retry.

Ray provides the placement groups and actor reservations, in a local Ray namespace on
each machine. Fleet enforces namespaces, tags, quotas and preemption, which a Ray
namespace alone doesn't.

## Proofs

### SDK protocol

SDK-driven apps keep the `GenerateProof`/`GetStatus` protocol:

1. The client submits to its app's own endpoint, which stores the inputs and sends
   Fleet only their references, SHA-256 hashes and job parameters.
2. Fleet admits the job and assigns it through node heartbeats. The proof actor reads
   the inputs from app storage and proves them on its own GPU server.
3. The node writes the outputs to app storage and reports their references, hashes,
   cycles and `prove_ms`. Fleet records the storage machine from the authenticated
   node.
4. The client polls its endpoint, which checks the completion references and reads
   the proof and public values locally.

Each proof replica owns one GPU server and runs one proof at a time; a proof never
spans servers. Every eligible replica and the app endpoint must reach the same app
storage path: references are `file://`, with owner-managed shared mounts for
replicas on different machines. Fleet keeps admission, quotas, placement, retries and
execution leases, and stores no proof inputs or outputs.

The generic `fleet-ziren-node` SDK and CUDA client follow Ziren's `main` branch
in the manifest. `Cargo.lock` fixes both at the validated Ziren 2.0 revision
`c9988a098d92bc55a85ca9307c470d1f97be3337`, matching GPU image
`ziren-gpu:c9988a0-8f3c7fb6`. Pin the guest and its receipt verifier to that same
revision. The circuit label `v2.0.0` alone does not establish CUDA wire compatibility:
older 2.0 clients can fail decoding the newer server's compressed proof. Upgrade
the client binary with the image; do not mix their proof representations. CI
compiles the full SDK/CUDA adapter as well as testing storage without the runtime.
Use locked builds. Dependency updates must revalidate this tuple; regenerating
the lockfile is an intentional upgrade, including its Plonky3 dependencies.
SDK proof apps package the worker and GPU server into one immutable image and
select `runtime: {executor: shim, driver: docker}` in their `apps.yaml`
entry. They retain `kind: ziren-gpu` or `ziren-cpu`, their app/ELF identity and the
existing proof submission, queue, quota and completion protocol. Execution uses the
same durable shim Docker lifecycle as other containers. The bridge advertises
`shim-prover-v1`; older adapters cannot receive these launches. OpenShell is
not supported for this packaged SDK worker path.
It exposes only its private HTTP health port; SDK ingress and artifact retrieval
continue through the existing proof-app endpoint and storage protocol.
A prover's node entry can set `network: host`: its one replica then runs on the
host's network and listens on `container_port` itself, with `port_base: 0`. The
node advertises `shim-host-network-v1` for it. An app or a batch has no such setting.

`deploy/build-proof-image.sh OUTPUT_IMAGE GPU_IMAGE@sha256:DIGEST` builds the locked
SDK worker and puts it with the operator-validated GPU base in one image. The build
record includes the SDK revision, base image digest and worker SHA-256. The Docker
context contains only the binary, launcher, Dockerfile and build record. The script
does not start a prover or publish an image. Supply the registry digest of the
compatible GPU build; a local image tag alone is insufficient. Deploy the resulting
complete worker image by digest.
Tini owns process reaping and signals; a failed worker or GPU server ends the
container. The optional worker `/healthz` endpoint remains healthy during an active
proof and is published only on node loopback through shim.

The manifest explicitly supplies environment and bind mounts. Each volume is
`PATH[:ro|rw|rslave]`, mounted at the same path inside the container. Container templates
can reference app scalar fields and `{name}`, `{machine}`, `{index}`, `{port}`,
`{gpus}` (container GPU ordinals), `{gpu_count}`, `{cpu_count}`, `{scheduler}`,
`{cluster_token_file}` and `{scheduler_ca_file}`. Credential contents are never
injected by Fleet; an administrator must approve the individual read-only mounts.
Keep proof storage mounted at the same absolute path so persisted `file://`
references and completed lease records remain usable. The host storage
fetch handler fetches historical outputs.

Guest/verifier compatibility still needs validation for the pinned image tuple;
an immutable image alone does not attest to hardware or prove compatibility.
Check `fleet_bridge.__file__` from the adapter's Python interpreter and working
directory when deploying: a source tree in that directory takes precedence over
an installed wheel, so updating the wheel alone may leave the running code old.

Every app replica and batch job runs through trusted-shim. The node-owned
`apps.yaml` sets `shim_binary` (the absolute `trusted-shim-runtime` executable path)
and `shim_state_dir` (an absolute private persistent directory). Upgrade the scheduler and `fleet-node` before the Python bridge: node report
v3 gains the optional `runtime_capabilities` field, with no database migration.
An older node advertises none and cannot receive a shim launch or acknowledge its
cleanup. A supported node advertises `shim-docker-v1` after probing the binary.

Every `apps.yaml` entry selects its runtime, `runtime: {executor: shim, driver: DRIVER}`
with driver `docker`, `openshell`, `tee` or `tee-simulation`; the bridge refuses a file
with an entry that selects none. An entry the scheduler creates runs on the Docker
driver unless its spec selects another, and a prover's serves its health check on
`container_port: 8080` behind a loopback port the shim picks (`port_base: 0`). Use the app settings API to change an app's
driver; its normal drain, stop, confirmed cleanup and apply sequence runs first.
Selection changes require administrator credentials.

The machine adapter persists the selected binary, request binding, node, allocation,
lease and generation under its existing jobs state before dispatching to Ray.
Recovery keeps that identity; cleanup follows the persisted record even if
configuration changes. Directly
editing a live app's runtime cannot replace its actor. An uncertain shim launch or
cleanup retains its reservation and never falls back to Docker directly. A stopped
or lost shim execution needs explicit cleanup and a new generation to run again.
With the persistent shim (`runtime-resume-v1`), a Docker runtime whose container
exited is started again in place: by a health probe, and when its actor starts
again after a Ray restart or a reboot. A TEE runtime is started again the same way when the
shim advertises `runtime-resume-tee-v1`, only behind a fresh attestation admission
with new evidence and its own proof. An admission can take 300 s: meanwhile the
replica reports `starting`, and the daemon continues it if the bridge stops
waiting. Its retries back off from 5 min, doubling up to an hour.
Keep the allocation and shim journals across upgrades and restarts.

Shim delegates lifecycle to upstream Docker and checks a container's effective
CPU/memory limits against the launch request before start. For a Docker runtime
Ray reserves its `cpus` and `memory_gb`, and the
request sets no container limit; TEE runtimes set both as hard limits. On either
driver, an entry's `cpu_limit` and `memory_limit_gb` are the container's hard
limits in place of those, and a replica with `cpus: 0` also gets
`limits.cpu_shares` 64; an entry without them sends the launch request it always
has. Each
allocation records its launch request revision, and one recorded without a
revision keeps the hard limits and environment it was launched with until it is
cleaned up and launched as a new generation; from revision 2, `{cpu_count}` is
`max(1, int(cpus))`: 1 for a replica that
reserves 0 or a fraction of a CPU, whatever its `cpu_limit`, since a cap isn't a
reservation. The shim creates
containers with `--pull=never`. When a first Docker launch fails and the node lacks
the image, the bridge pulls it, within an hour, and makes the same launch again;
a failed pull fails the launch naming the image. A launch that finds its container,
as after a Ray restart or a reboot, needs no image check or registry. The app's
`ready_timeout_s` starts after the launch.
A Docker `kind: app` entry with GPUs gets `NVIDIA_DRIVER_CAPABILITIES=all` unless
its `env` sets it. The container's stdout and stderr go
to `<log_dir>/<replica>.node.log`, which `fleetctl app logs` reads: a thread of the
replica's actor streams them from Docker's API. `<replica>.node.log.cursor` beside
the log holds the time of the last line written, so a resumed container or a
restarted actor continues after it; after a crash, up to a second of output can
appear twice. The follower's own errors go to the bridge log, and repeated
failures retry after a backoff of 30 s doubling to 15 min. Its service
port is published only on local
loopback; declared services use the independent router through outbound node
connectors. GPU ids are Ray's assigned ids. The Docker profile is explicitly non-TEE
and makes no shell-policy enforcement claim; it rejects attestation requirements.
OpenShell access is always enabled across drivers; absent user rules add no
restrictions. A configured `access_policy_sha256` binds user rules to the native
provisioning gate. TEE profiles below require synchronous
remote-attestation receipts. Application action admission remains a separate gate.

The bundled companion includes OpenShell and Ziren receipt verification support. Operators configure its
native enforcement endpoint in the **same** `apps.yaml`, alongside `shim_daemon`:

```yaml
shim_daemon: true
shim_openshell:
  socket: /run/openshell/compute.sock
  admission_policy: '<acknowledgement from the native driver configuration>'
  ledger_endpoint: https://fleet.example
  # Optional private CA; absolute, regular, not group/world writable.
  ledger_ca_file: /etc/zkm-fleet/ca.pem
  # Optional local service adapter: exact local ID of the composed supervisor.
  # supervisor_image: sha256:<64 lowercase hex characters>
```

These are operator settings, not allocation credentials. The native driver remains
an unmodified OpenShell process with a same-UID private socket. Node bootstrap
compares the running daemon's configuration digest with these settings; changing
or removing them requires explicit companion reconciliation. A node upgrade never
restarts the companion to apply a different policy gateway or driver. Existing
Docker apps and their persisted execution selection remain unchanged.

The daemon's private `provision` operation accepts exact native gateway bytes and
a proof-backed grant, consumes the grant centrally before Create, and keeps only
ownership/digests and admitted public service metadata on disk. Native inspection/cleanup survives gateway outages and
expired credentials. An app with a user access-policy commitment reserves Ray resources and a durable
shim allocation before native provisioning, while retaining its `docker`, `tee`
or `tee-simulation` driver. The control plane supplies `access_policy_sha256`
from the selected OpenShell rules; there is no enable toggle. Omit it when the
user has no rules: effective permissions then remain exactly the driver boundary.
Legacy `driver: openshell` records remain readable for recovery. A rule change
requires confirmed cleanup and a new generation; an upgrade never migrates one.
This profile requires a whole `gpus` count (0 to 16), positive integer `cpus`, and
`memory_gb` (at least 6 MiB); command/environment are literal. Optional HTTP app
services declare `services`, one `container_port` and `health_path` (default `/`).
These ports are private to the workload; `port_base`, host `volumes`, entrypoint,
workdir and user overrides remain unsupported.

**Ingress and logs.** An HTTP application service may have an `ingress` rule
(`paths`, `methods`, optional `single_use`), as on shim Docker, but never
`bearer_env`: a native node holds no copy of the application's credential, so the
shim forwards the caller's own `Authorization` header and the application checks
it. `services` may also name `control` beside at least one application service
(TCP ones included) for `GET /logs?tail=N` with Docker's bounds, read on the node
from the owned workload container. Native control has no exec, restart or
metadata routes; see administrative exec and lifecycle below. Placement requires
`shim-openshell-ingress-v1`. A launch request must carry exactly the admitted
rules in `driver_options.ingress`, endpoint `ingress:<service>` for a service with
a rule, `app` otherwise, and `control` (always `http1`) for control.

**GPUs and durable storage.** With `gpus` above zero, the reservation actor reports
the device ids Ray granted it as `RuntimeInstance.gpu_ids`; a report whose ids are
not exactly that many distinct ids is dropped. Durable storage is `mounts`, a list
of up to 16 `{name, target, read_only}` entries (`name` lowercase letters, digits
and `-`; `target` an absolute container path). Each becomes a node-local named
Docker volume derived from the namespace, workload and `name`: stable across
generations, unnameable by another workload, and never removed by runtime
cleanup. Both stay out of TEE drivers. Placement requires
`shim-openshell-attachments-v1`, which a companion advertises only when its
driver's acknowledged admission policy has `allow_driver_config: true`; the
scheduler's `admission_policy` acknowledgement must then carry the same setting,
or preparation is refused. The launch request carries `gpus` and `mounts`: Fleet
requires `gpus` to equal the owning node's report and `mounts` to equal the
admitted manifest. The shim translates them into the stock driver's own
`driver_config` (`nvidia.com/gpu=<id>` CDI devices and `type: volume` mounts,
plus an exact `gpu.count`) and rejects any native request that differs. It creates
each missing volume with the policy's required labels before dispatch and refuses
existing volumes that upstream admission would not accept. The native driver must
run with `--admission-config-json` allowing driver config, and with CDI enabled on
its Docker daemon for GPUs. The gateway's `[openshell.drivers.docker]` section
needs the same `allow_driver_config = true`, or it refuses the driver at startup.
Neither the shim nor the driver changes ownership: Docker gives a fresh volume
the owner and mode of the image's directory at `target`, or `root:root 0755`
when the image has none. An image whose policy user must write a mount has to
create that directory with that owner.
Only a companion advertising `openshell-allocation-v1` exposes its placement
capability. Native reservations remain `ready: false` with no services; a complete
reservation set holds quota without repeatedly scheduling another start. With
services, placement instead requires `shim-openshell-services-v1`, advertised only
by a companion with local health reporting and configured `shim_network`.

The private shim adapter additionally supports proof-bound HTTP service declarations
and `network` commands with the actual runtime driver. Fleet checks their service IDs,
container port and health path against the admitted manifest; the daemon stores
those targets and the native bundle's public SSH fingerprint. Later network
commands cannot change them. The operator's immutable `supervisor_image` enables
`openshell-local-services-v1` and `openshell-service-health-v1`. Existing Docker
network records remain compatible.

Local health gates registration and unhealthy applications lose active streams.
The daemon retries health recovery, restores connectors after a companion restart,
and drains them before acknowledging runtime deletion. Shim `inspect` reports
`application_ready` from a successful probe within six seconds; this state is
cleared on failure, cancellation or restart and does not wait for router grants.
The bridge retains sanitized compute observations alongside local service health.
Fleet publishes only after joining that health with authenticated gateway
configuration readiness. The native supervisor health socket reports session
acceptance before asynchronous policy activation and cannot replace this check.
A node report claiming `ready: true` alone cannot publish a native service.

The scheduler embeds the stock OpenShell compute extension. Enable it in the
existing scheduler configuration, under `policy_gateway` alongside its approved
program and remote `preflight` proof-network settings:

```yaml
policy_gateway:
  # Existing program_elf, program_sha256, program_vkey and preflight settings.
  openshell:
    compute_socket: /run/zkm-fleet/openshell/compute.sock
    admission_policy: '<exact native driver resource-admission acknowledgement>'
    # Optional: required to publish native application services.
    gateway:
      endpoint: https://openshell-gateway:17670
      ca_file: /etc/zkm-fleet/openshell-ca.pem
      cert_file: /etc/zkm-fleet/openshell-client.pem
      key_file: /etc/zkm-fleet/openshell-client.key
```

The socket directory must already exist, owned by the scheduler/gateway UID with
mode 0700; in Compose, `fleet-ctl.sh openshell up` prepares it. Fleet creates a 0600
socket and accepts only that UID. Configure the unmodified gateway with
`--compute-driver docker --compute-driver-socket` pointing to this socket. Fleet's
optional `openshell` [Compose profile](installation.md#openshell-gateway) deploys the
unchanged gateway beside the scheduler. Operators deploy the node's
native compute driver separately; the scheduler does not launch either process.
The admission acknowledgement must match
the node's `shim_openshell` settings. This profile supports CPU/memory
limits, exact-count GPUs, durable volumes, and private HTTP or banner-checked TCP app routes. It
advertises GPU count selection only (no driver-chosen default devices) and no gateway-managed lifecycle.

For native SSH services, set `service_protocol: tcp`, `container_port: 22`,
`health_path: /` and `health_tcp_banner: "SSH-2.0-"` in the existing app spec.
The launch request must carry the same banner, port and protocol. The banner is
a nonempty printable ASCII prefix of at most 128 bytes; shim must receive it
within three seconds before reporting health. A tunnel ACK alone is insufficient.
Fleet requires `shim-openshell-tcp-services-v1`; old HTTP-only nodes cannot accept
these workloads. TCP protocols that do not send a banner remain unsupported in
this native profile. No additional node port is exposed.

The scheduler's observer calls the stock API over mTLS; the key must be a
same-UID 0600 regular file and certificates must not be group/world writable.
All paths are absolute and symlinks are rejected. The HTTPS origin must match
the server certificate. These credentials remain in the central scheduler.
Omitting `gateway` preserves provisioning and leaves native publication closed.

The observer joins the exact public sandbox/workspace with the Fleet Compute
extension's persisted Create identity and fresh owned native observation. Public
compute fields may be absent. It requires Ready/configuration acceptance, the approved native policy hash and
loaded latest policy revision. It compares accepted configuration/provider
revisions with the current desired configuration. Configuration instance and
supervisor session IDs are separate upstream identities. Queries have a two-second
total deadline per sandbox, with at most 16 in flight. Successful observations
expire five seconds after query start and are never persisted; failure removes
cached readiness. Publication also rechecks node session freshness, live intent,
quota, allocation generation, physical instance and active Rail policy.

This gates new route grants and renewals. The updated embedded Router gateway
issues 30-second node registration leases, counted from the start of each Fleet
readiness check. Shim renews in place; failed renewal cannot extend the lease.
Expiry drains existing HTTP/SSE/WebSocket streams even if their application
Access tokens remain valid (up to 900 seconds). With the five-second observation
cache, native readiness loss drains in about 35 seconds plus timer/clock skew;
gateway and relay clocks must be synchronized. Explicit route revocation,
control-lease expiry, local health loss and deletion can drain sooner. Transient
readiness loss permits reconnection to the same execution after recovery without
another proof/Create. Upgrade embedded gateways before depending on this bound:
previously issued long leases retain their original signed expiry, including
through a shorter renewal. This does not migrate or restart existing workloads.

Fleet namespace IDs exceeding OpenShell's 19-byte routable-name limit map to
stable native workspace names. Full canonical identities remain in authorization
and proof commitments; conflicting mappings are rejected. Explicit nonempty
commands are required at launch preparation: an omitted command makes the stock
gateway request an interactive TTY, which this profile excludes.

The startup flow is synchronous through proof verification and native creation:

1. Admit the App, wait for its reserved execution identity, and activate its policy
   using Fleet's existing policy APIs. The backend resolves its user's identity;
   these APIs accept Fleet administrators, not application API keys.
2. `POST /policy/openshell/launches` with `request` (the exact admitted
   `LaunchRequest`), `native_policy_base64` (stock `SandboxPolicy` protobuf),
   `principal`, `cost`, and `expires_at` (Unix seconds, at most 15 minutes ahead).
   Omit `gpus`: Fleet fills the owning node's reported grant before translating, so
   callers never handle node-local device ids. An explicit list must match it.
   The response contains a deterministic `name`, mapped `workspace`, and
   `create_request_base64` (stock
   public `CreateSandboxRequest` protobuf). It is private and `no-store`; workload
   environment may contain secrets. An identical preparation is idempotent.
3. The control-plane caller submits that decoded request to the authenticated stock
   gateway, in the returned `workspace` (create it through the stock API first). The gateway calls Fleet's
   validation extension before minting credentials, then calls Create with its
   exact private native bytes. Fleet binds those bytes to the allocation and policy.
4. Fleet stages `runtime.create` at its central ledger, submits the pinned guest to
   the configured remote proof app, waits for the receipt, and verifies it against
   the exact decision and active policy epoch. Only then does it issue a single-use
   grant and stage the existing private delivery. No local prover is started.
5. Outbound heartbeats carry only a ticket, execution identity and digest. The node
   claims the private body once, then forwards it directly to the same-UID shim
   socket. The shim repeats native binding checks, consumes the grant centrally,
   and calls the native compute driver. Claim/Consume recheck the current node
   session, generation, deployment intent and quota.
6. Shim inspection returns a bounded native observation stripped of configuration,
   credentials, local endpoint addresses and condition messages. Bridge heartbeats
   carry that control metadata while retaining `ready: false` and no services.
   Fleet returns only fresh, allocation-owned observations to the stock gateway;
   an observation cannot bypass proof/delivery, cross workspaces, or replace a
   pinned native instance. The gateway still owns supervisor/fence readiness.

`GET /policy/openshell/launches/<name>` exposes phase, execution, sandbox ID and
proof job ID, never credentials or launch inputs. Phases are `prepared`, `bound`,
`proving`, `dispatching`, `delivered`, and `created`; `created` acknowledges native
creation, not application readiness. Four creates may run concurrently. A callback
with different private bytes is rejected. A disconnected RPC does not cancel an
accepted create; inspect the same operation before retrying.

**Exec.** `POST /policy/openshell/launches/<name>/exec` takes the
same body as shim's Docker `control` service, `{command, user, stdin,
timeout_seconds}`, and returns `{stdout, stderr, exit_code}` (stdin/stdout/stderr
base64). It needs the scheduler's `gateway` settings and runs through the stock
gateway's interactive exec: the sandbox supervisor runs the command under the
approved native process policy, so `user` may only repeat that policy's
`run_as_user`. Fleet never executes on the node. Each call is accepted only for a
`created` launch whose owning node session, generation, deployment intent and
quota are current; losing any of them closes control at once. Bounds: at most 256
arguments and 256 KiB assembled command, 1 to 600 seconds, 64 MiB stdin (sent in
512 KiB frames, under the gateway's 1 MiB message limit) and 64 MiB per output
stream; four execs per scheduler, an administrator's and owners' together. Every call gets a fresh upstream `request_id`,
so a transport retry never relaunches a command. A stream that ends without an
exit status reports unavailable, not success.

An app's owner runs a command with `POST /apps/NAME/exec` and a namespace token
with `manage`: the same body, bounds and conditions, in the app's one replica or
the one `?replica=ALLOCATION` names (`GET /apps/NAME/policy` lists them). The
replica's activated policy decides each call. Rail evaluates it at the ledger as
operation `exec` on resource `ALLOCATION`, for the principal the replica was
launched for, with `parameters` `{command, user, stdin_sha256, timeout_seconds}`.
An `allow` draws 1 from the launch's budget, its grant is consumed, and then the
command runs; the reply adds `decision` with the decision's `job_id`, `policy`
and `reason_codes`, and the decision's proof follows under that job, as for
`POST /policy/preflight`. Any other decision answers 403 with `decision` and
runs nothing; a spent budget answers 409.

**Approvals.** When the policy answers `approval_required`, the 403 also carries
`approval` `{id, expires_at}`: a request to approve that exact exec in that
replica generation, open for 24 hours. An app holds at most 32 open requests;
beyond them the answer carries no `approval`.

- `GET /apps/NAME/approvals` lists the app's requests with their `parameters`
  (the command, user, stdin hash and timeout), `principal`, `reason_codes` and
  `state`: `pending`, `approved`, `refused`, `used` or `expired`.
- `POST /apps/NAME/approvals/ID` with `{"approver": "NAME", "approve": true}`
  decides a pending request, once; `false` refuses it. It takes a token with
  `manage` on the app, or an administrator's.
- `POST /apps/NAME/exec?approval=ID` with the same body sends the exec again.

An approval changes no decision. The policy evaluates the exec afresh, with
`parameters.approval` `{id, approver, approved_at, independent}`, and decides
what the approval is worth; `independent` is true when the approval came with
another credential than the one that sent the exec. An allow spends the approval
and runs the command; a policy that still does not allow answers 403 as before.
`approver` is the name the approving caller gives, as `principal` is the name the
owner gives.

Approvals cover execs. A launch starts only on an `allow` for `runtime.create`,
and no approval request is opened for any other answer.

**An app's policy.** An app's owner can hand that whole flow to the scheduler:
`PUT /apps/NAME/policy` (or `policy` in the app's `POST /apps`) stores what the
scheduler launches each of the app's replicas with, and takes the owner's own
namespace token:

| Field | Meaning |
| --- | --- |
| `policy` | The Rail policy bundle, with its digest in `policy.sha256` |
| `context` | The authoritative context object the policy is evaluated with |
| `native_policy_base64` | The stock `SandboxPolicy` protobuf the sandbox runs under |
| `principal` | Whom the owner starts the app for; it is the request's principal |
| `cost` | What each start draws from the namespace's policy budget; at least 1 |
| `state_root` | Optional 64-hex state root; all zeros when omitted |

For every reservation a node reports for the app, and again for each new
generation (a restart, a reschedule, a settings update), the scheduler activates
the policy for that exact execution, prepares the launch from the entry Fleet
admitted and the devices Ray granted, and submits the create to the stock gateway
in the namespace's workspace. Steps 4 to 6 above then run as they do for a
caller's own submission, with one proof per generation. An attempt that fails is
tried again, after 2 s doubling to 60 s, for as long as the node reports the
reservation.

- The launches draw from the budget `namespace:<namespace ID>`. An administrator
  sets its allowance with `PUT /policy/budgets`; until then the ledger takes no
  activation and nothing is submitted.
- It needs the scheduler's `gateway` settings, and an entry on the `openshell`
  driver or one with `access_policy_sha256` on the `docker` driver. That hash
  names the native rules the entry admits: `POST /apps` with a `policy` fills it
  in from the policy's, and a policy with other native rules is refused. A TEE
  driver's launch comes from a caller that knows the node's attestation profile.
- A policy applies to the launches prepared after it is set. A replica that is
  already launched keeps the one it started with until its next generation;
  `POST /apps/NAME/restart` starts one.
- `GET /apps/NAME/policy` returns the policy's reference (not its modules, data
  or context), the native rules' hash, `principal`, `cost`, the budget's name and
  each reported replica's `phase` (`pending` before its first preparation) with
  the last attempt's `error`. `DELETE /apps/NAME/policy` removes it, and deleting
  the app does too. Responses are `no-store`.

Native lifecycle stays with Fleet allocations: the compute extension refuses
gateway stop/start, so "restart" is a replacement generation, and durable `mounts`
carry state across it. Gateway sandbox logs are supervisor events only; the
application's own stdout/stderr stay on its node, readable through a declared
`control` service rather than this API.

Persist `data_dir/policy/openshell`, the policy ledger and allocation/delivery
journals together. A durable proof claim precedes submission; after an ambiguous
submit, only the same job ID is polled. A crash between claiming and submitting
requires operator reconciliation. Before minting a grant/delivery, a durable
`dispatching` fence prevents reminting credentials after a crash. An ambiguous
`dispatching` operation never reports success from a node observation alone.
Expired/failed operations need reconciliation; they are not automatically recreated.

The lower-level administrative `PUT /policy/runtime-deliveries/<64-hex-ticket>`
remains available for exact `ProvisionRequest` delivery. Its 4 MiB private bodies
stay in memory; `workloads/runtime-deliveries` stores metadata only. Queued records
become `awaiting_payload` after restart and require the identical body/ticket while
its grant is valid. Dispatched records are never automatically redelivered. Inspect
the exact shim allocation for an uncertain result. `DELETE` cancels only a queued
delivery; stop/delete of a running allocation belongs to Fleet's lifecycle API,
not the stock gateway's lifecycle RPCs in this profile.

This retains Fleet's trusted cluster-agent/session model; it is not TEE attestation
or individual cryptographic node enrollment. Production links use HTTPS.
Application readiness and service routing remain migration gates; native gateway
readiness alone does not enable application routes.
Never place native payloads, grants or ledger credentials in App specs, heartbeats,
logs or Ray actors. GOAT registration execution remains TODO.

Remote runtime nodes need no public inbound listener. Fleet node initiates HTTPS
to the scheduler, the native supervisor initiates its authenticated connection to
the central OpenShell gateway, and the shim data-plane connector initiates the
router tunnel. Bidirectional application traffic uses that established tunnel;
it is not carried by heartbeats. Shim and compute-driver APIs remain local Unix
sockets. Central services must be reachable from the node's outbound network.
Set the native Docker driver's `grpc_endpoint` and trusted `guest_tls_ca` to the
central gateway; a remote node must not use the gateway host's loopback address.
The local acceptance below uses supervisor host networking to reach a loopback
test gateway. Workloads still use `network=none`. This test does not certify a
cross-machine deployment or a firewall/NAT configuration.

The pinned router transport keeps idle outbound WSS connections alive with
15-second Ping/Pong controls and a 15-second reply deadline. Silent network loss
drains application streams within about 30 seconds plus scheduling delay;
recovery never replays requests. Dedicated ingress idle timeouts must allow this
interval; configure at least 60 seconds for operational margin. No node inbound
listener is needed.

Router's `tools/test_nat.py` covers two isolated stateful NATs, overlapping private
addresses, TCP/443-only egress, blocked inbound traffic, TLS validation, tenant
and app-key isolation, idle continuity, network blackholes and recovery.
`tools/test_remote.py` additionally exercises a real remote connector through
temporary public TLS ingress, with SSH used only for fixture setup/cleanup.
These transport tests use synthetic grants and loopback applications; they do
not replace cross-machine Fleet/OpenShell control or real Film/ClawUp acceptance.

The opt-in control integration test launches the pinned, unmodified gateway with
mTLS and exercises actual Fleet HTTP/UDS, node delivery and the persistent shim.
Proof and native compute are explicit test doubles; it does not establish workload
isolation or application readiness:

```sh
OPENSHELL_GATEWAY_BINARY=/absolute/path/to/pinned/openshell-gateway \
  cargo test --locked -p fleet-scheduler --lib stock_gateway_startup -- --ignored
```

The Docker acceptance additionally launches the stock Docker driver and real
workload/supervisor containers. Build both binaries from the pinned OpenShell
source and supply compatible, preloaded images by immutable ID. Compose the
supervisor with trusted-shim's `docker/openshell/Dockerfile.supervisor` and static
`trusted-shim-socket` helper; the upstream supervisor and driver remain unchanged.
The workload image must provide Python 3's standard library and numeric UID/GID
1000 support (the acceptance uses `python:3.12-slim`):

```sh
OPENSHELL_GATEWAY_BINARY=/absolute/path/to/pinned/openshell-gateway \
OPENSHELL_DOCKER_DRIVER_BINARY=/absolute/path/to/pinned/openshell-driver-docker \
OPENSHELL_TEST_SANDBOX_IMAGE=sha256:... \
OPENSHELL_TEST_SUPERVISOR_IMAGE=sha256:... \
OPENSHELL_TEST_WORKLOAD_IMAGE=sha256:... \
  cargo test --locked -p fleet-scheduler --lib stock_docker_startup -- --ignored --nocapture
```

The test checks proof-before-create, joined gateway readiness, configuration
activation, hard CPU/memory limits, network isolation, stale-owner rejection and
offline shim cleanup. It also runs a real local Data Router and outgoing connector
against the supervisor's local SSH boundary adapter: HTTP application credentials
are preserved, SSE streams and WebSocket text/binary frames pass, foreign tenants,
wrong supervisor images and wrong SSH host keys are rejected. Revocation and
local cancellation close active streams. Application bytes continue locally with
the central OpenShell gateway offline, within the test's route lease. Test-issued
route grants do not replace production Fleet control-snapshot freshness. A second route exercises the
persistent daemon against the same real workload: unhealthy startup and central
denial prevent registration, 30-second leases renew without interrupting active
SSE/WebSocket streams, and central observer expiry drains them while local health
remains good. Readiness recovery reopens the same route without replacing its
containers. Health loss drains an active SSE stream, recovery
needs no Ray/client restart, companion restart restores the exact allocation
without another proof/Create, and deletion drains streams and removes connector
records. The durable record retains the public host fingerprint and excludes
native credentials/environment.
The workload has no published host ports and retains `network=none`.
It keeps node observations flowing until both native
creation and gateway readiness complete: the stock driver can acknowledge Create
before assigning a physical instance. Proof submission/verification remain test
doubles; no prover is started and this is not a fresh cryptographic proof test.
Containers and volumes are scoped by a unique test namespace. Private diagnostics
and ephemeral certificates remain in the printed temporary directory, outside the
repository. Cleanup targets only that namespace, including after test failure.

`stock_docker_ingress_logs_exec_and_durable_volume` runs the same stack with a
permissive admission policy and declares `chat` (single-use ingress) and
`control`. Through the real router, it checks that a bound request reaches the
app once, with the caller's own `Authorization`, and that replays, unbound
grants, other bodies, paths and methods never do. Control returns the app's
stdout and has no metadata, exec or restart routes. Administrative exec goes
through the stock gateway as UID 1000, including multi-megabyte stdin, and a
caller-chosen `root` is refused. Data the exec writes to the durable volume can
still be read after the sandbox is deleted. The shim-created volume is removed
when the test ends.

For local acceptance, build the companion `trusted-shim-runtime`, preload
`python:3.12-slim`, then run from this repository:

```sh
PYTHONPATH=bridge TRUSTED_SHIM_TEST_BINARY=/absolute/path/trusted-shim-runtime \
  python -m pytest -q -s bridge/tests/test_shim_cluster.py
```

The test starts an isolated local Ray cluster, runs a shim app,
checks container ids/start times across adapter recovery, verifies that the
container has no Docker CPU/memory limit and only loopback publication, and
confirms cleanup. No live Fleet node or
proof network is contacted. Without the explicit binary path this test is skipped.

EthProofs owns its whole pipeline (head polling, witness preparation,
loopback GPU proving, durable outputs and reporting) and needs no separate EthProofs
client or SDK endpoint. Its
cursor advances one block at a time, once the block's output and report are
committed.

### App-owned proof endpoint

Run an endpoint beside the app's storage, apart from its GPU replicas:

```bash
fleet-proof-app --scheduler https://scheduler.example.com:8905 \
  --namespace NAMESPACE_ID --elf-id FULL_ELF_SHA256 \
  --storage-root /srv/goat --elf-file /srv/goat/program.elf \
  --token-file /etc/my-app/user.token \
  --stage-addr 127.0.0.1:51012 --http-addr 127.0.0.1:51013
```

Set `proof_storage_root: /srv/goat` on each matching `ziren-gpu` prover, in its app's
spec or its `apps.yaml` entry, and point the SDK's `ENDPOINT` at the stage port,
`http://127.0.0.1:51012`.

- **Tokens.** The SDK keeps its signing key. An older SDK that sends no bearer header
  uses the endpoint's `--token-file`, which is reread on every request, so rotation
  and revocation still apply. A request's own authorization is checked as given,
  never replaced by the configured token.
- **Programs.** Repeat `--elf-id` and `--elf-file` for more programs in the
  namespace. Each is its own Fleet app: the endpoint finds its name through
  `GET /apps` with the caller's token and submits to `/apps/NAME/jobs`.
- **One per app.** Give each SDK-driven app its own storage root, scoped token and
  pair of loopback ports. GOAT uses `51012` and `51013`.
- **Results.** A successful status call reads the proof and public values from the
  app's local output storage, checking their size, SHA-256 hash and that they're
  inside the output directory, with no fetch through Fleet. The prover wakes its
  control loop as soon as a proof finishes, so completion doesn't wait for a
  heartbeat.
- **Network.** The endpoint binds loopback only; outside access needs an
  authenticated HTTPS gateway. Pass `--scheduler-ca-file ca.pem` for an
  operator-issued CA. Fleet CLI and MCP clients keep the control-plane URL.
- **Logs.** It logs input storage and admission time separately from the node's
  `prove_ms`. Changing the endpoint doesn't enable a disabled app.

On the node, give the agent `--proof-node-binary /path/to/fleet-ziren-node` and a
`--proof-storage-root /srv/goat` for each approved root. With `deploy/node-start.sh`,
set `FLEET_PROOF_STORAGE_ROOTS_FILE` to an administrator-owned file with one absolute
root per line; `deploy/install.sh node --proof-storage-roots FILE` installs it.
`fleet-ziren-node` isn't in the release package; build it from source
([self-built programs](maintenance.md#self-built-node-programs)). Keep
the roots configured while GPU actors are stopped. The agent runs storage-only
fetches apart from its heartbeats and Ray work.

### Fetching proofs

`fleetctl job fetch JOB_ID --out DIR` downloads a proof's JSON and public values into
a new directory. Fleet authorizes the request, hands the storage node a short-lived
ticket in its next heartbeat, and relays the node's HTTPS upload in bounded memory;
the control host keeps no proof files. `fleet-ziren-node fetch-artifact` checks the
files against the job, its execution lease and their hashes, without starting a
prover or GPU. A fetch waits for a heartbeat, and an offline storage node returns a
retryable error. Proofs stay fetchable after their prover is disabled; the app owner
decides how long to keep them.

### Proving time

`prove_ms` is execution time. For a GPU proof it comes from the GPU server and leaves
out input and output transfers, queueing, setup and fetching; CPU and mock provers
time the local proof call. Wall time is kept separately for diagnostics and isn't
proving time. A GPU server that doesn't report execution time fails the job rather
than fall back to RPC time. The SDK returns the value in `GetStatus.proving_time`,
and reporting clients must keep it, for example with `take_prove_ms()`. EthProofs
takes it from its GPU response, leaving out preparation, queueing and reporting.

### Proof jobs

A proof job comes from a namespace token (`Authorization: Bearer`) with `submit`, and
is signed by that token's key: the signature must recover to the token's address. The
scheduler finds the job's app by namespace and ELF ID. `GetStatus` needs a token of
that namespace with `read` or `submit`, and checks the app grant before it waits or
shows anything; the SDK can still poll a refusal of its own signer, including one for
an app it wasn't granted.

The SDK keeps its own proof ID. The scheduler maps `(namespace_id, tag,
client_proof_id)` to a unique internal ID, a SHA-256 of the three for new jobs, which
nodes, retries and deduplication use; different tags can reuse a client ID.
Resubmitting within a tag returns the existing job without replacing its input, and a
resubmission with different inputs or parameters is refused.

| Field | Meaning |
|---|---|
| `id` | Internal execution and storage identifier |
| `owner` | Internal proof signer audit identifier, kept when the token is refreshed |
| `namespace` | Immutable namespace ID, for accounting and access |
| `tag` | `key=value` label; `""` for untagged work |
| `client_id` | The SDK's proof ID, unique within namespace and tag |
| `app`, `elf_id`, `kind`, `created_ms` | Submission metadata, kept through retries; `app` is empty for a request refused before its app was known |

`node` and `started_ms` can be absent while a job is queued, and `finished_ms` until
it ends. Counters start at zero and the failure history empty. A refused request from
an authenticated owner keeps that identity; one without is refused without a record.

Public-values URLs carry the issuing token's ID and a key, and are bound to that
token: revoking it or suspending its namespace invalidates them, and a replacement
token can poll the job for a fresh URL.

## Namespaces, tags and tokens

**Namespaces.** A namespace is the unit of ownership, accounting and billing. An
administrator creates it with an immutable ID and a unique name such as `alice`: a
lowercase letter, then lowercase letters, digits or hyphens, at most 63 characters.
`system` and `admin` are reserved, and namespaces can't yet be renamed or deleted.
Operator-managed apps use `system`, as do a node's configured batches and app entries
unless they name another; no user token can be issued in it. Registrations need an
existing namespace: a missing one isn't created.

**Tags.** A tag is `KEY=VALUE`, such as `scope=default` or `team=research`. The key
starts with a letter, and each part has up to 63 letters, digits, dots, underscores or
hyphens. Bare tags are refused, and tags match exactly. A tag belongs to its
namespace: `scope=default` comes with each namespace, and others appear when an
administrator assigns one to a token or gives it a cap. Tokens with the same tag share
proofs and batch jobs, which other tags in the namespace can't see; untagged tokens
share the untagged work. A workload created with a tag belongs to it: only that tag's
tokens change it, and only they see or submit to a tagged batch. An administrator's
job on a tagged batch takes the batch's tag. A record's tag is `null` when absent,
except a proof record's, which is `""`. There is no group resource.

**Tokens.** A namespace token has an optional tag and some of the permissions `read`,
`submit` and `manage` (default `read` and `submit`). An infrastructure token may
instead hold the exclusive `router_sync` permission: an API token with a namespace,
no tag or app grants, and access only to its explicitly enrolled router stream.
An ordinary app token without `manage` also
needs `--app` grants, by app or batch name. Grants are explicit, and one from another
namespace authorizes nothing. An administrator's token has only `admin`, no namespace,
and may do everything. A token's namespace and tag never change: refreshing copies
them, with its permissions, grants and label, to a new key, which keeps access to
earlier work. Requests take their namespace and tag from the token: a selector must
match them, and a submission body can't set them. An administrator's request names
its namespace when it creates an app or batch. Administrators select with `--namespace NAME --tag TAG`
(`?namespace=NAME&tag=TAG`). Records keep namespace IDs, tags and the submitting
token for audit. Token IDs are opaque, and proof signer bindings aren't exposed.

Revoking a token blocks that key, and suspending a namespace blocks all of its
tokens; resuming it doesn't restore individually revoked ones. Neither cancels
accepted work: an administrator stops jobs explicitly.

### Permissions

| Action | Needs |
|---|---|
| Read the own namespace, apps, batches, jobs, logs and scaling status | `read`, within the token's scope |
| Submit proofs and batch jobs; cancel batch jobs | `submit`, within the token's scope |
| Create, remove, enable, disable, restart, prioritize, scale or update apps and batches; set a native app's policy, run a command in it and approve one | `manage`, with any app grants. Removing, enabling, disabling, restarting or prioritizing a proof app also needs the tag it was created for and the tag of every node entry running it; scaling and updating it need the tag of the entry they change. |
| Batch `allow` and `user`; app `user`, `volumes` and `proof_storage_root`; changing an app's image, environment, command, user, workdir or volumes | `admin` |
| Preempt batch jobs | `admin` |
| Namespaces, tokens, quotas, suspension, allocations, the preemption plan, nodes and status | `admin` |

`/healthz` needs no token, `/access` takes any
valid token, and `/internal/*` takes the cluster token.

### Administrator workflow

```bash
fleetctl namespace create alice --gpus 2 --cpus 16 \
  --endpoint http://127.0.0.1:51012 --out alice.env
fleetctl namespace list
fleetctl namespace get alice
# Give alice.env, alice.token and alice.ca.pem (if written) to the owner securely.

# A token for another tag, limited to one batch:
fleetctl token issue --namespace alice --tag team=research --app training \
  --endpoint http://127.0.0.1:51012 --out research.env

# Move a user to a new key, then revoke the old one once they have switched:
fleetctl token refresh OLD_TOKEN_ID --endpoint http://127.0.0.1:51012 --out alice-next.env
fleetctl token revoke OLD_TOKEN_ID

fleetctl namespace suspend alice
fleetctl namespace resume alice
```

The owner bundle is for `scope=default` (`FLEET_TAG=scope=default`) with `read`,
`submit` and `manage`. `--endpoint` is the bundle's SDK `ENDPOINT`, the owner's
[app-owned proof endpoint](#app-owned-proof-endpoint). The bundle's Fleet URL must be
the public TLS address, since `127.0.0.1` would be the user's own machine; add
`--server-ca` only when it uses a private CA. Issuing needs an existing, active
namespace, so a typo can't create an owner. `refresh` doesn't revoke the old key or
overwrite an existing bundle. The owner then creates workloads:

```bash
fleetctl --token-file alice.token --namespace alice --tag scope=default batch add training --image IMAGE
fleetctl --token-file alice.token --namespace alice --tag scope=default app add eth --spec prover.json --elf-id FULL_ELF_SHA256
```

## Quotas

A namespace quota caps the **concurrent** GPU and CPU units held by its proofs,
batch jobs and app replicas, across all its tags and tokens; a tag can have its own
cap as well, and admission must fit both. A tag cap reserves no share of the fleet,
and no quota expires, resets or counts time: work keeps its reservation for as long
as it runs.

```bash
fleetctl namespace quota alice --gpus 4 --cpus 32                  # replace both caps
fleetctl namespace quota alice --tag team=research --gpus 2 --cpus 16
fleetctl namespace get alice                                       # caps and current use
fleetctl namespace get alice --tag team=research
fleetctl namespace quota alice                                     # clear both caps
```

Each `quota` command replaces both caps: an omitted cap is unlimited, and zero allows
none (`--gpus 0 --cpus 8` permits CPU-only jobs). New namespaces start unlimited.
Fractional GPUs are supported, with each job's VRAM requirement (`gpu_mem`) for shared
cards. With a
team cap of 8 GPUs, production holding 6 and research 2, neither can admit another
GPU job.

**Batch jobs.** A job asking for more than either cap gets 422 before it's created.
Otherwise the scheduler picks a ready node with enough reported capacity, reserves
namespace and tag units and sends the launch, and the node creates the Ray placement.
While capacity or quota is short, the job waits as `waiting_quota` without a
reservation, as does a job waiting on a dependency. Once assigned, a job holds its
reservation through execution, VRAM waits and retries until its cleanup is confirmed,
even if the node disconnects before acknowledging the launch. Ending or cancelling
releases it once the actors and container are confirmed stopped or gone, which can
take a few seconds after the final status. An unreachable node or a failed Docker
inspection keeps it until cleanup succeeds, so an orphaned container can't free quota;
cancelling a job on an unreachable node doesn't release it either. Preemption releases
it, and the job needs admission again to restart.

**Proofs.** A proof is charged its prover replica's whole GPU and CPU allocation, and
stays queued until a replica fits both caps. A timed-out or disconnected proof keeps
its reservation and rejoins the same prover; it can't fail over while that execution
is uncertain. Owners can `fleetctl app scale` a prover between proofs to fit their
quota.

**Apps.** A replica reserves its configured units against its namespace and tag for
as long as it's enabled; disabling or removing it releases them once it stops. Shared
infrastructure in the `system` namespace is counted there.

Reservations are in Postgres and survive restarts; refreshing tokens doesn't touch
them. If an admission can't be recorded, new batch jobs wait. Lowering a cap lets
admitted work finish, including work recovered during an upgrade, and holds new
admissions until use fits; a waiting batch job
that no longer fits fails with a reason. Administrators see the reservations, per tag,
at `GET /namespaces/alice/allocations`.

## HTTP API

`fleetctl` and MCP use the same authenticated API, served on the scheduler's private
listener behind the HTTPS gateway. Administrators select a namespace with
`?namespace=`, and listings and job reads also take `&tag=`.

| Workload | Routes |
|---|---|
| Apps | `GET` / `POST /apps`, `GET` / `DELETE /apps/NAME`, `PUT /apps/NAME/priority` with `{"priority":N}`, `PUT` / `DELETE /apps/NAME/disabled`, `PUT /apps/NAME/settings`, `GET /apps/NAME/logs`, `POST /apps/NAME/restart`, `GET` / `PUT` / `DELETE /apps/NAME/policy`, `POST /apps/NAME/exec`, `GET /apps/NAME/approvals` and `POST /apps/NAME/approvals/ID` (a native OpenShell app's policy, a command in one of its replicas, and the approvals its policy asks for) |
| Scaling | `PUT` / `GET /apps/NAME/scale` (the app's replicas over all its nodes; `GET ...?node=NODE` shows one node's part) |
| Proof app jobs | `GET /apps/NAME/jobs?limit=N&active=true`, `POST /apps/NAME/jobs` (app-storage references), `GET /apps/NAME/jobs/ID`, `GET /apps/NAME/jobs/ID/output/proof.json` and `.../output/public_values.bin` |
| Batches | `GET` / `POST /batches`, `GET` / `DELETE /batches/NAME`, `PUT /batches/NAME/priority`, `PUT /batches/NAME/settings`, `PUT` / `DELETE /batches/NAME/disabled` |
| Batch jobs | `GET` / `POST /batches/NAME/jobs`, `GET /batches/NAME/jobs/ID`, `POST /batches/NAME/jobs/ID/cancel`, `POST /batches/NAME/jobs/ID/preempt`, `GET /batches/NAME/jobs/ID/logs` |

| Namespaces and tokens | Action |
|---|---|
| `GET /namespaces` | A scoped token's own namespace; every namespace for an administrator |
| `GET /namespaces/alice[?tag=TAG]` | Quota and use, with each tag's caps and use, or one tag's with `?tag=` |
| `POST /namespaces` with `{"name":"alice"}` | Create |
| `PUT /namespaces/alice/quota[?tag=TAG]` with `{"gpus":2,"cpus":16}` | Replace the namespace's or a tag's caps, recording a new tag if needed |
| `PUT` / `DELETE /namespaces/alice/suspended` | Suspend / resume |
| `GET /namespaces/alice/allocations` | Reservations across tags |
| `GET` / `POST /tokens`, `DELETE /tokens/TOKEN_ID` | List token IDs, permissions and scopes; issue; revoke |

A scoped token reads only its own namespace, by name or ID, and only its own tag's
caps and use; another namespace or tag selector is refused. Token registration takes
a namespace name or ID, an optional `tag` and its `permissions`; refreshing is a CLI
operation built on lookup and registration.

### Independent data-router control

The Router initiates outbound HTTPS requests to Fleet; node heartbeats and Fleet
never carry application streams. Shared JSON records come from `RouterControl`,
`RouterEnrollment`, `RouterSyncRequest` and `RouterSyncResponse` in `model.proto`.

| Endpoint | Authorization and effect |
|---|---|
| `PUT /routers/ID` | Administrator enrolls `{"issuer":"gateway-issuer","token_id":"..."}`. Token must hold only `router_sync`. Re-enrollment rotates credentials within the same issuer/namespace, preserving epoch and revocations. |
| `GET /routers/ID` | Administrator reads desired revocations, last delivered revision and reported `applied_revision`/`applied_ms`. |
| `DELETE /routers/ID/routes/ROUTE_ID` | Administrator permanently revokes one exact execution/service route. Idempotent; does not stop or migrate its workload. |
| `POST /routers/ID/sync` | Only that enrolled token ID, holding `router_sync`, may fetch and acknowledge this stream. Administrator and other router/app credentials are rejected. |

Initial request: `{"epoch":null,"after_revision":0,"applied_revision":null}`.
Response: `router_id`, `issuer`, stable random `epoch`, `from_revision`, `revision`,
`revoked_routes`, `lease_seconds:30`. Revisions count the append-only tombstones.
Reconnects send the persisted epoch and last applied cursor; the response contains
the contiguous suffix after that cursor. Acknowledgments cannot exceed a delivered
revision or go backwards. An epoch mismatch or cursor ahead of Fleet returns 409;
the Router must fail closed instead of silently resetting its state.

The Router polls every five seconds, durably applies the whole response and
cancels revoked sessions before acknowledging it on its next request. The ACK is
a local enforcement report, not a ZK proof. Control freshness expires after 30
seconds: active streams close and new access fails. Revoking its sync token or
suspending its namespace blocks renewal; explicit denial suspends access sooner.
The Router requires fresh synchronization after restart. Production requires
Fleet's durable PostgreSQL store; memory-only Fleet remains for local tests.
Schema 17 adds only the router table and permission; existing apps, allocations,
node heartbeats and running tasks retain their behavior.

This initial bounded stream supports 4096 permanent route tombstones per router;
further new revocations return 507 without dropping old entries. Provision another
router with a fresh grant issuer/key before this limit, withdraw the old router
and reissue grants; resetting state under the old issuer would revive old grants.
Revoking the sync token can suspend the entire router.
Signing-key updates and automatic app lifecycle revocation remain follow-up work.
Application ownership checks remain mandatory.
Configure the existing relay environment with `TRUSTED_ROUTER_FLEET_URL`,
`TRUSTED_ROUTER_ID`, `TRUSTED_ROUTER_TOKEN_FILE`; see trusted-data-router's
`crates/transport/CONTROL.md` for deployment and binary acceptance instructions.

Schema 18 adds a separate authorization snapshot and applied revision. An admin
can `PUT /routers/ID/access-policy` with `{namespace, workload, service, version,
sha256, enabled}`. The first policy activates identity enforcement for that relay.
`workload: "*"` supplies a default within one exact namespace and service; an exact
workload policy always wins, including `enabled: false`. Requests cannot use a
wildcard target. Upgrade both Fleet and the relay before publishing defaults.
Relays advertise `x-fleet-router-capabilities: workload-defaults-v1,scoped-revocations-v1` on every sync;
Fleet records this on the enrolled router and rejects a new default policy with
409 until support has been reported. A relay without that capability cannot
renew a router that already has default policies. The header leaves older Fleet's
closed JSON request unchanged. Token rotation clears the stored capabilities;
the replacement relay must sync before publishing new defaults.
The reserved `control` service requires an exact workload policy; wildcard control
policies cannot grant access and legacy enabled entries are withdrawn on sync.
Delegate subject revocations carry `application_only: true`; operator revocations
can cover control too. Fleet rejects scoped revocation until the relay advertises
support, and rejects a capability downgrade once scoped records exist.
New authorization records have a 64-record namespace budget within the existing
1024-record router limit (at most 16 fully reserved namespace shares). Historical
larger namespaces remain readable; policy replacement retains its existing slot.

With `revocation-compaction-v1` advertised, an explicit broad subject revocation
atomically replaces session records it fully covers. Retrying a broad revocation
also removes covered records retained by older versions. Namespace, principal and
operator/application scope must all be covered; Fleet never turns session logout
into a user ban automatically. The relay independently validates coverage before
committing and canceling affected streams. Upgrade the relay first: a rollback
can reject the compacted snapshot and must not reset its durable state. Without
this capability, Fleet retains the old append-only representation.

With `revocation-expiry-v1` advertised, each delegate revocation carries
`expires_at`, set by Fleet to one hour of grant lifetime plus 15 minutes of clock
margin from now. Every grant signed before the denial has expired by then, and
Fleet refuses access bindings for the subject while the denial exists. Fleet drops
the denial five minutes after it expires, during sync or the next revocation. The
relay accepts the drop only once its own clock has passed `expires_at`. Callers
cannot choose the lifetime (400). Repeating a logout refreshes its one record.
Operator revocations stay permanent. Expiring denials do not count toward the
64-record budget; they have their own: 256 live per namespace and 4096 per
router. Once a relay holds expiring denials, Fleet rejects its sync without the
capability (409); never downgrade the relay.

With `policy-retirement-v1` advertised, Fleet retires the enabled exact policies of
a workload that has had no app or batch in its namespace for
`router_policy_grace_secs` (600 by default), so they stop counting toward the
64-record budget. Set a policy after creating its app, or within that time before.
A retirement happens during a sync whose relay has applied the current
authorization revision, and removes the policies as one new revision. It raises
`retired_policy_version` to the highest version removed, and that floor never
decreases. A policy added afterwards is stored above the floor, whatever version
its request names, so a grant bound to a retired policy matches no later one of
the same name. Disabled policies and `*` defaults are never retired. Upgrade the
relay first; once a floor is stored, Fleet rejects its sync without the
capability (409), so never downgrade the relay.

Changing an existing policy requires a higher version. Repeating a policy with
the same `sha256` and `enabled`, at its stored version or a lower one, changes
nothing. New agents can inherit
defaults without letting application backends write infrastructure policy. Defaults
also apply to existing workloads without an exact entry; publish explicit disabled
entries first for workloads that must remain denied.

OpenShell startup retries resubmit the same proof ID and witness after an uncertain
submission. Proof cryptography runs before the Fleet allocation lock; the short
ledger commit still rechecks live ownership and the staged epoch under that lock.
Private delivery credentials are reproducible for the same ticket and validity,
with durable revocation tombstones. Queued payloads can be restored after a crash;
claimed deliveries are never automatically sent twice. Expired attempts can be
renewed only before dispatch, with unchanged inputs and a new proof ID. After
dispatch, a fresh owned observation can complete startup after grant expiry.
Readiness and list/watch work select live allocations rather than historical
launches; queued delivery expiry uses an index while durable history is retained.

Schema 20 retains the schema 19 normalization of existing 17/18 router JSON
for compare-and-swap writes and adds application-scoped revocation support. Epochs, revocations, acknowledgments, jobs
and allocations are retained. Schedulers supporting only schema 19 or earlier
reject this database at startup, including rollback builds that cannot interpret
scoped revocations. Keep a schema-20-compatible build for rollback.

A backend's namespace-wide `read,router_delegate` credential may request an
opaque connection binding at `POST /routers/ID/authorize` after it authenticates
the user and checks business ownership. Film's model API keys and permissions,
and ClawUp's chat credentials and bot ownership, remain application-owned. Fleet
checks namespace/service infrastructure access; it does not validate those keys.
Administrators cannot obtain ordinary delegated application bindings; the reserved
`control` service requires an administrator. Backends may revoke opaque subjects
or sessions only within their namespace via `POST /routers/ID/revoke-subject`.
The response contains only that delegate's namespace policies and revocations,
including on an idempotent retry; administrators retain the complete view.
Suspending a namespace atomically disables its current policies across every
router, increasing policy and authorization versions without changing runtime
allocations. Relays withdraw affected streams on their next successful sync;
an unreachable control plane cannot renew the 30-second lease. Administrators
can still disable policies and revoke subjects while the namespace is suspended.
Resuming it does not re-enable policies or revive old grants: publish a higher
enabled policy version explicitly after recovery.

`authorization.revision`, `delivered_authorization_revision` and
`applied_authorization_revision` are independent of route generations/revocations.
Before switching pilot traffic, require the applied authorization and route
revisions to match the desired values and `applied_ms` to be less than 30 seconds
old. Snapshot capacity is 1024 total policies plus permanent subject revocations;
new entries fail with 507 at capacity without discarding denials. Large rollouts
require safe collection/pagination. These references and ACKs are not policy or
TEE execution proofs. No existing application is rescheduled by a policy update.

### Central policy ledger

The optional `policy_gateway` section in the existing `fleet.yaml` pins an
approved Ziren 2.0 policy guest ELF, its SHA-256 and its `vk.bytes32()` value.
`fleet-scheduler` embeds the ledger in its existing HTTP listener; the existing
HTTPS gateway forwards `/trusted.ledger.v1.AuthorizationLedger/` using HTTP/2.
There is no new public port, standalone Rail server or local prover.
Retain `data_dir/policy` (mode 0700), including its SQLite WAL and signing secret,
across upgrades. Back it up consistently with SQLite's backup API. Do not share
this SQLite file across active schedulers or restore an older ledger over live
allocations. Disabling the section leaves legacy workloads unchanged.

All `/policy/*` routes require a Fleet administrator token. Namespace owners,
router delegates and execution credentials cannot call them; an owner sets its own
app's policy with `PUT /apps/NAME/policy`.

| Method and route | Input / result |
| --- | --- |
| `GET /policy/program` | Approved guest hashes and `program_vkey` |
| `GET /policy/program/elf` | Approved ELF bytes |
| `PUT /policy/budgets` | `{name, allowance}`; cumulative allowance never resets spending |
| `POST /policy/allocations/prepare` | Exact execution identity for an admitted, unfinished start command and one configured replica; persist before activation |
| `POST /policy/allocations/activate` | `{execution, policy, context, previous_state_root, budget}` |
| `POST /policy/allocations/revoke` | Exact execution identity; durable even after replacement |
| `POST /policy/requests` | `{job_id, request, cost}`; persist before remote proof submission |
| `GET /policy/requests/:id` | Exact staged evaluation request for recovery |
| `POST /policy/preflight` | `{job_id, request, cost}`; native decision, optional single-use grant and background receipt state |
| `GET /policy/preflight/:id` | Receipt state and `receipt_hex` only after cryptographic verification |
| `POST /policy/credentials` | `{execution, permission, expires_at}`; returns `{id, token, expires_at}` once, with `Cache-Control: no-store` |
| `DELETE /policy/credentials/:id` | Revoke by SHA-256 identifier; never put the bearer in a URL |

`permission` is `admission`, `execution` or `admission_and_execution`; credentials
expire in at most one hour and bind every field of the execution identity.
The gRPC methods `Reserve` and `Consume` require those scoped credentials, not
an administrator or namespace token. The ledger verifies the pinned proof and
current policy/context, accounts for budget atomically and consumes each grant
once. A transport failure after consumption is ambiguous: do not retry dispatch.
The shim must reconcile its durable local operation instead.

Runtime action authorization uses preflight: the current approved Rego policy is
evaluated before dispatch; the exact proof witness and native decision are saved
in the same transaction as the Allow grant and budget reservation. Deny and
ApprovalRequired also produce background receipts, without execution grants.
The action's tool consumes its existing grant rather than producing another proof.
The preflight endpoint is still administrator-only; an agent cannot choose this
mode to downgrade a strict request. It rejects `runtime.*` operations and reuse
of a strict job/action. Startup proof verification remains synchronous; the
existing proof-required Reserve RPC is unchanged. GOAT registration execution
is deferred and is not a prerequisite for the current Docker migration path.

Enable the worker in the existing `policy_gateway` section (the existing guest
manifest fields remain required):

```yaml
policy_gateway:
  program_elf: /opt/fleet/policy-guest.elf
  program_sha256: <approved-ELF-SHA256>
  program_vkey: <approved-vk.bytes32>
  preflight:
    endpoint: https://policy-proof-app.example/
    token_file: /private/policy-proof.token
    # signer_file: /private/policy-proof-sdk.key
```

This is the policy proof app's StageService endpoint, not Fleet's control URL.
The optional SDK signer is a private hex-encoded secp256k1 key for deployments
that require signed proof submissions. Files must be absolute, private, bounded
and regular; no token/key discovery occurs. TLS verifies the OS trust roots;
plain HTTP is restricted to numeric loopback tests. With no worker configuration,
preflight admission returns 503. No local prover or extra gateway is started.

The scheduler drives up to four remote operations concurrently. Receipt states
are `queued`, `proving`, `verified` and `failed`. Only the approved cryptographic
verifier can produce `verified`; native Allow and a completed network job cannot.
Workers retain the original policy/request through changes and expiry, and never
renew grants when receipts complete. Queued jobs survive restart; ambiguous
submissions are polled by their original ID without automatic resubmission. A
crash before that RPC can need operator reconciliation. Failed jobs block new
preflight actions for that instance. At 128 outstanding receipts (including
failures), admission rejects further requests. This is backpressure, not an
end-to-end latency guarantee. Existing executed effects cannot be rolled back by
a late proof failure. Receipt retention/monitoring and application action-hook
wiring are still deployment responsibilities.

GOAT registration execution remains TODO. The companion shim exposes a pure
EIP-712 owner-consent interface; this Fleet revision does not invoke it or submit
chain transactions. The future adapter must obtain the user's signature in
advance, atomically consume its nonce and preserve ownership when registering.
Signing alone does not register an agent or attest its runtime. Non-TEE identities
will be explicitly `unattested`; TEE identities will additionally require a
verified RA result bound to their instance/key before that trust profile is ready.
Future GOAT policy synchronization must authenticate owner and chain/contract,
confirmed policy version/digest and revocation before activating the local
snapshot. Current preflight uses the administratively activated snapshot and
does not claim chain synchronization or hardware attestation.

Activation, staging, credential issuance, reservation and consumption are fenced
by current Fleet state: exact reported or centrally prepared runtime identity, active deployment,
fresh connected node, active namespace and held resource allocation. The fence
remains locked through the ledger transaction. Node replacement, scale changes,
suspension and resource release reject old credentials without waiting for TTL.
Credential rotation does not reauthorize a revoked execution or reset budgets.

For creation, prepare an identity whose `lease_id` is the admitted `SetApp`
command ID, `allocation_id` is an actual `entry-N` replica, and namespace,
workload and machine match the command. A preparation cannot replace a reported
runtime, change an existing command's identity, reuse an older generation, or
overlap a preceding unfinished preparation. It does not schedule a new job or
change the requested driver. Retain `workloads/policy-preparations` with command
history across upgrades; those generated state records contain no credentials.
An unfinished command must remain admitted with unchanged deployment intent and
held quota. After command completion, the exact reported runtime identity takes
over; absence never authorizes a new creation. A late report of an older prepared
generation cannot revive its authorization.

The native OpenShell compute extension uses Fleet's existing remote proof and
private outbound delivery flow as described above; OpenShell introduces no
additional scheduler.
Do not put credentials or native launch secrets in App specs, environment,
node reports or diagnostics. Docker still rejects requested
proof-required execution modes; mounting this ledger does not change that driver.
Film model keys and ClawUp user/session authorization remain application-owned.

## MCP

Give external MCP users these connection details:

- **Server:** ZKM Fleet
- **Transport:** Streamable HTTP over HTTPS
- **URL:** `https://fleet.example:8905/mcp`
- **Authentication:** `Authorization: Bearer <user-token>`

An administrator issues each user a namespace- and tag-scoped token and delivers it
securely; MCP takes no administrator token. Initialization and `tools/list` need no
token, so clients can discover the tools and their schemas first. `tools/call` needs a
current user token, and the scheduler applies its namespace, tag and grants. A client
must support remote Streamable HTTP and a configurable Bearer header; one that runs
only local `stdio` servers can't connect.

The repository's `.mcp.json` configures this server for Claude Code from two
environment variables: `FLEET_MCP_URL` (your gateway's `/mcp` URL) and
`FLEET_MCP_AUTH` (the user token). The server accepts only the `Host` values in
`FLEET_MCP_ALLOWED_HOSTS`, which the control host install sets from `--public-url`;
without it, only loopback.

| Tool | Does |
|---|---|
| `fleet_apps_list`, `fleet_app_get` | `fleetctl app`, `fleetctl app NAME` |
| `fleet_app_create` | `fleetctl app add`: a `spec` and optional `priority`, plus a prover's `elf_id`, `class`, `anchor`, `backends`, `zkm_version` and `max_queued` |
| `fleet_app_logs` | `fleetctl app logs` |
| `fleet_batch_create` | `fleetctl batch add`; non-empty `allow` needs an administrator |
| `fleet_batches_list`, `fleet_batch_get` | `fleetctl batch`, `fleetctl batch NAME` |
| `fleet_batch_jobs_list`, `fleet_batch_job_get` | `fleetctl job --type batch` (optional `batch`), `fleetctl job ID` |
| `fleet_batch_job_submit` | `fleetctl batch submit`: a `batch`, a command and optional `mounts` such as `["/data/models:ro"]` |
| `fleet_batch_job_logs`, `fleet_batch_job_cancel` | `fleetctl batch logs`, `cancel`; `batch` is optional |
| `fleet_jobs_list` | `fleetctl job`, with `kind: "batch"` or `kind: "app"` |

Changing existing workloads, enabling and scaling go through `fleetctl` or the API.
Proofs are submitted through the Ziren network SDK; MCP lists and inspects them.

## Data

| What | Where |
|---|---|
| Registered apps, proof jobs, tokens | Postgres: `fleet_apps` (each namespace's programs and placement policy, with when each was disabled), `fleet_jobs` (state, kind and a failed job's stage status by name, with CHECK constraints), `fleet_tokens`, `fleet_namespaces` (namespace and tag quotas), `fleet_namespace_allocations`, `fleet_schema` (the schema version) |
| SDK proof ELFs and inputs | The app's `proof_storage_root`: `elf/<hash>` and `inputs/<hash>`; Fleet keeps references, hashes and parameters |
| SDK proof outputs | The app's `outputs/<job id>/<execution lease>/`: proof, public values and a committed manifest |
| EthProofs data | Its storage root: `checkpoint.json`, `inputs/`, `outputs/<block>/`, `outbox/` and `reported/`, in the app's own format |
| Apps, deployments, batches, jobs and commands | `data_dir/workloads/{apps,deployments,jobs,commands}` on the control host: durable JSON records, including lease and cleanup fences |
| Batch inputs, outputs and checkpoints | The owner's shared or object storage |
| Batch logs | `data_dir/workloads/artifacts/<job-id>/<lease>/logs` on the control host; nodes keep execution files in `FLEET_JOBS_DIR/<job-id>/`, normally `work/jobs/` |
| Policy ledger | `data_dir/policy` on the control host (mode 0700): SQLite with its WAL and signing secret |
| OpenShell gateway state | The `openshell-data` Compose volume: the gateway's SQLite database and WAL |
| Trustee AS policies and reference values | The `trustee-data` Compose volume |

## Ports

| Port | Serves |
|---|---|
| 8905 | The HTTPS gateway: the API, MCP and node heartbeats. Nodes need outbound HTTPS (HTTP/2) to it; the scheduler's own listeners stay private behind it. `GATEWAY_BIND` in `compose.env` limits the interface. |
| `127.0.0.1:9191` | The scheduler's HTTP API, for `fleetctl` on the control host |
| `127.0.0.1:17670` | The OpenShell gateway (`openshell` profile), with mTLS; `FLEET_OPENSHELL_BIND` and `FLEET_OPENSHELL_PORT` in `compose.env` change the address and port, for example to reach remote supervisors |
| 51001, 8906, 50004, 8081 | Private to the Compose network: the scheduler's node gRPC, MCP, Trustee AS and the OpenShell gateway's health check |
| `127.0.0.1:51012`, `51013` | GOAT's proof endpoint: SDK stage and HTTP, loopback only |
| 6379–6381, 52365, 8080, 10002–19999 | Each node's local Ray; the metrics port (8080) and the worker range (10002–19999) [move](operations.md#configure-resources) in `etc/watchdog.env` |

## Tokens and TLS

| Token | Held by | Issued by |
|---|---|---|
| User token (`FLEET_TOKEN_FILE`) | The namespace owner or client; authorizes proof RPCs and the API within its scope | Namespace creation or token issuance; revocable |
| SDK signing key (`ZKM_PRIVATE_KEY`) | The client | Generated in the token bundle and bound to its token |
| Server certificate and CA | The gateway and control API; clients get `CA_CERT_PATH` when supplied | `install.sh` (`fleetctl init tls`) |
| Cluster token | The scheduler and every node; authenticates node requests | `install.sh` (`fleetctl init token`) |
| Administrator token | Operators | `install.sh` (`fleet-scheduler issue-admin`); more with `fleetctl token admin` |

Administrator tokens live in the database.
`fleet-scheduler -c fleet.yaml issue-admin --label NAME --out FILE`, on the scheduler
host, records one and writes its secret to FILE (mode 0600, never overwritten); a
running scheduler reloads tokens every `heartbeat_secs`. `install.sh` issues the first
into `etc/admin.token` once Compose is up, and `fleetctl token admin --label NAME
--out FILE` issues more. A scheduler without `database_url`, for development and
tests, writes its own to `data_dir/admin.token` at start. `fleetctl` reads
`FLEET_TOKEN_FILE` (`--token-file`), and otherwise `admin.token` from `FLEET_ETC`,
`~/.config/zkm-fleet` or `/etc/zkm-fleet`.

Node control gRPC uses TLS with the scheduler's certificate and the cluster token, so
`apps.yaml` names an HTTPS `scheduler` URL. With a private CA, set
`scheduler_ca_file` so prover containers trust it; the installer sets it to its
generated CA, which each execution machine needs a copy of. Proof clients use their
bundled signing key and bearer token at their app endpoint: plain HTTP on loopback,
an authenticated HTTPS gateway from outside.

### Shim service data plane

Ordinary shim apps can declare `services: [http]` in the existing app spec. These
names map to the app's loopback listener; `container_port` optionally differs from
`port_base + replica index`, and `entrypoint` optionally overrides the image's
entrypoint. All three are administrator-owned settings and require
`runtime: {executor: shim, driver: docker}`. Changes use the existing drain,
confirmed cleanup and replacement sequence.

For an opaque TCP service such as OpenSSH, declare `services: [ssh]` and
`service_protocol: tcp` in that same spec. HTTP remains the default; no old app
configuration or launch hash changes. TCP requires `shim-tcp-v1` and uses a
loopback TCP readiness probe, never an HTTP request to the SSH port. Its client
opens the existing authenticated `/v1/tunnel/{route}` WebSocket byte stream.
Native application authentication still runs inside the tunnel. HTTP ingress
rules cannot be combined with a TCP app; an optional reserved `control` service
continues to use HTTP and administrator-only grants. Changing the protocol is an
administrator operation and uses the existing drain/reconfigure path. This adds
no host network access, storage replication or automatic task migration.

The existing node apps.yaml accepts `shim_network` with `gateway_url`, `relay_url`,
`token_file`, `public_key_file`, `issuer` and optionally `tunnel_binary`, the absolute
path of the Chisel tunnel helper ([maintenance](maintenance.md#execution-node)).
These are trusted node settings, never passed into the workload environment. The central gateway URL serves only
registration/control requests; `relay_url` is the independent WSS data plane.
Shim obtains and verifies exact-execution grants and owns outbound connections.
The gateway must authorize against live Fleet execution metadata before issuing
any grant. No service tokens or application traffic are carried in heartbeats.

The additive `NodeReport.runtime_instances` field carries typed execution
identity, readiness and approved service names. App responses expose these as
`runtime_instances` only while the corresponding enabled allocation and worker
are live, excluding other tags. Consumers must use the complete identity and
must not guess node addresses or host ports. Replacing an execution changes its
route; session/ComfyUI affinity must fail rather than switch instances silently.

If an application's health probe fails while its shim runtime still runs, the
node may report only the declared `control` service as ready. This lets an
administrator repair or restart the exact allocation without advertising its
unhealthy application services. Disabled, stale, ended and retiring allocations
remain hidden. Upgrade the scheduler's report validation before the bridge to
accept this restricted service subset.

Upgrade scheduler/fleet-node before the bridge (older JSON readers reject unknown
fields). The bridge advertises `shim-network-v1` only when the binary and node
network settings support it. New container-port/entrypoint options require
`shim-runtime-v2`. Shim
actors use a separate Ray worker environment so updated adapter code loads
without restarting Ray or existing detached apps. Stop/delete requires the shim
to drain live streams before releasing the port and Ray reservation.

### Shim management and ingress

The explicit Docker shim executor can declare the reserved `control` service for node-local administration. It maps to the shim's control endpoint; the central connection gateway requires Fleet administrator permission to issue that service's access grant. Application ingress is an administrator-only `ingress` map in the existing app spec, keyed by a declared service name (excluding `control`), with exact `paths`, allowed `methods`, and optional `bearer_env`. The shim injects that container environment credential locally after verifying the execution grant. A normal service with no ingress rule still maps directly to the app's port.

`port_base: 0` plus an explicit `container_port` asks the shim to reserve a host loopback port per allocation. `{port}` in shim command/environment templates denotes `container_port` when one is set; readiness and node reports use the actual host port returned by the shim. The reservation survives restart and is released only after confirmed cleanup. Capability requirements compose: automatic ports with services also require network support; container ports and entrypoints require the modern runtime adapter. The bridge rechecks these requirements on actor restart. Cleanup uses `shim-cleanup-v1` independently of launch support and reads persisted runtime ownership. Invalid runtime observations are quarantined per application while the node heartbeat and command results continue.

Ingress `bearer_env` values remain node-local. Reports and app views contain `@fleet-local-env:NAME` references; node configuration updates resolve those against the existing local value. A move to a node without that credential fails before writing configuration. Nodes must advertise `shim-private-env-v1` for these workloads.
 Runtime management/ingress requires `shim-management-v1`; automatic ports require `shim-automatic-port-v1`. Older nodes are ineligible. The bridge supervises a failed connector process without replacing the runtime. These capabilities require trusted-shim #5 and trusted-data-router #4.

Proof-required policy/TEE execution and the OpenShell driver are still not accepted by this Docker lifecycle path; requesting unsupported trust modes fails validation.

Shim HTTP ingress rules accept `single_use: true` to require an exact HTTP request
capability from the application's central gateway. Scheduler placement then
requires `shim-single-use-ingress-v1`; nodes advertise it only when both the shim
feature and outbound network configuration are present. Older ingress rules omit
the new field on the shim wire, preserving existing launch hashes and runtimes.
The shim persists request consumption and rejects replay after reconnect/restart.
This application admission setting does not enable PolicyEngine or TEE proofs.


### Data-router access authorization

Schema 18 adds the `router_delegate` permission; existing jobs and allocations are
not migrated or restarted. An administrator may issue a namespace-wide backend
token with `read,router_delegate`, no tag/app restrictions or other permissions.
Only this explicit delegation permission can call `POST /routers/:id/authorize`
and revoke subjects in its own namespace. Administrators may revoke subjects but
cannot use `/authorize` to assert business identities. Only the reserved `control`
service accepts administrator authorization, under its own enabled exact service
policy; namespace delegates cannot obtain control-service bindings. Application backends still
must check user ownership before requesting an access binding.

An administrator's `PUT /routers/:id/access-policy` accepts an exact namespace ID,
workload, service, monotonic version, SHA-256 digest and enabled flag. This
activates identity enforcement for all access through that router. The policy is
an approved reference, not a proof or an executable rule evaluation. Backends
sign the returned principal/session/policy binding; runtime connector identity
and execution policy remain separate. `POST /routers/:id/revoke-subject` takes
`{namespace, principal, session}`; null session revokes the principal. Operator
revocations are permanent; a delegate's expire as described above once the relay
supports it.
Principal and session are opaque 64-character lowercase hex IDs, never raw tokens.

Sync includes a bounded full `authorization` snapshot with its own revision,
independent of the route tombstone cursor. Router status exposes delivered and
applied authorization revisions. Persistence precedes delivery; relay SQLite
commit and affected-stream cancellation precede applied ACK. Healthy propagation
is one five-second poll plus latency; an unavailable control plane expires the
relay's lease within 30 seconds. A mutation response acknowledges durable intent,
not completed enforcement or a cryptographic proof.

Upgrade Fleet, relay and shim before issuing identity-bound grants; old shim
parsers reject them. Enable all needed service policies before switching a shared
router's clients. Unconfigured routers retain their behavior.
The authorization snapshot caps policies plus permanent revocations at 1024
records, and live expiring delegate denials separately at 4096; it returns 507 at
capacity. Compaction only removes denials covered
by an explicitly requested broader ban; distinct permanent bans still consume
capacity. General collection/pagination, key rotation and
automatic app-lifecycle revocation remain follow-up work; do not discard records
to recover capacity. See trusted-data-router `crates/transport/CONTROL.md` for the
complete rollout and credential boundaries.

### Node-configured TEE runtimes

Use the existing `apps.yaml` for node-owned trust configuration. Fleet accepts
`runtime: {executor: shim, driver: tee}` or `driver: tee-simulation` on ordinary
apps; packaged proof workers retain their Docker profile. Hardware and simulation
have separate scheduler capabilities and cannot satisfy each other's placements.
Transport capabilities must also match the app's services. Existing allocations
retain their original runtime and cleanup path.

A TEE node's CPUs are its confidential VM's vCPUs, fixed when the VM boots: set its
Ray pool, `RAY_NUM_CPUS` in `etc/watchdog.env`, to the vCPUs minus what the node's
own services need. An app's `cpu_limit` must fit in that pool; the scheduler and the
node refuse a larger one. Tenants that reserve no CPU (`cpus: 0`) share the VM only
where its operator sets `allow_cpu_overcommit: true` inside `shim_tee` (default
`false`); the node then reports `shim-tee-cpu-overcommit-v1`. The bridge enforces
this setting, and it isn't part of the shim's machine configuration or its digest.

```yaml
shim_daemon: true
shim_machine_labels:
  runtime.driver: tee-simulation
  tee.platform: sample
shim_tee:
  mode: simulation
  platform: sample
  evidence_endpoint: http://127.0.0.1:8006/aa/evidence
  trustee_endpoint: https://fleet.example:8905
  trustee_tls:
    ca_file: /etc/zkm-fleet/tls/ca.pem
    cert_file: /etc/zkm-fleet/tls/node.pem
    key_file: /etc/zkm-fleet/tls/node.key
  trustee_policy_ids: [default]
  expected_measurement: abcde
  init_data_sha256: '<approved 64-character digest>'
  endpoint_key_sha256: '<approved 64-character digest>'
  proof_endpoint: https://proof.example
  proof_token_file: /etc/zkm-fleet/ra-network.token
  proof_signer_file: null  # optional explicit Fleet signer key file
  program:
    kind: remote_attestation
    program_sha256: '<approved RA ELF SHA256>'
    verifying_key_sha256: '<approved RA verifying-key SHA256>'
  program_vkey_hash: '<approved Ziren vkey bytes32>'
  elf_file: /opt/trusted-shim/ra.elf
```

This is a configuration template, not a runnable proof guest. A matching approved
RA guest/ELF must be supplied. Shim now includes an Automata-based TDX guest;
SNP hardware guest and live hardware/proof-network acceptance remain pending. Simulation
also requires a remote receipt in the production daemon. Test doubles exist only
in the shim test suite; they cannot authorize production launches.

For hardware, select `mode: hardware`, `platform: snp` or `tdx`, driver `tee`,
matching measurement/key/init-data values and an immutable workload image digest.
Shim and its local runtime endpoints must be inside the measured confidential
VM. This adapter does not provision VMs or implement SGX/Nitro isolation. Attestation
Agent collects evidence; the remote guest verifies the actual quote/collateral and
binding before shim creates the container. For the Automata TDX path, omit Trustee
settings and add the following inside `shim_tee`:

```yaml
dcap:
  collateral_file: /var/lib/zkm-fleet/attestation/tdx-collateral.abi
  min_tcb_evaluation_data_number: 20 # example; approve the current Intel floor
```

The artifact is Automata's ABI-encoded collateral, fetched outside the guest and
refreshed atomically. This is not an additional configuration file. Approve the
complete measured-profile hash and the `trusted-shim-dcap-guest` ELF/key mapping.
The guest rejects stale material, invalid signatures and incorrect launch bindings;
shim still requires a valid remote receipt and checks expiry before creation.
The legacy SNP/sample contract remains separate. Trustee tokens alone never
satisfy the receipt requirement. See [shim's DCAP documentation](https://github.com/attestx/trusted-shim/tree/feat/rust-framework/attestation).

With no explicit driver, shim uses its configured TEE profile or detects local
interfaces. Detected TEE devices without an approved profile require explicit
configuration; `runtime.driver: docker` deliberately selects an unattested node.
`--probe-machine` reports devices only. `shim_openshell` is independent of `shim_tee`:
on TEE nodes, every new native sandbox passes through the same RA gate before its policy grant
is consumed and CreateSandbox is dispatched. Its compute driver, socket and
supervisor must be confined to the measured VM; SGX/Nitro need separate adapters.
The companion validates a canonical configuration
digest on reconnect; changing node settings cannot silently reconfigure a live
shim. Preserve its state and credentials through upgrades. Trust-profile changes
need drained allocations and an explicitly provisioned new state directory.
