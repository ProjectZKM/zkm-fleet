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
schema 16, and the scheduler checks the version at startup;
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
node's `apps.yaml` already runs, such as an integrated worker's prover bound with
`app: NAME`, isn't placed again.

**Moves.** Each start of an app the scheduler placed is placed again when it leaves
the queue: on its own node when that has room, else on the node that fits it best
now. That node gets a `create` with the app's entry, disabled. Once it reports the
entry, the old node removes its own (its configuration and leftover containers and
processes, never the app's data), which frees the room there, and the app starts on
the new node. A node whose create fails isn't picked again for that app. With no room
anywhere, the start preempts lower-priority work on its own node and keeps the room
that frees. A start whose entry is anchored with `machine` never moves; every
integrated worker's entry is. It waits for room on its node, preempting
lower-priority batch jobs of its namespace when it can. Fleet copies nothing an app
keeps on a node's disks: the app looks after its own storage. Anchor an app that keeps
data there, or runs from it, by setting `machine` in its entry in that node's
`apps.yaml` to the node's machine, as `fleetctl node` lists it.

**States.** An app is `configured`, `starting`, `running`, `draining`, `disabled`,
`offline` or `failed`; a proof app without provers is `registered`. The table shows
total configured GPUs and CPUs, and every tag when an app's entries use several; the
details show replica settings and job counts.

**Enabling and disabling.** Enabling starts the replicas before the app takes work.
Disabling drains and stops them, then releases their reservation; durable app data
stays. Disabling a proof app blocks new submissions, drains its active proofs and
stops its GPU server; queued proofs wait for the next enable, and its retained proofs
stay fetchable.

**Changing.** `fleetctl app scale` sets an app's GPUs, CPUs and replicas within its
quotas ([operations](operations.md#configure-resources)). They are the app's, not a
node's: the replica count covers all its nodes, and a node the count leaves none
removes the app. An active proof can't be
resized ([issue #3](https://github.com/ProjectZKM/zkm-fleet/issues/3)).
`fleetctl app update NAME` (`PUT /apps/NAME/settings`) changes the image,
environment, command, user, workdir, volumes, shm and memory sizes, health path or
timeouts. The image, environment, command, user, workdir and volumes need an
administrator, since an app can reach host networking, storage and prover
credentials; an update mixing them with other fields is refused whole. Changing
priority alone doesn't restart replicas.

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
  their directories: record, logs and `out/`. An app's replicas' logs and its workers'
  directories (`worker_workdir`) go, and so do its proof jobs' files in its proof
  storage: each job's output directory, and the inputs and ELF that no other job or
  app uses. An EthProofs worker's directory is its proof storage root, so its
  checkpoints and proofs go with it.

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
true, `min_runtime_s` 300 and `stop_timeout_s` 30. Other fields are refused.
`fleetctl batch update NAME --image ...` (`PUT /batches/NAME/settings`) changes them,
except `priority` and `disabled`, which have their own commands;
accepted jobs keep the settings they were submitted with. Only administrators set
`allow` (the host paths jobs may mount) and `user`, so an owner's batch starts with no
host mounts and the image's default user.

**Records.** A batch record (`type: batch`) has an app record's envelope. Its `state`
is `enabled`, `disabled` or `deleted`, its `jobs` count the batch's job records, and
its policy is under `batch`: `image`, `user`, `max_gpus`, `default_gpu_mem_gb`,
`shm_gb`, `allow`, `env`, `preemptible`, `min_runtime_s`, `stop_timeout_s` and
`workers`.

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

A stop sends SIGTERM, then SIGKILL after `stop_timeout_s`. Reservations last until
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
For a staged upgrade, an administrator can set `prover_binary` to an absolute
path and `machine` to this host in that prover's existing node-owned `apps.yaml`
entry. Both fields are node-owned; the scheduler preserves updates and never
automatically copies that executable selection to another machine. Other apps retain
the shared `node_binary`. This override is not accepted through user app creation
or settings APIs. It keeps the native prover's SDK input path; `worker_command`
instead denotes an integrated app that owns its inputs and rejects SDK submission.
The bridge resolves this selection before handing the replica to Ray. Its deployment
hash covers the selected executable, so changing an unused shared `node_binary`
does not restart a pinned prover, and an old adapter that ignored the override no
longer makes the wrong executable look current. New explicitly selected prover
actors load the updated adapter in a distinct Ray worker environment. Detached
legacy actors and their deployment hashes remain unchanged.

Upgrade scheduler and `fleet-node` before the bridge: report v3 adds optional
`native_provers` records (`entry_name`, `replica_name`, `executable`). Before the
upgrade, add `machine` to entries already using `prover_binary`. The actor reads
the running executable from `/proc/<pid>/exe`; older actors omit this evidence.
The machine's status includes these records for operator inspection. A selected
prover is not reported ready if its observed executable differs or is unknown.
Drain/restart only the affected app before submitting work. The shared process
role marker makes orphan cleanup work for renamed prover executables while still
checking the exact replica name.

Executable observations are operational metadata, not remote attestation or an
SDK/CUDA protocol compatibility check. Scheduling still matches the circuit label;
a validated protocol fingerprint covering the SDK, CUDA server and guest/verifier
remains follow-up work. Operators must deploy the validated tuple above together.
Check `fleet_bridge.__file__` from the adapter's Python interpreter and working
directory when deploying: a source tree in that directory takes precedence over
an installed wheel, so updating the wheel alone may leave the running code old.

Ordinary container apps can opt in to trusted-shim without migrating existing
workloads. Add `shim_binary` (the absolute `trusted-shim-runtime` executable path)
and `shim_state_dir` (an absolute private persistent directory) to the existing
node-owned `apps.yaml`. These shared settings do not enter legacy deployment
hashes. Upgrade the scheduler and `fleet-node` before the Python bridge: node report
v3 gains the optional `runtime_capabilities` field, with no database migration.
An older node advertises none and cannot receive a shim launch or acknowledge its
cleanup. A supported node advertises `shim-docker-v1` after probing the binary.

Keep old app entries unchanged. Only an administrator explicitly selecting
`runtime: {executor: shim, driver: docker}` on an ordinary app activates shim.
Use the existing app settings API to switch an app; its normal drain, stop,
confirmed cleanup and apply sequence runs first. Selection changes require
administrator credentials. `{executor: legacy}` selects the existing executor.
Batch jobs and Ziren prover apps retain their existing execution paths and durable
leases. No existing configuration, image or data migration is needed.

The machine adapter persists the selected binary, request binding, node, allocation,
lease and generation under its existing jobs state before dispatching to Ray.
Recovery keeps that identity; cleanup follows the persisted executor even if
configuration changes. Healthy unchanged legacy replicas are preserved. Directly
editing a live app's executor cannot replace its actor. An uncertain shim launch or
cleanup retains its reservation and never falls back to Docker directly. A stopped
or lost shim execution needs explicit cleanup and a new generation to run again.
Keep the allocation and shim journals across upgrades and restarts.

Shim delegates lifecycle to upstream Docker, applies CPU/memory hard limits and
checks effective limits before start. Its service port is published only on local
loopback; declared services use the independent router through outbound node
connectors. GPU ids are Ray's assigned ids. This first adapter supports non-TEE Docker lifecycle without a policy proof;
it makes no shell-policy enforcement claim. OpenShell/TEE and proof-required
lifecycle requests are unsupported and rejected. The existing separate proof-required
action admission path in trusted-shim is unchanged.

The bundled companion is built with OpenShell support. Operators can enable its
native provisioning endpoint in the **same** `apps.yaml`, alongside `shim_daemon`:

```yaml
shim_daemon: true
shim_openshell:
  socket: /run/openshell/compute.sock
  admission_policy: '<acknowledgement from the native driver configuration>'
  ledger_endpoint: https://fleet.example
  # Optional private CA; absolute, regular, not group/world writable.
  ledger_ca_file: /etc/zkm-fleet/ca.pem
```

These are operator settings, not allocation credentials. The native driver remains
an unmodified OpenShell process with a same-UID private socket. Node bootstrap
compares the running daemon's configuration digest with these settings; changing
or removing them requires explicit companion reconciliation. A node upgrade never
restarts the companion to apply a different policy gateway or driver. Existing
Docker apps and their persisted execution selection remain unchanged.

The daemon's private `provision` operation accepts exact native gateway bytes and
a proof-backed grant, consumes the grant centrally before Create, and keeps only
ownership/digests on disk. Native inspection/cleanup survives gateway outages and
expired credentials. This enables local native provisioning, not Fleet App
selection of OpenShell: automatic gateway orchestration, private outbound delivery
to remote nodes, and supervisor/application readiness are still required. Do not
place the native payload, ledger bearer or grant in an app, heartbeat or environment.

For local acceptance, build the companion `trusted-shim-runtime`, preload
`python:3.12-slim`, then run from this repository:

```sh
PYTHONPATH=bridge TRUSTED_SHIM_TEST_BINARY=/absolute/path/trusted-shim-runtime \
  python -m pytest -q -s bridge/tests/test_shim_cluster.py
```

The test starts an isolated local Ray cluster, runs legacy and shim apps together,
checks container ids/start times across adapter recovery, verifies Docker hard
limits and loopback-only publication, and confirms cleanup. No live Fleet node or
proof network is contacted. Without the explicit binary path this test is skipped.

Integrated EthProofs owns its whole pipeline (head polling, witness preparation,
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
root per line; `deploy/install.sh node --proof-storage-roots FILE` installs it. Keep
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
| Create, remove, enable, disable, prioritize, scale or update apps and batches | `manage`, with any app grants. Removing, enabling, disabling or prioritizing a proof app also needs the tag it was created for and the tag of every node entry running it; scaling and updating it need the tag of the entry they change. |
| Batch `allow` and `user`; app `user`, `volumes` and `proof_storage_root`; changing an app's image, environment, command, user, workdir or volumes | `admin` |
| Preempt batch jobs | `admin` |
| Namespaces, tokens, quotas, suspension, allocations, the preemption plan, nodes and status | `admin` |

`/healthz` and `/{proof_id}/wrap/public_values.bin` need no token, `/access` takes any
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
| Apps | `GET` / `POST /apps`, `GET` / `DELETE /apps/NAME`, `PUT /apps/NAME/priority` with `{"priority":N}`, `PUT` / `DELETE /apps/NAME/disabled`, `PUT /apps/NAME/settings`, `GET /apps/NAME/logs` |
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
Changing an existing policy requires a higher version. New agents can inherit
defaults without letting application backends write infrastructure policy.

A backend's namespace-wide `read,router_delegate` credential may request an
opaque connection binding at `POST /routers/ID/authorize` after it authenticates
the user and checks business ownership. Film's model API keys and permissions,
and ClawUp's chat credentials and bot ownership, remain application-owned. Fleet
checks namespace/service infrastructure access; it does not validate those keys.
Administrators cannot obtain ordinary delegated application bindings; the reserved
`control` service requires an administrator. Backends may revoke opaque subjects
or sessions only within their namespace via `POST /routers/ID/revoke-subject`.

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
router delegates and execution credentials cannot call them.

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
| `POST /policy/credentials` | `{execution, permission, expires_at}`; returns `{id, token, expires_at}` once, with `Cache-Control: no-store` |
| `DELETE /policy/credentials/:id` | Revoke by SHA-256 identifier; never put the bearer in a URL |

`permission` is `admission`, `execution` or `admission_and_execution`; credentials
expire in at most one hour and bind every field of the execution identity.
The gRPC methods `Reserve` and `Consume` require those scoped credentials, not
an administrator or namespace token. The ledger verifies the pinned proof and
current policy/context, accounts for budget atomically and consumes each grant
once. A transport failure after consumption is ambiguous: do not retry dispatch.
The shim must reconcile its durable local operation instead.

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

Native OpenShell preparation and automatic private delivery of credentials
still require node/gateway integration. Do not put credentials or native launch secrets in App
specs, environment, node reports or diagnostics. Docker still rejects requested
proof-required execution modes; mounting this ledger does not change that driver.
Film model keys and ClawUp user/session authorization remain application-owned.

## MCP

Give external MCP users these connection details:

- **Server:** ZKM Fleet
- **Transport:** Streamable HTTP over HTTPS
- **URL:** `https://34.215.238.232:8905/mcp`
- **Authentication:** `Authorization: Bearer <user-token>`

An administrator issues each user a namespace- and tag-scoped token and delivers it
securely; MCP takes no administrator token. Initialization and `tools/list` need no
token, so clients can discover the tools and their schemas first. `tools/call` needs a
current user token, and the scheduler applies its namespace, tag and grants. A client
must support remote Streamable HTTP and a configurable Bearer header; one that runs
only local `stdio` servers can't connect.

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
| Integrated EthProofs data | Its storage root: `checkpoint.json`, `inputs/`, `outputs/<block>/`, `outbox/` and `reported/`, in the app's own format |
| Apps, deployments, batches, jobs and commands | `data_dir/workloads/{apps,deployments,jobs,commands}` on the control host: durable JSON records, including lease and cleanup fences |
| Batch inputs, outputs and checkpoints | The owner's shared or object storage |
| Batch logs | `data_dir/workloads/artifacts/<job-id>/<lease>/logs` on the control host; nodes keep execution files in `FLEET_JOBS_DIR/<job-id>/`, normally `work/jobs/` |

## Ports

| Port | Serves |
|---|---|
| 8905 | The HTTPS gateway: the API, MCP and node heartbeats. Nodes need outbound HTTPS (HTTP/2) to it; the scheduler's own listeners stay private behind it. |
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
entrypoint. All three are administrator-owned settings and require explicit
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
`token_file`, `public_key_file`, and `issuer`. These are trusted node settings,
never passed into the workload environment. The central gateway URL serves only
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
accept this restricted service subset. No legacy task changes executor.

Upgrade scheduler/fleet-node before the bridge (older JSON readers reject unknown
fields). The bridge advertises `shim-network-v1` only when the binary and node
network settings support it. New container-port/entrypoint options require
`shim-runtime-v2`. Existing Docker-only shim and legacy tasks remain supported;
installing network settings leaves legacy deployment hashes unchanged. New shim
actors use a separate Ray worker environment so updated adapter code loads
without restarting Ray or existing detached apps. Stop/delete requires the shim
to drain live streams before releasing the port and Ray reservation.

### Shim management and ingress

The explicit Docker shim executor can declare the reserved `control` service for node-local administration. It maps to the shim's control endpoint; the central connection gateway requires Fleet administrator permission to issue that service's access grant. Application ingress is an administrator-only `ingress` map in the existing app spec, keyed by a declared service name (excluding `control`), with exact `paths`, allowed `methods`, and optional `bearer_env`. The shim injects that container environment credential locally after verifying the execution grant. A normal service with no ingress rule still maps directly to the app's port.

`port_base: 0` plus an explicit `container_port` asks the shim to reserve a host loopback port per allocation. `{port}` in shim command/environment templates denotes `container_port` when one is set; readiness and node reports use the actual host port returned by the shim. The reservation survives restart and is released only after confirmed cleanup. Runtime management/ingress requires `shim-management-v1`; automatic ports require `shim-automatic-port-v1`. Older nodes are ineligible. The bridge supervises a failed connector process without replacing the runtime. These capabilities require trusted-shim #5 and trusted-data-router #4.

Installing this support does not select it for legacy apps. Proof-required policy/TEE execution and the OpenShell driver are still not accepted by this Docker lifecycle path; requesting unsupported trust modes fails validation.

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
`{namespace, principal, session}`; null session permanently revokes the principal.
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
router's clients. Unconfigured routers and legacy tasks retain their behavior.
The initial authorization snapshot caps policies plus permanent revocations at
1024 records and returns 507 at capacity. Collection/pagination, key rotation and
automatic app-lifecycle revocation remain follow-up work; do not discard records
to recover capacity. See trusted-data-router `crates/transport/CONTROL.md` for the
complete rollout and credential boundaries.
