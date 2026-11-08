# Maintenance

Routine work in one place: upgrading, changing a node's capacity, liveness and
troubleshooting. One-time moves between specific older and newer releases are in
[one-time migrations](#one-time-migrations) at the end of this file; ordinary
upgrades need none of them.

## Upgrading

`deploy/fleet-ctl.sh` (in the release package, or in a checkout of that revision)
upgrades the control host and systemd-supervised nodes. Cron-supervised nodes use
the manual procedure below. Every scripted upgrade copies what it replaces to `backup/` first, then waits
for the scheduler's health check or the node's reconnection; the matching `rollback`
restores that backup.

```bash
export FLEET_PREFIX=/path/to/control/install FLEET_NODE_SSH=gpu-1   # or ~/.config/zkm-fleet/ctl.env
deploy/fleet-ctl.sh status                       # control services, health, nodes and apps
deploy/fleet-ctl.sh -n scheduler upgrade v0.1.6  # -n prints every step and changes nothing
```

### Control host

The scheduler and MCP run from the release's GHCR image
(`ghcr.io/projectzkm/zkm-fleet-scheduler:VERSION`) in Docker Compose; only `fleetctl`
is a host binary.

```bash
deploy/fleet-ctl.sh scheduler upgrade v0.1.6
```

It downloads the release package (SHA-256 verified), pulls the image first (a missing
image changes nothing), backs up `fleetctl`, the Compose files and `compose.env`,
records the new image, recreates `scheduler` — and `mcp`, when enabled — and waits
for `/healthz`; the recreation takes ~90s while the scheduler finishes in-flight
calls. `scheduler rollback` restores the backup. Site changes to the release's
Compose file, such as another gateway or an existing Postgres volume, stay in
`deploy/docker-compose.override.yml`: an upgrade replaces only `docker-compose.yml`,
and every Compose call applies the override after it.

Verify the gateway's `/healthz`, `fleetctl node` and `fleetctl app`; when MCP is
enabled, check its initialization and one authenticated tool call. For a
permissions-only update, exercise the changed grant with a temporary workload and a
token without the grant, then remove the workload and revoke the temporary tokens.

### Execution node

```bash
deploy/fleet-ctl.sh node upgrade v0.1.6
# or from a local build of the same revision:
deploy/fleet-ctl.sh node upgrade target/release/fleet-node bridge \
  target/release/fleet-proof-app deploy
```

It installs the release's `fleet-node`, bridge, `fleet-proof-app` and the three
launch scripts (`node-start.sh`, `ray-start.sh`, `fleet-watchdog.sh`) around a short
stop/start of the agent's unit, checking that the new bridge imports in the node's
own venv before the agent is touched, and waits for the node to reconnect.
`node rollback` restores everything the last upgrade replaced, launch scripts
included. The last two local-file arguments are optional: the proof app (its
endpoint units, named in `FLEET_PROOF_APP_UNITS`, restart too) and the launch
scripts' directory.

A program update does not require restarting Ray or changing its pool; running apps
and batches keep going through the agent restart. The upgrade touches neither
`etc/` (watchdog.env, apps.yaml, batches.yaml, tokens) nor `work/`; keep independent
app endpoints running when their programs are unchanged.

#### Cron-supervised nodes

`fleet-ctl.sh node upgrade` and `node rollback` require a systemd unit. For a
cron-supervised node, first comment out its watchdog line with `crontab -e`, send
SIGTERM to that installation's `fleet-node` process, and wait for it to exit.
Keep Ray running. With `PACKAGE` set to an unpacked, checksum-verified release
directory (or a local package from the tested revision), replace the programs:

```bash
FLEET_PREFIX="$HOME/app/fleet"
BACKUP="$FLEET_PREFIX/backup/node-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$BACKUP/scripts"
cp -a "$FLEET_PREFIX/bin/fleet-node" "$FLEET_PREFIX/bridge" "$BACKUP/"
for script in node-start.sh ray-start.sh fleet-watchdog.sh; do
  cp -a "$FLEET_PREFIX/deploy/$script" "$BACKUP/scripts/"
done
install -m 755 "$PACKAGE/bin/fleet-node" "$FLEET_PREFIX/bin/fleet-node"
cp -a "$PACKAGE/bridge/fleet_bridge/." "$FLEET_PREFIX/bridge/fleet_bridge/"
for script in node-start.sh ray-start.sh fleet-watchdog.sh; do
  install -m 755 "$PACKAGE/deploy/$script" "$FLEET_PREFIX/deploy/$script"
done
FLEET_ENV="$FLEET_PREFIX/etc/watchdog.env" "$FLEET_PREFIX/deploy/fleet-watchdog.sh"
```

Restore the same cron line, then verify a fresh heartbeat and workload progress.
If verification fails, pause cron and stop the agent again, restore `fleet-node`,
`bridge/` and the launch scripts from `BACKUP`, and restart the watchdog before
restoring cron. Preserve `etc/` and `work/`. If also updating `fleet-proof-app`,
back up and replace that binary while its endpoint supervisor is stopped, then
restart that supervisor; keep its previous binary for rollback.

### Control host and nodes together

Upgrade the control host and the nodes from the same release when the release
changes the shared model or execution protocol: the scheduler, agent and Python
bridge must agree on the machine report (currently version 3). A change limited to
scheduler permissions, the CLI or MCP needs only a control-host update. Read the
release's changes before choosing; the one-time migrations below record the past
cross-version moves.

## Self-built node programs

CI builds on Ubuntu 24.04 (glibc 2.39). Nodes on older glibc (22.04 is 2.35) run
such binaries with a loader warning, and production node programs are usually cut in
a matching build image instead. Build from the release's revision, naming the
release so each node's agent reports it in the VERSION column of `fleetctl node`
(a build that names nothing shows `-`):

```bash
FLEET_VERSION=0.1.6 cargo build --locked --release \
  -p fleet-scheduler -p fleetctl -p fleet-mcp -p fleet-node -p fleet-proof-app
```

For systemd nodes, install with the local-file form of `node upgrade` above;
for cron nodes, follow the manual procedure with the tested local package.

## Changing a node's capacity

Only this procedure restarts Ray. Use administrator credentials for node commands on
the control host and the app owner's token for workload actions.

**Pause execution.** Drain the node, then disable each enabled app on it:

```bash
fleetctl node drain NODE
fleetctl --token-file owner.token --namespace NAMESPACE app disable APP
```

Wait for active proof/batch jobs to finish and for app cleanup to complete before
restarting Ray. Integrated workers own their own input streams, so draining the node
alone does not stop them. Disabling EthProofs preserves completed outputs and
retries any unfinished block after resume; its
[app guide](../apps/ethproofs/README.md#operate-and-recover) describes finishing a
current proof first. Record the enabled apps, their allocations and each app's saved
progress. Back up `etc/` and app-owned durable directories under `work/apps/`. Do
not change app checkpoints or remove pending reports.

**Change the capacity.** On the node, edit `~/app/fleet/etc/watchdog.env`:

```bash
RAY_NUM_CPUS=128
CUDA_VISIBLE_DEVICES=0,1,2,3,4,5,6,7
```

Choose capacity within the machine's hardware; an empty `RAY_NUM_CPUS` enables
automatic CPU detection. These values are total node capacity, shared by every
allocation. Stop the agent with its existing supervisor (`sudo systemctl stop
zkm-fleet-node.service`, or, for cron installations, comment out the watchdog line
and stop `fleet-node` with SIGTERM, waiting for it to exit — one supervisor per
agent). Then restart local Ray while holding the watchdog lock, so the minute
watchdog cannot race the restart:

```bash
bash <<'SH'
set -euo pipefail
. "$HOME/app/fleet/etc/watchdog.env"
export PATH="$FLEET_HOME/venv/bin:$PATH"
export CUDA_VISIBLE_DEVICES RAY_NUM_CPUS FLEET_WORKDIR
export RAY_METRICS_EXPORT_PORT RAY_MIN_WORKER_PORT RAY_MAX_WORKER_PORT
exec 8>"$FLEET_WORKDIR/watchdog.lock"
flock 8
ray stop
"$FLEET_HOME/deploy/ray-start.sh"
SH
```

**Restart the agent.** For systemd, run
`sudo systemctl start zkm-fleet-node.service`. For cron, restore the watchdog line
and run
`FLEET_ENV="$HOME/app/fleet/etc/watchdog.env" ~/app/fleet/deploy/fleet-watchdog.sh`.

**Verify and resume.** On the control host:

```bash
fleetctl node NODE
fleetctl node undrain NODE
fleetctl --token-file owner.token --namespace NAMESPACE app scale APP --gpus GPU_COUNT --cpus CPU_COUNT
fleetctl --token-file owner.token --namespace NAMESPACE app enable APP
```

Wait for the node to be ready with the expected total capacity before scaling, and
for scaling to complete before enabling a disabled app. Restore only the apps that
were enabled before maintenance. Confirm the observed allocations, application
progress and output/report delivery in the normal logs. EthProofs must resume at its
saved next block and continue without gaps; keep the prior state backup until that
check passes. On failure, disable the affected app, inspect its error and restore
the prior configuration; keep the current durable cursor and reports.

## Liveness

Nothing here monitors itself; watch these on a schedule (a five-minute timer or
cron), alerting on state change rather than repeating:

- Control host: the gateway's `/healthz`; the scheduler, MCP, Postgres and gateway
  containers running; the scheduler log without repeated
  `job dispatch retained durable intents` lines (a stalled scale conflict — see
  below); the root disk under 90% (upgrades stage ~400 MB in /tmp).
- Each node: in `fleetctl node`, READY with a fresh heartbeat and the expected
  VERSION; its disk under 90%; `work/logs/watchdog.log` quiet.
- Workloads: `fleetctl app` — nothing sitting `failed`, or `starting` beyond its
  ready timeout, and `fleetctl job` — nothing stuck `waiting_quota` for hours.

## Troubleshooting

### A node is offline or not READY

Check its supervisor (the unit, or the cron watchdog), `work/logs/ray.log` and
`watchdog.log`, the gateway URL, cluster token and CA trust, then the node-local
traps:

- The Ray temp link `/tmp/zkf-ray-<uid>` must resolve. A dead link (a reboot, or a
  stray `ray-start.sh` run against a cleaned-up temp workspace) aborts every agent
  restart with an opaque log-path error; the watchdog repairs it each minute from
  v0.1.7, and by hand with
  `ln -sfn ~/app/fleet/work/ray /tmp/zkf-ray-$(id -u)`.
- A full disk: bridge writes, app starts and even the agent's log fail on it.

### An app is stuck starting, failed, or a job waits on quota

`fleetctl app` DETAIL says why: `waiting for capacity` (the start is held until it
fits; `app disable` or a smaller `app scale` replaces the held start), `starting on
NODE`, or `waiting for NODE to report it ready`. Read the app log (`fleetctl app
logs APP`) and the node's `work/logs/<app>-0.node.log`. If the scheduler log
repeats `job dispatch retained durable intents`, a scale is blocked — typically by
a stale deployment of the same app on another node (its node no longer configures
it). Since v0.1.7 the blocked scale defers while the rest of the fleet keeps
working, and stale starts are retired; on older releases, stop the scheduler, back
up and remove the stale `data/workloads/deployments/<node>-<app>.json` (and any
undone command for it under `data/workloads/commands/`), then start it again.

A start whose own node has no room for it moves to a node that has room, unless the
node's entry anchors it with `machine`. An anchored start waits for room on its node,
preempting lower-priority batch jobs of its namespace when it can. Every integrated
worker (an app whose node runs its prover as a Fleet worker, such as goat's proof
builder) is anchored, since the bridge requires `machine` for one. Up to v0.1.9 an
anchored start moved too, and the move rewrote the entry's `machine` to the new node's.
Pin such an app again by restoring its node's entry (the bridge keeps the old
`apps.yaml` as `apps.yaml.removed-<UTC time>`).

Once the node a moved app left has removed it, the scheduler frees the app's room
there. Up to v0.1.9 it never did, so the app's quota stayed held, and the start could
be sent back to the node the app had left, which left the app on no node until
`fleetctl app enable` placed it again.

### Every node's heartbeat times out while jobs queue

The nodes log `node heartbeat rejected; local execution continues ... Timeout
expired` and the gateway logs heartbeat 499s, yet they look idle while `fleetctl
job` lists jobs `queued` (assigned; awaiting node acknowledgement). The scheduler sits
at a full core with sqlx `slow_acquire` warnings although Postgres is idle. Up to
v0.1.7 each dispatch pass scanned every job and command record ever kept, once per
waiting job, while holding the lock node reports wait for. With tens of thousands of
records and about a hundred jobs in flight a pass outlasts the nodes' 10 s heartbeat
timeout, and no launch reaches them. Later releases scan only what is in flight and
let reports in between jobs. On v0.1.7 and older, keep fewer jobs in flight: below
about 50 the passes stay short.

A bulk purge (`fleetctl job purge --older-than DAYS`) of a busy batch's history has the
same symptom on v0.1.8 and older: it holds that lock while it writes a record and a
node command for every job, minutes for tens of thousands. Its node commands then
crowd out launches for as long again, at 16 per heartbeat. Later releases purge in
chunks and send a node's purges after its other commands.

The scheduler logs one `api request` line for each API call that can change state:
method, path with query, status, duration, and the caller's token id, label,
namespace and tag (never the token). `docker logs zkm-fleet-scheduler-1 | grep 'api
request'` shows who submitted, cancelled, purged or reconfigured what.

## Installed files and backups

Keep source/build files separate from the installed runtime. A node installation
uses these directories:

| Directory | Contents |
|---|---|
| `bin/`, `bridge/`, `venv/`, `deploy/` | Installed Fleet programs and launch scripts |
| `apps/` | Packaged application binaries and guest ELFs |
| `etc/` | Machine configuration and private credentials |
| `work/jobs/` | Local batch job records and output workspaces (`FLEET_JOBS_DIR`) |
| `work/apps/` | App-owned checkpoints, inputs, outputs and reports |
| `work/logs/`, `work/ray/`, `work/spill/` | Process logs, Ray runtime and spilled objects |
| `work/.maintenance/` | Deployment and resource provisioning receipts |
| `backup/` | Temporary rollback package; remove after successful verification |

Back up the control host's Postgres and its `data_dir` together: batch records and
app intents live under `data_dir/workloads/`. Keep tokens private. Delete obsolete
build packages and old logs only after checking running processes and rollback
needs; keep proof retention an explicit app policy.

Pin validated GPU images by digest. Update EthProofs using its committed submodule
revision with `git submodule update --init --recursive`; never deploy a moving
submodule branch. Package the GOAT worker from its committed `third_party/bitvm-node`
revision with `apps/goat/build.sh`; a guest change is a new app identity (see the
[GOAT guide](../apps/goat/README.md)). See [data and ports](reference.md#data) for
storage and network details.

## One-time migrations

Each section names the versions it applies to; a routine upgrade needs none of them.

### Machine report version 2 to 3

Upgrading from report version 2 changes the control host and every node at once.
Let in-flight commands finish, stop the scheduler and every node, and migrate their
stored state before starting the new binaries. The scheduler's per-node records move
from `workloads/services` to `workloads/deployments` (`service` becomes `spec`),
stored commands and `service:` reservations take the new names, and each registered
proof app needs its app record. A node's `etc/services.yaml` becomes
`etc/apps.yaml` (`services:` becomes `apps:`) and its `node-start.sh` passes
`--apps`. Back up `data_dir`, Postgres and the node's `etc/` first.

### The scheduler's SDK stage API (retired)

The release that moved to the app model retired the scheduler's SDK stage API
(`GenerateProof`/`GetStatus` on `stage_addr`): SDK clients prove through their
[app-owned proof endpoint](reference.md#app-owned-proof-endpoint). Remove
`stage_addr`, `tls`, `public_url`, `long_poll_ms` and `proof_output_dir` from
`fleet.yaml` first; the scheduler refuses to start with them. Let proof jobs
submitted through the stage API finish: one still queued or running fails when the
new scheduler starts. Remove the gateway's `/stage.v1.StageService/` route, which
has nothing behind it. The control host's `data_dir/elf`, `data_dir/jobs` and
`data_dir/proofs` held that API's payloads; delete them once rollback is no longer
needed. A node ignores `proof_output_dir` in `apps.yaml`.

### Database schema 14 or 15 to 16

Schema 16 installations retain namespaces and tokens across ordinary updates. A
schema 14 or 15 database migrates automatically when the scheduler starts, in one
transaction: each app takes a name (its tenant's when that tenant had one app in
the namespace, else `TENANT-<first 8 hex of the ELF id>`) and the default
placement policy, jobs name their app, `TENANT/ELF` grants become app names, and
namespace-less tenant signer tokens are removed. Update each prover's `apps.yaml`
entry to `app: NAME` plus `elf_id`. Remove `tenants`, `default_tenant`,
`require_signature` and `admin_token_file` from `fleet.yaml` first; the scheduler
refuses to start with them. Issue an administrator token with
`fleet-scheduler -c fleet.yaml issue-admin --label NAME --out FILE`, since the old
`admin_token_file` no longer applies. Only another schema requires an explicit
metadata reset. A reset invalidates namespace access, so recreate and redistribute
scoped credentials before resuming clients.

Across any update, keep the existing `etc/`, Postgres database, control `data_dir`
and node `work/`; back them up first — the rollback procedures above restore
programs, not data.
