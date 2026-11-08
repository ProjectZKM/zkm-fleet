# Maintenance

Upgrading, changing a node's capacity, liveness and troubleshooting. Moves between
specific older releases are in [one-time migrations](#one-time-migrations); routine
upgrades need none of them.

## Upgrading

`deploy/fleet-ctl.sh`, from the release package or a checkout of that revision,
upgrades the control host and systemd-supervised nodes; cron-supervised nodes use the
[manual procedure](#cron-supervised-nodes). Each scripted upgrade first copies what it
replaces to `backup/`, then waits for the scheduler's health check or the node's
reconnection, and the matching `rollback` restores that copy.

```bash
export FLEET_PREFIX=/path/to/control/install FLEET_NODE_SSH=gpu-1   # or ~/.config/zkm-fleet/ctl.env
deploy/fleet-ctl.sh status                       # control services, health, nodes and apps
deploy/fleet-ctl.sh -n scheduler upgrade v0.1.6  # -n prints every step and changes nothing
```

### Control host

The scheduler and MCP run in Docker Compose from the release's GHCR image
(`ghcr.io/projectzkm/zkm-fleet-scheduler:VERSION`); only `fleetctl` is a host binary.

```bash
deploy/fleet-ctl.sh scheduler upgrade v0.1.6
```

It downloads the release package and checks its SHA-256, pulls the image (a missing
image changes nothing), backs up `fleetctl`, the Compose files and `compose.env`,
records the new image, recreates `scheduler` (and `mcp` when enabled) and waits for
`/healthz`. The old scheduler can take about 90 seconds to finish in-flight calls, and
the new one reads every job and command record it keeps before it listens, a minute or
more on a busy host. The script waits up to five minutes (`FLEET_CTL_HEALTH_WAIT`
seconds), and gives up at once if the scheduler's container exits or restarts.
`scheduler rollback` restores the backup. Keep site changes to the Compose
file, such as another gateway or an existing Postgres volume, in
`deploy/docker-compose.override.yml`: an upgrade replaces only `docker-compose.yml`,
and every Compose call applies the override after it.

Then check the gateway's `/healthz`, `fleetctl node` and `fleetctl app`, and with MCP
enabled, its initialization and one authenticated tool call. After a permissions
change, try the changed grant with a temporary workload and a token without the
grant, then remove both.

### Execution node

```bash
deploy/fleet-ctl.sh node upgrade v0.1.6
# or from a local build of the same revision:
deploy/fleet-ctl.sh node upgrade target/release/fleet-node bridge \
  target/release/fleet-proof-app deploy
```

It installs the release's `fleet-node`, bridge, `fleet-proof-app` and launch scripts
(`node-start.sh`, `ray-start.sh`, `fleet-watchdog.sh`) around a short stop and start
of the agent's unit, after checking that the new bridge imports in the node's venv,
and waits for the node to reconnect. `node rollback` restores everything the last
upgrade replaced. In the local form, the last two arguments are optional: the proof
app, whose endpoint units in `FLEET_PROOF_APP_UNITS` restart too, and the launch
scripts' directory.

Ray keeps running, and so do apps and batches, through the agent restart. The upgrade
touches neither `etc/` (watchdog.env, apps.yaml, batches.yaml, tokens) nor `work/`;
leave app endpoints running when their programs are unchanged.

#### Cron-supervised nodes

`node upgrade` and `node rollback` need a systemd unit. On a cron-supervised node,
comment out the watchdog line with `crontab -e`, send SIGTERM to that installation's
`fleet-node` and wait for it to exit, leaving Ray running. With `PACKAGE` set to an
unpacked, checksum-verified release directory (or a local package from the tested
revision), replace the programs:

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

Restore the cron line, then check for a fresh heartbeat and workload progress. To roll
back, pause cron and stop the agent again, restore `fleet-node`, `bridge/` and the
launch scripts from `BACKUP`, and restart the watchdog before restoring cron. To
update `fleet-proof-app` too, back up and replace it while its endpoint's supervisor
is stopped, then restart that supervisor.

### Control host and nodes together

Upgrade both from the same release when it changes the shared model or execution
protocol: the scheduler, agent and Python bridge must agree on the machine report
(version 3). A change to scheduler permissions, the CLI or MCP needs only the control
host. The release notes say which applies.

## Self-built node programs

CI builds on Ubuntu 24.04 (glibc 2.39). Older nodes (22.04 has 2.35) run those
binaries with a loader warning, so production node programs are usually built in a
matching image instead. Build from the release's revision and name the release, so
the agent reports it in `fleetctl node`'s VERSION column (`-` otherwise):

```bash
FLEET_VERSION=0.1.6 cargo build --locked --release \
  -p fleet-scheduler -p fleetctl -p fleet-mcp -p fleet-node -p fleet-proof-app
```

Install it with the local form of `node upgrade` on systemd nodes, and the manual
procedure on cron nodes.

## Changing a node's capacity

This is the only procedure that restarts Ray. Node commands need administrator
credentials, workload commands the app owner's token.

**Pause.** Drain the node, then disable each enabled app on it:

```bash
fleetctl node drain NODE
fleetctl --token-file owner.token --namespace NAMESPACE app disable APP
```

Wait for active proofs and batch jobs to finish and for app cleanup to complete.
Draining alone doesn't stop integrated workers, which run their own input streams.
Disabling EthProofs keeps its finished outputs and retries any unfinished block on
resume; its [guide](../apps/ethproofs/README.md#operate-and-recover) describes
finishing the current proof first. Note the enabled apps, their allocations and each
app's saved progress, and back up `etc/` and the apps' durable directories under
`work/apps/`. Don't change app checkpoints or remove pending reports.

**Change.** On the node, edit `~/app/fleet/etc/watchdog.env`:

```bash
RAY_NUM_CPUS=128
CUDA_VISIBLE_DEVICES=0,1,2,3,4,5,6,7
```

These are the node's total capacity, shared by every allocation, within its hardware;
an empty `RAY_NUM_CPUS` detects CPUs automatically. Stop the agent with its
supervisor, of which each agent has exactly one: `sudo systemctl stop zkm-fleet-node.service`, or on cron, comment out the
watchdog line and send `fleet-node` SIGTERM, waiting for it to exit. Then restart Ray
while holding the watchdog lock, so the minute watchdog can't race the restart:

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

**Restart the agent.** On systemd, `sudo systemctl start zkm-fleet-node.service`. On
cron, restore the watchdog line and run
`FLEET_ENV="$HOME/app/fleet/etc/watchdog.env" ~/app/fleet/deploy/fleet-watchdog.sh`.

**Resume.** On the control host:

```bash
fleetctl node NODE
fleetctl node undrain NODE
fleetctl --token-file owner.token --namespace NAMESPACE app scale APP --gpus GPU_COUNT --cpus CPU_COUNT
fleetctl --token-file owner.token --namespace NAMESPACE app enable APP
```

Wait for the node to be ready with the expected capacity before scaling, and for
scaling to complete before enabling. Re-enable only the apps that were enabled
before. Check the observed allocations, the apps' progress and their output and
report delivery in the logs. EthProofs must resume at its saved next block with no
gaps; keep the state backup until it does. If something fails, disable the app,
read its error and restore the previous configuration, keeping its current cursor
and reports.

## Liveness

Nothing here monitors itself. Check these on a schedule, such as a five-minute timer,
and alert when they change:

- **Control host:** the gateway's `/healthz`; the scheduler, MCP, Postgres and gateway
  containers running; no repeated `job dispatch retained durable intents` in the
  scheduler log (a [blocked scale](#an-app-is-stuck-starting-failed-or-a-job-waits-on-quota));
  the root disk under 90%, since upgrades stage about 400 MB in /tmp.
- **Each node:** READY in `fleetctl node`, with a fresh heartbeat and the expected
  VERSION; its disk under 90%; a quiet `work/logs/watchdog.log`.
- **Workloads:** no app in `fleetctl app` sitting `failed` or `starting` beyond its
  ready timeout, and no job in `fleetctl job` stuck `waiting_quota` for hours.

## Troubleshooting

### A node is offline or not READY

Check its supervisor (the unit or the cron watchdog), `work/logs/ray.log` and
`watchdog.log`, the gateway URL, the cluster token and CA trust. Then the node's own
traps:

- **The Ray temp link** `/tmp/zkf-ray-<uid>` must resolve. A dead link, after a reboot
  or a stray `ray-start.sh` run against a cleaned-up temp workspace, aborts every
  agent restart with an opaque log-path error. The watchdog repairs it every minute
  from v0.1.7; by hand, `ln -sfn ~/app/fleet/work/ray /tmp/zkf-ray-$(id -u)`.
- **A full disk** fails bridge writes, app starts and even the agent's log.

### An app is stuck starting, failed, or a job waits on quota

`fleetctl app` DETAIL says why ([operations](operations.md#logs-and-recovery)). Read
the app's log with `fleetctl app logs APP`, and the node's
`work/logs/<app>-0.node.log`.

If the scheduler log repeats `job dispatch retained durable intents`, a scale is
blocked, usually by a stale deployment of the same app on a node that no longer
configures it. From v0.1.7 the blocked scale waits while the rest of the fleet keeps
working, and stale starts are retired. On older releases, stop the scheduler, back up
and remove the stale `data/workloads/deployments/<node>-<app>.json` and any undone
command for it under `data/workloads/commands/`, then start it again.

A start whose node has no room moves to a node that has, unless its entry anchors it
with `machine`; an anchored start waits on its node, preempting lower-priority batch
jobs of its namespace when it can. Every integrated worker (an app whose node runs
its prover as a Fleet worker, such as goat's proof builder) is anchored, since the
bridge requires `machine` for one. Up to v0.1.9:

- an anchored start moved too, and the move rewrote the entry's `machine` to the new
  node's. Pin such an app again by restoring its node's entry; the bridge keeps the
  old `apps.yaml` as `apps.yaml.removed-<UTC time>`.
- the scheduler never freed a moved app's room on the node it left, so its quota
  stayed held, and a start could be sent back to that node, leaving the app on no
  node until `fleetctl app enable` placed it again.

### Every node's heartbeat times out while jobs queue

The nodes log `node heartbeat rejected; local execution continues ... Timeout
expired` and the gateway logs heartbeat 499s. The nodes look idle while `fleetctl job`
lists jobs `queued` (assigned, awaiting node acknowledgement), and the scheduler sits
at a full core with sqlx `slow_acquire` warnings although Postgres is idle.

- **Up to v0.1.7**, each dispatch pass scanned every job and command record ever kept,
  once per waiting job, while holding the lock node reports wait for. With tens of
  thousands of records and about a hundred jobs in flight, a pass outlasts the nodes'
  10-second heartbeat timeout and no launch gets through. Keep fewer than about 50
  jobs in flight. Later releases scan only what's in flight and let reports in between
  jobs.
- **Up to v0.1.8**, a bulk purge of a busy batch's history (`fleetctl job purge
  --older-than DAYS`) does the same: it holds that lock while it writes a record and a
  node command for every job, minutes for tens of thousands, and its node commands
  then crowd out launches for as long again, at 16 per heartbeat. Later releases purge
  in chunks and send a node's purges after its other commands.

### Who changed what

The scheduler logs an `api request` line for each API call that can change state:
method, path and query, status, duration, and the caller's token ID, label, namespace
and tag (never the token itself).
`docker logs zkm-fleet-scheduler-1 | grep 'api request'` shows who submitted,
cancelled, purged or reconfigured what.

## Installed files and backups

Keep source and build files apart from the installed runtime. A node installation
has:

| Directory | Contents |
|---|---|
| `bin/`, `bridge/`, `venv/`, `deploy/` | Fleet's programs and launch scripts |
| `apps/` | Packaged application binaries and guest ELFs |
| `etc/` | Machine configuration and private credentials |
| `work/jobs/` | Local batch job records and output workspaces (`FLEET_JOBS_DIR`) |
| `work/apps/` | App-owned checkpoints, inputs, outputs and reports |
| `work/logs/`, `work/ray/`, `work/spill/` | Process logs, Ray runtime and spilled objects |
| `work/.maintenance/` | Deployment and resource provisioning receipts |
| `backup/` | The last upgrade's rollback copy; remove it once the upgrade checks out |

Back up the control host's Postgres and `data_dir` together, since batch records and
app intents are under `data_dir/workloads/`, and keep tokens private. Across any
update, keep `etc/`, Postgres, the control `data_dir` and each node's `work/`, and back
them up first: rollbacks restore programs, not data. Delete old build packages and
logs only after checking running processes and rollback needs; proof retention is the
app's own policy.

Pin validated GPU images by digest. Update EthProofs to its committed submodule
revision with `git submodule update --init --recursive`, never a moving branch.
Package the GOAT worker from its committed `third_party/bitvm-node` revision with
`apps/goat/build.sh`; a guest change is a new app identity (see the
[GOAT guide](../apps/goat/README.md)). Storage paths and ports are in the
[reference](reference.md#data).

## One-time migrations

Each applies only to the versions it names.

### Owner-controlled runtime settings

Releases that moved app execution settings, batch mount roots and runtime users to
administrators don't rewrite existing configuration or cancel jobs an earlier
scheduler accepted. Review existing apps' settings, batches' mount roots and runtime
users, and queued jobs before giving untrusted owners access again.

### Machine report version 2 to 3

This changes the control host and every node at once. Let in-flight commands finish,
stop the scheduler and every node, back up `data_dir`, Postgres and each node's
`etc/`, and migrate their stored state before starting the new binaries:

- The scheduler's per-node records move from `workloads/services` to
  `workloads/deployments` (`service` becomes `spec`); stored commands and `service:`
  reservations take the new names; each registered proof app needs its app record.
- A node's `etc/services.yaml` becomes `etc/apps.yaml` (`services:` becomes `apps:`),
  and its `node-start.sh` passes `--apps`.

### The scheduler's SDK stage API (retired)

The release that moved to the app model retired the scheduler's SDK stage API
(`GenerateProof`/`GetStatus` on `stage_addr`): SDK clients prove through their
[app-owned proof endpoint](reference.md#app-owned-proof-endpoint).

1. Remove `stage_addr`, `tls`, `public_url`, `long_poll_ms` and `proof_output_dir`
   from `fleet.yaml`; the scheduler refuses to start with them. A node ignores
   `proof_output_dir` in `apps.yaml`.
2. Let proofs submitted through the stage API finish: any still queued or running
   fails when the new scheduler starts.
3. Remove the gateway's `/stage.v1.StageService/` route, which has nothing behind it.
4. Once rollback is no longer needed, delete the control host's `data_dir/elf`,
   `data_dir/jobs` and `data_dir/proofs`, which held that API's payloads.

### Database schema 14 or 15 to 16

A schema 14 or 15 database migrates automatically when the scheduler starts, in one
transaction: each app takes a name (its tenant's when that tenant had one app in the
namespace, else `TENANT-<first 8 hex of the ELF id>`) and the default placement
policy, jobs name their app, `TENANT/ELF` grants become app names, and namespace-less
tenant signer tokens are removed.

- Update each prover's `apps.yaml` entry to `app: NAME` plus `elf_id`.
- Remove `tenants`, `default_tenant`, `require_signature` and `admin_token_file` from
  `fleet.yaml` first; the scheduler refuses to start with them and names what replaced
  each.
- Issue an administrator token with
  `fleet-scheduler -c fleet.yaml issue-admin --label NAME --out FILE`, since
  `admin_token_file` no longer applies.

Any other schema needs an explicit metadata reset, which removes job history and
access registrations. Recreate namespaces and tokens, redistribute the scoped
credentials before clients resume, and keep the apps' own proofs and checkpoints.
Schema 16 keeps namespaces and tokens across ordinary updates.
