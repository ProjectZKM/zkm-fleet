# Maintenance

Routine work in one place: upgrading, changing a node's capacity, liveness and
troubleshooting. One-time moves between specific older and newer releases live in
[migrations](migrations.md); ordinary upgrades need none of them.

## Upgrading

`deploy/fleet-ctl.sh` (in the release package, or in a checkout of that revision) is
the upgrade path. Every upgrade copies what it replaces to `backup/` first, then waits
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
app endpoints running when their programs are unchanged. For a node supervised by
cron rather than systemd, the watchdog keeps restarting the old agent until the new
files are in place — the script's stop/start window covers it, but do not start a
second supervisor.

### Control host and nodes together

Upgrade the control host and the nodes from the same release when the release
changes the shared model or execution protocol: the scheduler, agent and Python
bridge must agree on the machine report (currently version 3). A change limited to
scheduler permissions, the CLI or MCP needs only a control-host update. Read the
release's changes before choosing; [migrations.md](migrations.md) records the past
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

Install with the local-file form of `node upgrade` above.

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
