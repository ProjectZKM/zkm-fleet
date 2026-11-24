# Maintenance

Upgrading, changing a node's capacity, liveness and troubleshooting. Moves between
specific older releases are in [one-time migrations](#one-time-migrations); routine
upgrades need none of them.

## Upgrading

`deploy/fleet-ctl.sh`, from the release package or a checkout of that revision,
upgrades the control host and systemd-supervised nodes; cron-supervised nodes use the
[manual procedure](#cron-supervised-nodes). `install.sh control` also installs a copy as
`$FLEET_PREFIX/deploy/fleet-ctl.sh`, but that is the version from the last install: for
an upgrade, run the new release's copy. Each scripted upgrade first copies what it
replaces to `backup/`, then waits for the scheduler's health check, or for the scheduler
to report the node READY with a heartbeat newer than the restart, and the matching
`rollback` restores that copy.

```bash
export FLEET_PREFIX=/path/to/control/install FLEET_NODE_SSH=gpu-1   # or ~/.config/zkm-fleet/ctl.env
export FLEET_NODE_PREFIX=/path/to/node/install                      # default ~/app/fleet
deploy/fleet-ctl.sh status                       # control services, health, nodes and apps
deploy/fleet-ctl.sh -n scheduler upgrade v0.2.10  # -n prints every step and changes nothing
```

### Control host

Upgrade the independent data-router relay before publishing `workload: "*"`
policies. Its next authenticated poll must advertise `workload-defaults-v1` in
`GET /routers/ID`'s `capabilities`; publication otherwise returns 409. The updated
relay also works with the previous Fleet JSON API. Once a default is stored, keep
that relay capability through rollback, even if the default is disabled.
The relay must also advertise `scoped-revocations-v1` before application delegates
can revoke sessions. These revocations affect application services; operator
control revocation remains an administrative action. Use a distinct `router_sync`
token for each router. Existing duplicate bindings remain readable but cannot be
created or rotated into a shared token.


The scheduler and MCP run in Docker Compose from the release's GHCR image
(`ghcr.io/projectzkm/zkm-fleet-scheduler:VERSION`); only `fleetctl` is a host binary.

```bash
deploy/fleet-ctl.sh scheduler upgrade v0.2.10
```

It downloads the release package and checks its SHA-256, pulls the image (a missing
image changes nothing), backs up `fleetctl`, the Compose files, `compose.env`,
`$FLEET_ETC/gateway.yaml` and the database schema version, records the new image,
recreates `scheduler` (and `mcp` when enabled) and waits for `/healthz`. It re-renders
`gateway.yaml` from the new `gateway.yaml.in` and recreates the HTTPS gateway when that
changes it. It replaces `gateway.yaml` only while it still matches what the previous
template rendered: an edited one is left alone and the gateway is not restarted, with the
new render beside it as `gateway.yaml.release` to merge by hand. A site without that file (it runs
its own gateway) gets a note instead. It leaves the OpenShell gateway alone. The old
scheduler can take about 90 seconds to finish in-flight calls, and the new one reads
every job and command record it keeps before it listens, a minute or more on a busy
host. The script waits up to five minutes (`FLEET_CTL_HEALTH_WAIT`
seconds), and gives up at once if the scheduler's container exits or restarts.
`scheduler rollback` restores the backup, except that in `compose.env` it restores only
the scheduler image, so settings added since (such as the OpenShell image pin and
profile) stay. It recreates the HTTPS gateway if its configuration changes back. It refuses when
the database schema changed since the upgrade (as from 0.1 to schema 20), because the
older scheduler can't start on the migrated database: restore the Postgres and
`data_dir` backups taken before the upgrade instead. The version is read from the bundled
Postgres or, when `database_url` points elsewhere, with `psql` from the Compose Postgres
image on the host network. A backup without a recorded version, such as one taken by an
older `fleet-ctl.sh`, needs `--force`. `scheduler rollback --force` skips that check. Keep site changes to the Compose file, such as another gateway or an
existing Postgres volume, in
`deploy/docker-compose.override.yml`: an upgrade replaces only `docker-compose.yml`,
and every Compose call, the installer's included, applies the override after it.

Then check the gateway's `/healthz`, `fleetctl node` and `fleetctl app`, and with MCP
enabled, its initialization and one authenticated tool call. After a permissions
change, try the changed grant with a temporary workload and a token without the
grant, then remove both.

### Policy evaluator

The scheduler and shim pin Rail's v2 profile, including portable 32/64-bit
collection indices. Run `python3 scripts/check_policy_features.py` during the
scheduler build; it uses the pinned Rail audit for evaluator source and features.

For policy-enabled workloads, build the matching guest and run Rail's native/MIPS
checker, then verify a fresh receipt through the proof network. Approve the new
ELF/program key and new policy revisions with the v2 engine profile before
resuming admission on upgraded components. Old profiles are rejected; do not
relabel historical receipts or silently rewrite stored policy digests. Preserve
the old policy and program records for receipt history. This does not migrate or
stop existing workloads automatically.

### Execution node

```bash
deploy/fleet-ctl.sh node upgrade v0.2.10
# or from a local build of the same revision:
deploy/fleet-ctl.sh node upgrade target/release/fleet-node bridge \
  target/release/fleet-proof-app deploy
```

It installs the release's `fleet-node`, bridge, `fleet-proof-app` and launch scripts
(`node-start.sh`, `ray-start.sh`, `fleet-watchdog.sh`, `shim-start.sh`,
`ray-watch.sh`, `install-node-services.sh`) around a short stop and start
of the agent's unit, after checking that the new bridge imports in the node's venv
and parses the node's `apps.yaml`, and waits until the scheduler reports the node
READY with a heartbeat newer than the restart (a drained node never reports READY, so
for it a newer heartbeat is enough; check READY once it's undrained). Without the
control host's `fleetctl` and admin token, it only checks the agent's log for the
connection, and warns. A
parse error stops the upgrade before the agent is touched. The release form also
stages the package's `trusted-shim-runtime` and `trusted-router-tunnel` under
`libexec/<name>/<sha256>/` and prints their paths, without switching the running shim. `node rollback` restores everything the last
upgrade replaced. In the local form, the last two arguments are optional: the proof
app binary staged for its next explicit endpoint restart, and the launch scripts'
directory. Running proof endpoints keep their current processes.

The companion installer refreshes only recognized generated units, preserving
operator edits and drop-ins. Ray uses `KillMode=process`: losing its watchdog does
not kill the Ray processes. The installer hands the agent to systemd only once Ray
and the shim run, the shim check passes, the units are enabled and
`zkm-fleet-node.service` is active. A failure before that restores `apps.yaml`,
`watchdog.env` and the units it changed, and disables the units it enabled, so the
previous supervision resumes; repair the reported failure and rerun the installer.
A former oneshot Ray unit needs a watchdog restart after unit conversion; schedule
this during the maintenance window after checking that `KillMode=process` is loaded.
Re-running `install.sh node` changes an install-owned `watchdog.env` setting
(`FLEET_NODE_NAME`, `CUDA_VISIBLE_DEVICES`, `RAY_NUM_CPUS`, `FLEET_SCHEDULER_CA`,
`FLEET_WORKDIR`, `FLEET_JOBS_DIR` and the like) only when you pass its flag, and
retains custom settings and supervision ownership. A `--scheduler` that differs from
`apps.yaml`'s `scheduler` updates it there too.


Ray keeps running, and so do apps and batches, through the agent restart. The upgrade
touches neither `etc/` (watchdog.env, apps.yaml, batches.yaml, tokens) nor `work/`;
leave app endpoints running when their programs are unchanged.

On a node installed with `--with-shim`, shim and its connectors are supervised by
`zkm-fleet-shim.service`, independently of the node and Ray units. Before stopping
the agent, upgrade and rollback use the candidate Python adapter to negotiate
protocol version 1 with the running daemon. An unavailable or incompatible daemon,
or a rollback adapter without daemon support, rejects the operation while the old
node continues running. Set `FLEET_ETC` (or `FLEET_ENV`) when configuration lives
outside the default `$FLEET_PREFIX/etc`. With `FLEET_NODE_SSH`, node commands run on the
node over ssh with `FLEET_PREFIX` set to `FLEET_NODE_PREFIX`, and pass `FLEET_ETC`,
`FLEET_ENV` and `FLEET_NODE_UNIT` along when they're set. They run `sudo systemctl`
there, so the ssh user needs passwordless sudo.

These commands do not change the configured shim executable, restart its service,
release existing Ray reservations, or change allocation/lease/generation identities.
Do not add `PartOf=zkm-fleet-node.service` or `BindsTo` from shim/Ray to the node;
keep separate cgroups with normal `KillMode=control-group` cleanup. Detached Ray
actors alone cannot protect processes that share a service's stopping cgroup.

During the brief agent restart, scheduling/control operations can be unavailable.
Existing runtime streams continue through shim and the relay. Registration leases
renew on the same transport with a compatible relay; application Access credentials
still expire/revoke normally. A prolonged node outage can prevent new registration
grants because Fleet no longer has a live observation, and is not covered by the
node-upgrade continuity guarantee. Shim/Ray/Docker/host upgrades are separate
operations and must not be bundled into this restart.

ClawUp's Fleet adapter requires shim's authenticated `GET /logs?tail=N`
management endpoint. The release bundle includes that implementation. `node upgrade`
and a re-install only stage a new shim binary under `libexec/trusted-shim/<sha256>/`
and print its path; neither switches an already running shim. For an older shim,
schedule a separate maintenance window: drain the node, finish/stop affected
workloads and confirm their cleanup; stop `zkm-fleet-shim.service`, update
`shim_binary` in the existing `apps.yaml` to the staged executable, then start the
service and run the shim check before resuming workloads, from the bridge directory
so the current bridge code runs:

```bash
(. ~/app/fleet/etc/watchdog.env && cd "$FLEET_HOME/bridge" &&
  ../venv/bin/python -m fleet_bridge.shim check --apps "$FLEET_ETC/apps.yaml" --wait-seconds 30 --network)
```

It parses the whole `apps.yaml` and checks the running shim. Invalid node-wide settings
fail it. An invalid app entry prints a warning only: `node-start.sh` runs this check before
every start, and the agent must stay online so the entry can be fixed or disabled through
the scheduler. With `--network`, it also
fails when `shim_network.tunnel_binary` isn't an absolute path to a regular executable
that group and others can't write, or the running shim doesn't report
`chisel-network-v1`; without it, those only print a warning. Preserve
`shim_state_dir` and its allocation/replay journals. Do not turn a routine node
upgrade into an implicit runtime migration.

To move a shim node's service forwarding onto the pinned Chisel transport, use the
same maintenance window: drain the node and confirm cleanup as above, then set
`shim_network.tunnel_binary` in the existing `apps.yaml` to the staged
`libexec/trusted-router-tunnel/<sha256>/trusted-router-tunnel` path that `node upgrade`
or the installer printed, and run the shim check above with `--network`. Once it
passes, and the shim reports `data-plane-v1`, the node advertises
`shim-chisel-network-v1`; a configured helper that fails does not fall back to the
old transport. The helper must be a regular executable that neither group nor
others can modify. A missing or modifiable one, or a shim without `chisel-network-v1`,
withdraws only the network capabilities, so apps with services stop being placed there;
provers and apps without services keep running and stay scheduled. Rollback is the reverse edit: restore the previous path, or remove
the key to return to the existing transport. Staged hashes are never replaced or
removed by reinstalling, so the previous helper stays available.

Run the real systemd acceptance against local, isolated services (no local prover):

```sh
RUN_FLEET_NODE_UPGRADE=1 \
TRUSTED_FLEET_TEST_BIN=/absolute/fleet/bin \
TRUSTED_SHIM_TEST_BINARY=/absolute/trusted-shim-runtime \
TRUSTED_ROUTER_TEST_BINARY=/absolute/trusted-data-router \
PYTHONPATH=bridge python -m pytest -q -s deploy/tests/test_node_upgrade_cluster.py
```

It requires Docker with `python:3.12-slim`, the bridge/Ray environment, and
passwordless sudo for uniquely named test units. It checks upgrade, rollback,
incompatible-candidate rejection, unchanged container/PID/allocation, continuous
SSE/WebSocket traffic, business authorization, and an unreplayed application job.

One agent session at a time drives a node. A stopping agent ends its session, so the
scheduler admits the restarted agent at once. An agent that crashed, or one stopped
while the scheduler was out of reach, leaves its session open: the next agent is
refused ("another agent session is active for this worker") until the open session
has been quiet for 30 seconds. Ending a session needs a scheduler at least as new as
the agent, so upgrade the control host first. A remote `-n` run ships nothing to the node,
and the node's part of it prints the steps for the files it would have received.

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

Release and image binaries are built on Ubuntu 22.04 (glibc 2.35), so they run on
22.04 and newer. A binary built on a newer system needs that system's glibc: build
production node programs on the oldest release your nodes run, or in a matching image.
Build from the release's revision and name the release, so the agent reports it in
`fleetctl node`'s VERSION column (`-` otherwise):

```bash
FLEET_VERSION=0.2.10 cargo build --locked --release \
  -p fleet-scheduler -p fleetctl -p fleet-mcp -p fleet-node -p fleet-proof-app
```

Install it with the local form of `node upgrade` on systemd nodes, and the manual
procedure on cron nodes.

`fleet-ziren-node`, which `--proof-storage-roots` needs, isn't in the release package: build it the same way with
`-p fleet-ziren-node` and install it as the node's `bin/fleet-ziren-node`.

## Changing a node's capacity

This is the only procedure that restarts Ray. Node commands need administrator
credentials, workload commands the app owner's token.

**Pause.** Drain the node, then disable each enabled app on it:

```bash
fleetctl node drain NODE
fleetctl --token-file owner.token --namespace NAMESPACE app disable APP
```

Wait for active proofs and batch jobs to finish and for app cleanup to complete.
Draining alone doesn't stop an app's replicas, such as a worker that runs its own input stream.
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
while holding the watchdog lock, so the watchdog can't race the restart. On a systemd
node, leave out the last line (`ray-start.sh`): `zkm-fleet-ray.service` starts Ray
again within 30 seconds with the new settings, under its own unit.

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

**Restart the agent.** On systemd, once Ray is back, `sudo systemctl start zkm-fleet-node.service`. On
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
  containers running, and `openshell-gateway` and `trustee-as` where enabled; no
  repeated `job dispatch retained durable intents` in the scheduler log (a [blocked scale](#an-app-is-stuck-starting-failed-or-a-job-waits-on-quota));
  the root disk under 90%, since upgrades stage about 400 MB in /tmp.
- **Each node:** READY in `fleetctl node`, with a fresh heartbeat and the expected
  VERSION; its disk under 90%; a quiet `work/logs/watchdog.log` on cron nodes, or
  `journalctl -u zkm-fleet-node -u zkm-fleet-ray -u zkm-fleet-shim` on systemd nodes.
- **Workloads:** no app in `fleetctl app` sitting `failed` or `starting` beyond its
  ready timeout, and no job in `fleetctl job` stuck `waiting_quota` for hours.

## Troubleshooting

### A node is offline or not READY

Check its supervisor and its logs: on cron nodes `work/logs/watchdog.log` and
`node.log`, on systemd nodes `journalctl -u zkm-fleet-node -u zkm-fleet-ray -u
zkm-fleet-shim`; then `work/logs/ray.log`, the gateway URL, the cluster token and CA
trust. Then the node's own traps:

- **`local Ray snapshot unavailable`**, repeated in the agent's log, means the bridge
  can't build its report: Ray isn't up, or `apps.yaml` or `batches.yaml` doesn't
  parse (the traceback names the setting, such as a bad `shim_network`). The node
  stays not READY until it can.
- **The Ray temp link** `/tmp/zkf-ray-<uid>` must resolve. A dead link, after a reboot
  or a stray `ray-start.sh` run against a cleaned-up temp workspace, aborts every
  agent restart with an opaque log-path error. The watchdog repairs it every minute
  (every 30 seconds under `zkm-fleet-ray.service`); by hand,
  `ln -sfn ~/app/fleet/work/ray /tmp/zkf-ray-$(id -u)`.
- **A full disk** fails bridge writes, app starts and even the agent's log.

### An app is stuck starting, failed, or a job waits on quota

`fleetctl app` DETAIL says why ([operations](operations.md#logs-and-recovery)). Read
the app's log with `fleetctl app logs APP`, and the node's
`work/logs/<app>-0.node.log`.

If the scheduler log repeats `job dispatch retained durable intents`, a scale is
blocked, usually by a stale deployment of the same app on a node that no longer
configures it. The blocked scale waits while the rest of the fleet keeps working, and
stale starts are retired.

A start whose node has no room moves to a node that has, unless its entry anchors it
with `machine`; an anchored start waits on its node, preempting lower-priority batch
jobs of its namespace when it can.

### Who changed what

The scheduler logs an `api request` line for each API call that can change state:
method, path and query, status, duration, and the caller's token ID, label, namespace
and tag (never the token itself).
`docker logs zkm-fleet-scheduler-1 | grep 'api request'` shows who submitted,
cancelled, deleted or reconfigured what.

## Installed files and backups

Keep source and build files apart from the installed runtime. A node installation
has:

| Directory | Contents |
|---|---|
| `bin/`, `bridge/`, `venv/`, `deploy/` | Fleet's programs and launch scripts |
| `libexec/` | Pinned shim and tunnel-helper binaries (`<name>/<sha256>/`) that a running shim executes, and copies of the units the installer wrote; keep every hash `apps.yaml` names |
| `apps/` | Packaged application binaries and guest ELFs |
| `etc/` | Machine configuration and private credentials |
| `work/jobs/` | Local batch job records and output workspaces (`FLEET_JOBS_DIR`) |
| `work/apps/` | App-owned checkpoints, inputs, outputs and reports |
| `work/logs/`, `work/ray/`, `work/spill/` | Process logs, Ray runtime and spilled objects |
| `work/.maintenance/` | Deployment and resource provisioning receipts |
| `work/shim/` | The shim's state (`shim_state_dir`): its control socket and allocation and replay journals |
| `backup/` | The last upgrade's rollback copy; remove it once the upgrade checks out |

Back up the control host's Postgres and `data_dir` together, since batch records and
app intents are under `data_dir/workloads/` and the policy ledger is in
`data_dir/policy`, and keep tokens private. Where they're enabled, back up the
`openshell-data` and `trustee-data` volumes too ([installation](installation.md#openshell-gateway)).
Across any update, keep `etc/`, Postgres, the control `data_dir`, those volumes and
each node's `work/`, and back them up first: rollbacks restore programs, not data.
Delete old build packages and logs only after checking running processes and rollback
needs; proof retention is the app's own policy.

Pin validated GPU images by digest. Update EthProofs to its committed submodule
revision with `git submodule update --init --recursive`, never a moving branch.
Package the GOAT worker from its committed `third_party/bitvm-node` revision with
`apps/goat/build.sh`; a guest change is a new app identity (see the
[GOAT guide](../apps/goat/README.md)). Storage paths and ports are in the
[reference](reference.md#data).

## One-time migrations

Each applies only to the versions it names.

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

The 0.2 runtime release also migrates schema 16 through 19 automatically to 20.
For an unsupported schema, stop and prepare a tested migration; never reset
production metadata. Preserve job history, namespace/token identities and application
storage. After migration, rollback requires a schema-20-compatible scheduler.
