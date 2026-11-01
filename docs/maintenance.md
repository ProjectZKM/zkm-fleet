# Maintenance

Use this procedure when changing a node's Ray capacity or replacing installed
Fleet programs. Ordinary app scaling uses the app owner's token with
`fleetctl app scale`; it does not require restarting Ray.

## Pause execution

On the control host, use administrator credentials for node commands and the
app owner's token for workload actions:

```bash
fleetctl node drain NODE
fleetctl node NODE
fleetctl --token-file owner.token --namespace NAMESPACE app disable APP
```

Disable each enabled app on that node. Wait for active proof/batch jobs to finish
and for app cleanup to complete before restarting Ray. Integrated workers own
their own input streams, so node draining alone does not stop them. Disabling
EthProofs preserves completed outputs and retries any unfinished block after
resume. Its [app guide](../apps/ethproofs/README.md#operate-and-recover) describes
finishing a current proof before disable.

Record the enabled apps, their allocations and each app's saved progress. Back
up `etc/` and app-owned durable directories under `work/apps/`. Keep a tested
package for rollback. Do not change app checkpoints or remove pending reports.

## Change capacity and restart

On the node, edit `~/app/fleet/etc/watchdog.env`:

```bash
RAY_NUM_CPUS=128
CUDA_VISIBLE_DEVICES=0,1,2,3,4,5,6,7
```

Choose capacity within the machine's hardware. Set `RAY_NUM_CPUS=''` for automatic
CPU detection. These values are total node capacity, shared by every allocation.

Stop the agent with its existing supervisor. For systemd deployments:

```bash
sudo systemctl stop zkm-fleet-node.service
```

The default installer uses cron instead. With `crontab -e`, temporarily comment
out the Fleet watchdog line, then stop that installation's `fleet-node` process
with SIGTERM and wait for it to exit. Restore the same cron line after maintenance.
Use one supervisor per agent.

Restart local Ray while holding the watchdog lock, so the minute watchdog cannot
race the restart. Run this in a separate shell after workloads have stopped:

```bash
bash <<'SH'
set -euo pipefail
. "$HOME/app/fleet/etc/watchdog.env"
export PATH="$FLEET_HOME/venv/bin:$PATH"
export CUDA_VISIBLE_DEVICES RAY_NUM_CPUS FLEET_WORKDIR
exec 8>"$FLEET_WORKDIR/watchdog.lock"
flock 8
ray stop
"$FLEET_HOME/deploy/ray-start.sh"
SH
```

For systemd, restart the agent with `sudo systemctl start zkm-fleet-node.service`.
For the default cron installation, restore the watchdog line and run
`FLEET_ENV="$HOME/app/fleet/etc/watchdog.env" ~/app/fleet/deploy/fleet-watchdog.sh`.
If updating code, install the tested binaries and Python package while the agent
is stopped, preserving the existing paths and machine configuration.

## Verify and resume

On the control host:

```bash
fleetctl node NODE
fleetctl node undrain NODE
fleetctl --token-file owner.token --namespace NAMESPACE app scale APP --gpus GPU_COUNT --cpus CPU_COUNT
fleetctl --token-file owner.token --namespace NAMESPACE app scale APP
fleetctl --token-file owner.token --namespace NAMESPACE app enable APP
```

Wait for the node to be ready with the expected total capacity before scaling.
Wait for scaling to complete before enabling a disabled app. Restore only the
apps that were enabled before maintenance. Confirm the observed allocations,
application progress and output/report delivery in normal logs.

EthProofs must resume at its saved next block and continue without gaps. Retain
the prior package and state backup until that check passes. On failure, disable
the affected app, inspect its error and restore the prior package/configuration;
keep the current durable cursor and reports. See the [operations guide](operations.md)
for log locations and the distinction between node capacity and app allocation.

## Update Fleet

Deploy a tested Git revision. Changes to shared models or execution protocols
require updating the control host and nodes together: the scheduler, agent and
Python bridge must agree on machine report version 2. A compatible change limited
to scheduler permissions, CLI or MCP needs only a control-host update. Use the
pause procedure above before replacing node programs. A program update does not
require restarting Ray or changing its pool.

Build the generic programs from that revision:

```bash
cargo build --locked --release \
  -p fleet-scheduler -p fleetctl -p fleet-mcp -p fleet-node
```

Keep the existing `etc/`, Postgres database, control `data_dir` and node `work/`.
Schema 16 installations retain namespaces and tokens across ordinary updates. A
schema 14 or 15 database migrates automatically when the scheduler starts, in one
transaction: each app takes a name (its tenant's when that tenant had one app in
the namespace, else `TENANT-<first 8 hex of the ELF id>`) and the default
placement policy, jobs name their app, `TENANT/ELF` grants become app names, and
namespace-less tenant signer tokens are removed. Update each prover's
`services.yaml` entry to `app: NAME` plus `elf_id`. Remove `tenants`,
`default_tenant`, `require_signature` and `admin_token_file` from `fleet.yaml`
first; the scheduler refuses to start with them. Issue an administrator token with
`fleet-scheduler -c fleet.yaml issue-admin --label NAME --out FILE`, since the old
`admin_token_file` no longer applies. Only another schema requires an explicit
metadata reset. A reset invalidates namespace access, so recreate and
redistribute scoped credentials before resuming clients.

### Control host

The scheduler and MCP run from the release's GHCR image
(`ghcr.io/projectzkm/zkm-fleet-scheduler:VERSION`) in Docker Compose; only
`fleetctl` is a host binary. Upgrade with:

```bash
FLEET_PREFIX="$HOME/app/fleet" deploy/fleet-ctl.sh scheduler upgrade v0.1.1
```

It pulls the image, backs up `fleetctl`, the Compose files and `compose.env`,
records the new image in `compose.env`, recreates the services and waits for
`/healthz`. `scheduler rollback` restores that backup and recreates them.

Verify the gateway's `/healthz`, `fleetctl namespace` and `fleetctl node`.
When enabled, also check MCP initialization and an authenticated tool call.
For a permission update, use a temporary workload to check `manage` metadata
changes and their rejection for a token without `manage`, and confirm that owner
quota updates are refused. Remove the smoke workload and revoke temporary tokens afterward. Compare
namespace quotas and existing app allocations with the pre-update snapshot;
record the deployed control revision and binary checksums.

### Execution node

Replace `bin/fleet-node`, `bridge/` and the three launch scripts
(`node-start.sh`, `ray-start.sh`, `fleet-watchdog.sh`) from the same revision while
the agent is stopped. Install the tested bridge wheel into the existing virtual
environment, then start the agent with its existing supervisor:

```bash
FLEET_PREFIX="$HOME/app/fleet"
sudo systemctl stop zkm-fleet-node.service
install -m 755 /path/to/package/bin/fleet-node "$FLEET_PREFIX/bin/fleet-node"
# Replace bridge/ and the launch scripts from the package before starting.
"$FLEET_PREFIX/venv/bin/python" -m pip install --no-index --no-deps \
  --force-reinstall /path/to/package/fleet_bridge-0.1.0-py3-none-any.whl
sudo systemctl start zkm-fleet-node.service
```

The offline wheel command assumes Ray and PyYAML are already installed at the
versions required by the package. For cron installations, pause and restore the
watchdog as described above. Preserve `etc/watchdog.env`, including node capacity
and `FLEET_NODE_SUPERVISED`, along with `services.yaml`, `batches.yaml` and credentials.
Keep independent app endpoints running when their programs are unchanged.
Verify the fresh heartbeat, total/free resources, app states and saved progress
before following [verify and resume](#verify-and-resume).

## Control script

`deploy/fleet-ctl.sh` runs the update steps above and checks the result:

```bash
export FLEET_PREFIX=/path/to/control/install FLEET_NODE_SSH=gpu-box  # or ~/.config/zkm-fleet/ctl.env
deploy/fleet-ctl.sh status                       # control services, health, nodes, apps and the node
deploy/fleet-ctl.sh -n scheduler upgrade         # -n prints every step and changes nothing
deploy/fleet-ctl.sh scheduler upgrade v0.1.0           # download and verify the GitHub Release package
deploy/fleet-ctl.sh node upgrade v0.1.0                 # download and verify the node package
deploy/fleet-ctl.sh scheduler upgrade target/release   # legacy local scheduler binaries
deploy/fleet-ctl.sh node upgrade target/release/fleet-node bridge   # legacy local node files over ssh
deploy/fleet-ctl.sh node upgrade target/release/fleet-node bridge target/release/fleet-proof-app
deploy/fleet-ctl.sh scheduler rollback           # or node rollback: restore the last upgrade's backup
deploy/fleet-ctl.sh app cleanup SERVICE-0        # leftovers of a failed app start
```

An upgrade copies what it replaces to `backup/` first, then waits for the scheduler's
health check or the node's reconnection. Upgrade the scheduler and nodes together when a
release changes the protocol. The script uses `sudo systemctl` with the unit names in this
guide; set `FLEET_SCHEDULER_UNIT`, `FLEET_MCP_UNIT` or `FLEET_NODE_UNIT` for others.
A third `node upgrade` argument also installs `fleet-proof-app` and restarts the app
endpoint units listed in `FLEET_PROOF_APP_UNITS`.

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

Before an update, record running allocations, pause affected workloads and back
up configuration plus durable app data. Install a tested revision, restart the
required processes, then verify readiness, resumed progress and report delivery.

Back up control-host Postgres and its `data_dir` together; batch records and app
intents live under `data_dir/workloads/`. Preserve node app checkpoints, prepared inputs,
outputs and pending reports. Keep tokens private. Delete obsolete build packages
and old logs only after checking running processes and rollback needs; keep
proof retention an explicit app policy.

Pin validated GPU images by digest. Update EthProofs using its committed submodule
revision with `git submodule update --init --recursive`; never deploy a moving
submodule branch. Package the GOAT worker from its committed
`third_party/bitvm-node` revision with `apps/goat/build.sh`; a guest change is a
new app identity (see the [GOAT guide](../apps/goat/README.md)). See
[data and ports](reference.md#data) for storage and network details.
