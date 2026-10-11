# Maintenance

Use this procedure when changing a node's Ray capacity or replacing installed
Fleet programs. Ordinary app scaling needs only `fleetctl app scale`; it does
not require restarting Ray.

## Pause execution

On the control host:

```bash
fleetctl node drain NODE
fleetctl node NODE
fleetctl --namespace NAMESPACE app disable APP
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
fleetctl --namespace NAMESPACE app scale APP --gpus GPU_COUNT --cpus CPU_COUNT
fleetctl --namespace NAMESPACE app scale APP
fleetctl --namespace NAMESPACE app enable APP
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

Deploy one tested Git revision to the control host and every node. Shared models
come from protobuf, and machine reports use version 2; the scheduler, agent and
Python bridge must agree. Use the pause procedure above before replacing node
programs. A program update does not require restarting Ray or changing its pool.

Build the generic programs from that revision:

```bash
cargo build --locked --release \
  -p fleet-scheduler -p fleetctl -p fleet-mcp -p fleet-node
```

Keep the existing `etc/`, Postgres database, control `data_dir` and node `work/`.
Schema 14 installations retain namespaces and tokens across ordinary updates.
Only an incompatible schema requires an explicit metadata reset; there are no
database migrations. A reset invalidates namespace access, so recreate and
redistribute scoped credentials before resuming clients.

### Control host

For the default Docker Compose installation, rebuild the image from the checkout
and recreate the scheduler. Set `FLEET_PREFIX` to the installed directory:

```bash
FLEET_PREFIX="$HOME/app/fleet"
docker build -f deploy/Dockerfile.scheduler -t zkm-fleet-scheduler:local .
install -m 755 target/release/fleetctl "$FLEET_PREFIX/bin/fleetctl"
docker compose --env-file "$FLEET_PREFIX/compose.env" -p zkm-fleet \
  -f "$FLEET_PREFIX/deploy/docker-compose.yml" up -d --no-deps --force-recreate scheduler
```

If MCP is enabled, repeat the Compose command with `mcp` instead of `scheduler`.
Use the image tag already configured by the installation if it differs from
`zkm-fleet-scheduler:local`. Replace installed Compose/gateway templates when
their configuration changes; rebuilding an image alone does not replace them.

Existing native systemd installations keep their supervisor. For the deployed
control host, run from the checkout:

```bash
FLEET_PREFIX="$PWD/.fleet"
sudo systemctl stop zkm-fleet-mcp.service zkm-scheduler.service
install -m 755 target/release/fleet-scheduler target/release/fleetctl \
  target/release/fleet-mcp "$FLEET_PREFIX/bin/"
sudo systemctl start zkm-scheduler.service zkm-fleet-mcp.service
```

Verify the gateway's `/healthz`, `fleetctl namespace` and `fleetctl node`.
When enabled, also check MCP initialization and an authenticated tool call.

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
and `FLEET_NODE_SUPERVISED`, along with `services.yaml`, `jobs.yaml` and credentials.
Keep independent app endpoints running when their programs are unchanged.
Verify the fresh heartbeat, total/free resources, app states and saved progress
before following [verify and resume](#verify-and-resume).

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
submodule branch. See [data and ports](reference.md#data) for storage and
network details.
