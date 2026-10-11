# Node maintenance

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
RAY_NUM_CPUS=64
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

## Installed files and backups

Keep source/build files separate from the installed runtime. A node installation
uses these directories:

| Directory | Contents |
|---|---|
| `bin/`, `bridge/`, `venv/`, `deploy/` | Installed Fleet programs and launch scripts |
| `apps/` | Packaged application binaries and guest ELFs |
| `etc/` | Machine configuration and private credentials |
| `work/` | Ray files, logs, batch workspaces and durable app data |
| `backup/` | Current rollback snapshot and maintenance records |

Before an update, record running allocations, pause affected workloads and back
up configuration plus durable app data. Install a tested revision, restart the
required processes, then verify readiness, resumed progress and report delivery.

Back up control-host Postgres and its `data_dir` together; batch records and app
intents live in that directory. Preserve node app checkpoints, prepared inputs,
outputs and pending reports. Keep tokens private. Delete obsolete build packages
and old logs only after checking running processes and rollback needs; keep
proof retention an explicit app policy.

Pin validated GPU images by digest. Update EthProofs using its committed submodule
revision with `git submodule update --init --recursive`; never deploy a moving
submodule branch. See [data and ports](reference.md#data) for storage and
network details.
