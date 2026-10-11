# Operations

Follow the [installation guide](installation.md) to set up the control host and
nodes. These commands use the control-plane API; no SSH connection to a node is
needed for app scaling.

Run these on the control host. Remote operators set `FLEET_URL` to the HTTPS
gateway and `FLEET_ADMIN_TOKEN_FILE` to their private administrator token file.
Namespace users use `FLEET_TOKEN_FILE`. Add `--json` for structured output.

## Configure resources

There are two limits:

| Setting | Controls | Change it with |
|---|---|---|
| Node capacity | Total pool Fleet may allocate on that machine | Installer `--cuda` / `--cpus`, recorded in `etc/watchdog.env` |
| App allocation | GPUs and CPUs reserved per replica | `fleetctl app scale` |

CPU units are logical CPUs. Ray's configured pool can be smaller than the hardware.
`fleetctl node` shows total and currently free GPUs/CPUs. App listings show
configured allocations, including disabled apps; they do not show live utilization.
Namespace and optional tag quotas further limit allocation.

For example, allocate six GPUs and 64 CPUs to the existing EthProofs worker:

```bash
fleetctl --namespace zkm app scale ethproofs/334b45b8 \
  --gpus 6 --cpus 64 --replicas 1 --node ant-5090-2

# Read desired/observed allocation and operation status.
fleetctl --namespace zkm app scale ethproofs/334b45b8
fleetctl node ant-5090-2
```

Scaling requires administrator access and an online, ready node. Requests are
asynchronous; inspect until the state is `complete` or `failed`. Omitted resource
flags retain their current values. Disabled apps stay disabled. Use `--node` to
select a machine when the app has multiple deployments.

Integrated EthProofs uses exactly one replica. SDK-driven proof apps and serving
apps may use multiple replicas; resources are per replica. An 8-GPU/64-CPU node
with EthProofs at 6 GPUs/64 CPUs has 2 GPUs and 0 CPUs free for other allocations.
Existing allocations are accounted for when replacing an app's own allocation.

To change the node pool, edit `RAY_NUM_CPUS` and `CUDA_VISIBLE_DEVICES` in
`etc/watchdog.env`, then restart local Ray after pausing the node's workloads.
An empty `RAY_NUM_CPUS` enables automatic detection. Scaling an app does not
increase node capacity. Follow the [node maintenance procedure](maintenance.md).

## Apps and batches

```bash
fleetctl node
fleetctl --namespace zkm app
fleetctl --namespace zkm job
fleetctl --namespace zkm app disable ethproofs/334b45b8
fleetctl --namespace zkm app enable ethproofs/334b45b8
```

Disabling releases the app's execution resources after cleanup. Durable app data
remains. An interrupted EthProofs block is retried from its saved cursor;
see its [shutdown rules](../apps/ethproofs/README.md#operate-and-recover) to finish an
in-flight proof before stopping. `node drain` stops new Fleet proof/batch
placements; it does not pause an integrated app's own block stream.

For a batch, first register reusable configuration, then submit and inspect jobs:

```bash
fleetctl --namespace pi batch add gpu-test --image nvidia/cuda:12.8.0-base-ubuntu22.04
fleetctl --namespace pi batch submit --app gpu-test --gpus 1 --cpus 2 -- nvidia-smi
fleetctl --namespace pi batch logs JOB_ID -f
fleetctl --namespace pi batch cancel JOB_ID
```

The namespace must already exist. See the [reference](reference.md#administrator-workflow)
for namespace creation, token issuance and quotas. Use `fleetctl --help` and
subcommand `--help` for the complete command list. SDK clients use their app's
endpoint; Fleet CLI and MCP use the control-plane URL.

## Logs and recovery

Inspect ordinary logs on the node:

```bash
tail -f ~/app/fleet/work/logs/node.log
# With a systemd supervisor:
journalctl -u zkm-fleet-node.service -f
# EthProofs worker:
tail -f ~/app/fleet/work/logs/ethproofs-v5-gpu-0.node.log
```

For an offline node, check its supervisor, `ray.log`, `watchdog.log`, gateway URL,
cluster token and CA trust. For a stalled app, inspect its worker/container logs
and reporting errors. EthProofs retries failed blocks; diagnose the error instead
of changing the cursor to skip ahead.
