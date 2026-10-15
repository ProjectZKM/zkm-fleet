# Operations

Follow the [installation guide](installation.md) to set up the control host and
nodes. These commands use the control-plane API; no SSH connection to a node is
needed for app scaling.

Run these on the control host. Remote operators set `FLEET_URL` to the HTTPS
gateway and `FLEET_ADMIN_TOKEN_FILE` to their private administrator token file.
Namespace users use `FLEET_TOKEN_FILE`. Owner tokens (`manage_apps: true`) manage
app and batch metadata; ordinary app tokens submit and inspect permitted jobs.
Scoped tokens can list and inspect their own namespace, including its shared
quota and their tag's quota and usage. Node administration and namespace writes
require the administrator token. Clear
`FLEET_TOKEN_FILE` when switching to administrator commands: it takes precedence
over `FLEET_ADMIN_TOKEN_FILE`. Add `--json` for structured output.

## Namespaces and access

A namespace owns apps and batches and shares one GPU/CPU quota across their
allocations. Scoped users can read their own namespace; administrators manage
ownership, access and quotas:

| Command | Purpose | Access |
|---|---|---|
| `fleetctl namespace` or `namespace list` | List namespace identities; JSON also includes configured quotas. | Scoped users: own namespace; administrators: all namespaces. |
| `namespace create NAME --tenant TENANT --endpoint URL --out NAME.env` | Create the namespace and its owner bundle for `scope=default`. | Administrator |
| `namespace get NAME` | Inspect shared quota and usage, plus tag caps and usage. | Scoped users: own namespace/tag; administrators: any namespace/tag. |
| `namespace quota NAME --gpus N --cpus N` | Replace the namespace's shared limits. | Administrator |
| `namespace quota NAME --tag team=research --gpus N --cpus N` | Replace that tag's limits within the namespace. | Administrator |
| `namespace suspend NAME` | Block tokens and new submissions; accepted work continues. | Administrator |
| `namespace resume NAME` | Restore access; individually revoked tokens remain revoked. | Administrator |

Read your namespace with the same token used for app and batch commands:

```bash
fleetctl --token-file alice.token namespace
fleetctl --token-file alice.token namespace get alice
```

Names and immutable IDs are accepted. Scoped reads include namespace totals and
the token's tag; another namespace or tag selector is refused. Other tags' quota
settings and job allocation records are not exposed to scoped users.

For example:

```bash
fleetctl namespace create alice --tenant eth --gpus 6 --cpus 64 \
  --endpoint https://fleet.example:8905 --domain-name stage \
  --server-ca /etc/zkm-fleet/tls/ca.pem --out alice.env
fleetctl namespace get alice
fleetctl namespace quota alice --tag team=research --gpus 2 --cpus 16
fleetctl namespace get alice --tag team=research
```

Creation writes `alice.env`, `alice.token` and, when `--server-ca` is supplied,
`alice.ca.pem` beside the environment file. Give that bundle to the owner securely.
`--endpoint` is the proof SDK gateway; `--url` selects Fleet's control API.
Use an existing CA certificate here; `init tls` belongs to control-host installation.

Each `quota` command replaces both limits: an omitted limit becomes unlimited,
and `namespace quota NAME` clears both. Zero blocks additional resource allocation.
Quotas limit use of the node pool; [app scaling](#configure-resources) chooses an
app's allocation, and [node maintenance](maintenance.md) changes the pool itself.

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

For example, set the EthProofs worker's allocation to five GPUs and 96 CPUs using
the namespace `zkm` owner's token. The token's proof-signing tenant can differ
from the app's `ethproofs` tenant:

```bash
fleetctl --token-file zkm-owner.token --namespace zkm app scale ethproofs/334b45b8 \
  --gpus 5 --cpus 96 --replicas 1 --node ant-5090-2

# Read desired/observed allocation and operation status.
fleetctl --token-file zkm-owner.token --namespace zkm app scale ethproofs/334b45b8
# Node inspection uses administrator credentials.
fleetctl node ant-5090-2
```

Scaling requires app-owner access (`manage_apps: true`) and an online, ready node.
The token must cover the app's namespace, tag and app grant. An owner's signing
tenant does not restrict metadata or scaling. Namespace and tag quotas remain
administrator-managed. Requests are asynchronous; inspect until
the state is `complete` or `failed`. Omitted resource flags retain their current
values. Disabled apps stay disabled. Use `--node` to
select a machine when the app has multiple deployments.

Integrated EthProofs uses exactly one replica. SDK-driven proof apps and serving
apps may use multiple replicas; resources are per replica. The current
`ant-5090-2` pool is 8 GPUs and 128 CPUs. With only EthProofs enabled at 5 GPUs
and 96 CPUs, 3 GPUs and 32 CPUs remain for other allocations. Inspect current
allocations with `fleetctl app scale`; these resource settings can change.
Existing allocations are accounted for when replacing an app's own allocation.

To change the node pool, edit `RAY_NUM_CPUS` and `CUDA_VISIBLE_DEVICES` in
`etc/watchdog.env`, then restart local Ray after pausing the node's workloads.
An empty `RAY_NUM_CPUS` enables automatic detection. Scaling an app does not
increase node capacity. Follow the [node maintenance procedure](maintenance.md).

## Apps and batches

Apps and jobs show `TAG` and `STATE`. App state reports service readiness;
job state reports an execution's progress. App details retain replica settings
and job counts. Tags use `key=value`, for example `scope=default` or `team=research`.
The same namespace/tag selectors work for apps, batch profiles and jobs.

Owner tokens can register and remove workloads, set priority, enable/disable them
and scale apps within their grants. Batch owners set image, environment, resource
policy and allowed mount roots when registering a profile. Profile disabling
blocks new submissions; accepted batch jobs continue. App disabling drains the
configured services before releasing their reservations.

```bash
fleetctl --namespace zkm --tag scope=default app
fleetctl --namespace pi --tag team=research batch
fleetctl --namespace pi --tag team=research job
```

```bash
fleetctl node
fleetctl --namespace zkm app
fleetctl --namespace zkm job
fleetctl --token-file zkm-owner.token --namespace zkm app disable ethproofs/334b45b8
fleetctl --token-file zkm-owner.token --namespace zkm app enable ethproofs/334b45b8
```

Disabling releases the app's execution resources after cleanup. Durable app data
remains. An interrupted EthProofs block is retried from its saved cursor;
see its [shutdown rules](../apps/ethproofs/README.md#operate-and-recover) to finish an
in-flight proof before stopping. `node drain` stops new Fleet proof/batch
placements; it does not pause an integrated app's own block stream.

For a batch, first register reusable configuration, then submit and inspect jobs:

```bash
fleetctl --token-file pi-owner.token --namespace pi batch add gpu-test \
  --image nvidia/cuda:12.8.0-base-ubuntu22.04 --allow /data/models --priority 10
fleetctl --token-file pi-owner.token batch            # the profiles, with active jobs and the last job
fleetctl --token-file pi-owner.token batch gpu-test   # one profile and its newest jobs
fleetctl --token-file pi-owner.token job --type batch -a   # the queued and running batch jobs
fleetctl --token-file pi-owner.token batch priority gpu-test 20
fleetctl --token-file pi-owner.token batch submit --app gpu-test --gpus 1 --cpus 2 \
  --mount /data/models:ro -- nvidia-smi
fleetctl --token-file pi-owner.token batch logs JOB_ID -f
fleetctl --token-file pi-owner.token batch cancel JOB_ID
```

The namespace must already exist. See the [reference](reference.md#administrator-workflow)
for namespace creation, token issuance and quotas. Use `fleetctl --help` and
subcommand `--help` for the complete command list. SDK clients use their app's
endpoint; Fleet CLI and MCP use the control-plane URL.

Reusable batch templates are stored in node `etc/jobs.yaml`; app deployments
are configured in `etc/services.yaml`. Both share namespace, tag, priority,
image and resource policy. Each batch gets a workspace under
`FLEET_JOBS_DIR/JOB_ID/`, normally `~/app/fleet/work/jobs/JOB_ID/`.
The command's output directory is exposed as `FLEET_OUT`; retrieve artifacts
from application-owned storage. Fleet supplies status and logs.

## Logs and recovery

Read an app's log from anywhere with control-plane access. The scheduler asks the
node that runs the app through its next heartbeat, so nothing is stored centrally and
the answer takes a moment:

```bash
fleetctl app logs ethproofs/ELF_ID -n 200     # the last 200 lines (default 100, at most 2000)
fleetctl app logs APP --node NODE             # when the app runs on several machines
```

It needs the same access as `fleetctl app scale APP` without changing anything: the app
must be granted to the token and within its tag. An offline node, or one that is busy for
longer than 15 seconds, answers with an error. Each reply carries at most
256 KiB, from the first replica's `<service>-0.node.log` file in the node's log directory.
MCP clients use `fleet_app_logs`. A node must run this release for the command to work.

When `fleetctl app` shows an app as `starting`, DETAIL says why: `waiting for capacity`
with what it needs and what its node has free (the start is held until it fits; `app
disable` or a smaller `app scale` replaces the held start), `starting on NODE` once the node
has the request, or `waiting for NODE to report it ready`.

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
