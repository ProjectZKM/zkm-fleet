# Operations

Follow the [installation guide](installation.md) to set up the control host and
nodes. These commands use the control-plane API; no SSH connection to a node is
needed for app scaling.

Run these on the control host, where `fleetctl` reads the administrator's
`etc/admin.token` by default. Remote operators set `FLEET_URL` to the HTTPS
gateway and `FLEET_TOKEN_FILE` to their private administrator token file.
Namespace users set `FLEET_TOKEN_FILE` or `--token-file` to their own token.
Tokens with the `manage` permission change app and batch metadata; tokens with
`read` and `submit` submit and inspect permitted jobs. Scoped tokens can list and
inspect their own namespace, including its shared quota and their tag's quota and
usage. Node administration and namespace writes require an administrator token.
Add `--json` for structured output.

## Daily commands

The short list; everything else is in the sections below or in `fleetctl --help`.

```bash
fleetctl node                   # machines: capacity, READY, heartbeats, the agent's VERSION
fleetctl node NAME              # one machine's record
fleetctl app                    # every app with its state, allocation and node
fleetctl app logs APP -n 200    # an app's last log lines, read from where it runs
fleetctl job -a                 # queued and running jobs
fleetctl batch                  # batch workloads with their newest jobs
fleetctl node drain NAME        # hold new placements on a machine (undrain to release)
```

## Namespaces and access

A namespace owns apps and batches and shares one GPU/CPU quota across their
allocations. Scoped users can read their own namespace; administrators manage
ownership, access and quotas:

| Command | Purpose | Access |
|---|---|---|
| `fleetctl namespace` or `namespace list` | List namespace identities; JSON also includes configured quotas. | Scoped users: own namespace; administrators: all namespaces. |
| `namespace create NAME --endpoint URL --out NAME.env` | Create the namespace and its owner bundle for `scope=default`. | Administrator |
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
fleetctl namespace create alice --gpus 6 --cpus 64 \
  --endpoint http://127.0.0.1:51012 --out alice.env
fleetctl namespace get alice
fleetctl namespace quota alice --tag team=research --gpus 2 --cpus 16
fleetctl namespace get alice --tag team=research
```

Creation writes `alice.env`, `alice.token` and, when `--server-ca` is supplied,
`alice.ca.pem` beside the environment file. Give that bundle to the owner securely.
`--endpoint` is the bundle's SDK `ENDPOINT`, the owner's
[app-owned proof endpoint](reference.md#app-owned-proof-endpoint); `--url` selects Fleet's control API.
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
the namespace `zkm` owner's token:

```bash
fleetctl --token-file zkm-owner.token --namespace zkm app scale ethproofs-z4 \
  --gpus 5 --cpus 96 --replicas 1 --node ant-5090-2

# Read desired/observed allocation and operation status.
fleetctl --token-file zkm-owner.token --namespace zkm app scale ethproofs-z4
# Node inspection uses administrator credentials.
fleetctl node ant-5090-2
```

Scaling requires the `manage` permission and an online, ready node.
The token must cover the app's namespace, tag and app grant; for a proof app its
tag must cover every node entry running it. Namespace and tag quotas remain
administrator-managed. Requests are asynchronous; inspect until
the state is `complete` or `failed`. Omitted resource flags retain their current
values. Disabled apps stay disabled. Use `--node` to
select a machine when the app has multiple deployments.

Integrated EthProofs uses exactly one replica. Other apps may use multiple
replicas; resources are per replica. The current
`ant-5090-2` pool is 8 GPUs and 128 CPUs. With only EthProofs enabled at 5 GPUs
and 96 CPUs, 3 GPUs and 32 CPUs remain for other allocations. Inspect current
allocations with `fleetctl app scale`; these resource settings can change.
Existing allocations are accounted for when replacing an app's own allocation.

To change the node pool, edit `RAY_NUM_CPUS` and `CUDA_VISIBLE_DEVICES` in
`etc/watchdog.env`, then restart local Ray after pausing the node's workloads.
An empty `RAY_NUM_CPUS` enables automatic detection, and an empty
`CUDA_VISIBLE_DEVICES` gives the node no GPUs. Local Ray listens on ports 6379-6381,
52365, 8080 (metrics) and 10002-19999 (workers). On a machine where another service holds
one, set `RAY_METRICS_EXPORT_PORT`, `RAY_MIN_WORKER_PORT` and `RAY_MAX_WORKER_PORT` in
`etc/watchdog.env`. Scaling an app does not increase node capacity. Follow the
[node maintenance procedure](maintenance.md).

## Apps and batches

Apps and jobs show `TAG` and `STATE`. App state reports replica readiness;
job state reports an execution's progress. App details retain replica settings
and job counts. Tags use `key=value`, for example `scope=default` or `team=research`.
The same namespace/tag selectors work for apps, batches and jobs.

Owner tokens can create and remove workloads, set priority, enable/disable them
and scale apps within their grants. Batch owners set image, environment, resource
policy when registering a batch. Administrators set batch mount roots and runtime
users. App image, environment, command, user, workdir and volume changes also
require an administrator; owners can change resource sizes and timeouts. Batch disabling
blocks new submissions; accepted batch jobs continue. App disabling drains the
app's replicas before releasing their reservations.

When upgrading from owner-controlled runtime settings, review existing app
configuration, batch mount roots and runtime users, and queued jobs before
restoring untrusted owner access. The new authorization checks do not rewrite
configuration or cancel jobs accepted by an earlier scheduler.

```bash
fleetctl --namespace zkm --tag scope=default app
fleetctl --namespace pi --tag team=research batch
fleetctl --namespace pi --tag team=research job
```

```bash
fleetctl node
fleetctl --namespace zkm app
fleetctl --namespace zkm job
fleetctl --token-file zkm-owner.token --namespace zkm app disable ethproofs-z4
fleetctl --token-file zkm-owner.token --namespace zkm app enable ethproofs-z4
```

Create an app from a JSON file of its container settings, the keys of its
`apps.yaml` entry (`image`, `replicas`, `gpus`, `cpus`, `env`, and for a container app
`command` and `port_base`). A proof app is an app whose container is a Ziren prover: give
its program and proof policy too, and its spec's `kind` defaults to `ziren-gpu`
(`ziren-cpu` for a CPU prover):

```bash
fleetctl --token-file zkm-owner.token --namespace zkm app add model --spec model.json
fleetctl --token-file zkm-owner.token --namespace zkm app add eth --spec prover.json \
  --elf /path/to/program.elf --class latency --anchor exclusive --backend cuda
fleetctl --token-file zkm-owner.token --namespace zkm app enable model
```

An app starts disabled and on no node. Its first `app enable` places it: the scheduler
picks a ready node big enough for all its replicas, preferring one where they fit now,
has that node write the entry into its `apps.yaml`, then starts it. Users do not choose the node. App names are unique across
the fleet. The [reference](reference.md) lists every setting.

Disabling releases the app's execution resources after cleanup. Durable app data
remains. An interrupted EthProofs block is retried from its saved cursor;
see its [shutdown rules](../apps/ethproofs/README.md#operate-and-recover) to finish an
in-flight proof before stopping. `node drain` stops new Fleet proof/batch
placements; it does not pause an integrated app's own block stream.

For a batch, first register reusable configuration, then submit and inspect jobs:

```bash
fleetctl --token-file pi-owner.token --namespace pi batch add gpu-test \
  --image nvidia/cuda:12.8.0-base-ubuntu22.04 --priority 10
# An administrator approves host storage and its runtime user before jobs mount it.
fleetctl --namespace pi batch update gpu-test --allow /data/models --user 1000:1000
fleetctl --token-file pi-owner.token batch            # the batches, with active jobs and the last job
fleetctl --token-file pi-owner.token batch gpu-test   # one batch and its newest jobs
fleetctl --token-file pi-owner.token job --type batch -a   # the queued and running batch jobs
fleetctl --token-file pi-owner.token batch priority gpu-test 20
fleetctl --token-file pi-owner.token batch submit gpu-test --gpus 1 --cpus 2 \
  --mount /data/models:ro -- nvidia-smi
fleetctl --token-file pi-owner.token batch logs JOB_ID -f
fleetctl --token-file pi-owner.token batch cancel JOB_ID
```

The namespace must already exist. See the [reference](reference.md#administrator-workflow)
for namespace creation, token issuance and quotas. Use `fleetctl --help` and
subcommand `--help` for the complete command list. SDK clients use their app's
endpoint; Fleet CLI and MCP use the control-plane URL.

Batches are configured in node `etc/batches.yaml`; apps are configured in
`etc/apps.yaml`, where the scheduler also writes the apps it places. Both share namespace, tag, priority,
image and resource policy. Each batch job gets a workspace under
`FLEET_JOBS_DIR/JOB_ID/`, normally `~/app/fleet/work/jobs/JOB_ID/`.
The command's output directory is exposed as `FLEET_OUT`; retrieve artifacts
from application-owned storage. Fleet supplies status and logs.

## Logs and recovery

Read an app's log from anywhere with control-plane access. The scheduler asks the
node that runs the app through its next heartbeat, so nothing is stored centrally and
the answer takes a moment:

```bash
fleetctl app logs ethproofs-z4 -n 200         # the last 200 lines (default 100, at most 2000)
fleetctl app logs APP --node NODE             # when the app runs on several machines
```

It needs the same access as `fleetctl app scale APP` without changing anything: the app
must be granted to the token and within its tag. An offline node, or one that is busy for
longer than 15 seconds, answers with an error. Each reply carries at most
256 KiB, from the first replica's `<entry>-0.node.log` file in the node's log directory,
named after the app's `apps.yaml` entry.
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
