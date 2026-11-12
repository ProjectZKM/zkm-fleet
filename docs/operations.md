# Operations

Day-to-day work with `fleetctl`, once the [installation](installation.md) is done.
Nothing here needs SSH to a node.

On the control host, `fleetctl` uses the administrator's `etc/admin.token`.
Elsewhere, set `FLEET_URL` to the HTTPS gateway and `FLEET_TOKEN_FILE` (or
`--token-file`) to your token: an administrator's, or a namespace user's own. A
user token works within its namespace and tag, with what its permissions allow
([reference](reference.md#permissions)). Add `--json` for structured output.

## Daily commands

```bash
fleetctl node                   # machines: capacity, READY, heartbeats, the agent's VERSION
fleetctl node NAME              # one machine's record
fleetctl app                    # every app with its state, allocation and node
fleetctl app logs APP -n 200    # an app's last log lines, read from where it runs
fleetctl job -a                 # queued and running batch jobs and submitted proofs
fleetctl batch                  # batches with their newest jobs
fleetctl node drain NAME        # hold new placements on a machine (undrain to release)
```

`fleetctl --help` and each subcommand's `--help` list the rest.

## Namespaces and access

A namespace owns apps and batches and shares one GPU/CPU quota across them.

| Command | Does | Who |
|---|---|---|
| `fleetctl namespace` (`namespace list`) | List namespaces; JSON adds their quotas | Users see their own |
| `namespace create NAME --endpoint URL --out NAME.env` | Create it and its owner bundle for `scope=default` | Administrator |
| `namespace get NAME [--tag TAG]` | Show the quota and use, and the tags' caps and use | Users see their own namespace and tag |
| `namespace quota NAME [--tag TAG] --gpus N --cpus N` | Replace the namespace's or a tag's caps | Administrator |
| `namespace suspend NAME` / `resume NAME` | Block or restore its tokens; accepted work continues, and revoked tokens stay revoked | Administrator |

```bash
fleetctl namespace create alice --gpus 6 --cpus 64 \
  --endpoint http://127.0.0.1:51012 --out alice.env
fleetctl namespace quota alice --tag team=research --gpus 2 --cpus 16
fleetctl --token-file alice.token namespace get alice
```

Creating writes `alice.env`, `alice.token` and, with `--server-ca`, `alice.ca.pem`
beside it; give the bundle to the owner securely. `--endpoint` is the bundle's SDK
`ENDPOINT`, the owner's [app-owned proof endpoint](reference.md#app-owned-proof-endpoint),
and `--url` is Fleet's control API. Pass an existing CA certificate: `init tls` is for
installing the control host.

Each `quota` command replaces both caps: an omitted cap is unlimited, zero allows
none, and `namespace quota NAME` alone clears both. Quotas limit a namespace's share
of the node pool; [scaling](#configure-resources) sets an app's allocation, and
[maintenance](maintenance.md#changing-a-nodes-capacity) changes the pool. Tokens,
tags and quotas in full are in the [reference](reference.md#namespaces-tags-and-tokens).

## Configure resources

| Setting | Controls | Change it with |
|---|---|---|
| Node capacity | The total pool Fleet may allocate on a machine | Installer `--cuda` / `--cpus`, kept in `etc/watchdog.env` |
| App allocation | GPUs and CPUs per replica, and the replica count | `fleetctl app scale` |

CPU units are logical CPUs, and Ray's pool can be smaller than the hardware.
`fleetctl node` shows total and free GPUs and CPUs; app listings show configured
allocations, disabled apps included, not live use. Namespace and tag quotas cap
allocations further.

```bash
fleetctl --token-file zkm-owner.token --namespace zkm app scale ethproofs-z4 \
  --gpus 5 --cpus 96 --replicas 1
fleetctl --token-file zkm-owner.token --namespace zkm app scale ethproofs-z4   # desired, observed, status
fleetctl --token-file zkm-owner.token --namespace zkm app scale ethproofs-z4 --node gpu-1   # that node's part
```

Scaling needs `manage` on the app and an online, ready node. It runs in the
background: check it until it's `complete` or `failed`. Omitted flags keep their
values, and a disabled app stays disabled. Replicas, GPUs and CPUs are the app's
settings, not a node's: `--replicas` is how many the app runs wherever the scheduler
places them, so a scale takes no `--node` (it only narrows what a check shows). When
an app runs on several nodes, each keeps the replicas it has up to the count, new
ones join a node that runs it, and a node left with none stops them and
removes the app from its `apps.yaml`, which frees its room; the app's data stays on
that node. A node that comes back with a copy of an app whose replicas all run
elsewhere removes it too, unless its entry is anchored to the machine (`machine`).
Integrated EthProofs runs exactly one replica; other apps may run more, with
resources per replica. The app's current allocation counts toward the room for its
new one. Scaling never adds node capacity.

A node's pool is `RAY_NUM_CPUS` and `CUDA_VISIBLE_DEVICES` in its `etc/watchdog.env`;
empty means detect CPUs automatically, and no GPUs. Changing it restarts Ray, so
follow the [maintenance procedure](maintenance.md#changing-a-nodes-capacity). Local
Ray listens on ports 6379–6381, 52365, 8080 (metrics) and 10002–19999 (workers);
where another service holds one, set `RAY_METRICS_EXPORT_PORT`,
`RAY_MIN_WORKER_PORT` and `RAY_MAX_WORKER_PORT` there.

## Apps and batches

App and job tables both show `TAG` and `STATE`: an app's state is its replicas'
readiness, a job's its progress. The same `--namespace` and `--tag` selectors work
for apps, batches and jobs.

```bash
fleetctl --namespace zkm --tag scope=default app
fleetctl --namespace pi --tag team=research batch
fleetctl --namespace pi --tag team=research job
```

### Apps

Create an app from a JSON file of its container settings, the keys of its
`apps.yaml` entry: `image`, `replicas`, `gpus`, `cpus`, `env`, and for a container
`command` and `port_base`. A proof app's container is a Ziren prover: give its
program and proof policy too, and its `kind` defaults to `ziren-gpu` (or set
`ziren-cpu`). The [reference](reference.md#apps) lists every setting.

```bash
fleetctl --token-file zkm-owner.token --namespace zkm app add model --spec model.json
fleetctl --token-file zkm-owner.token --namespace zkm app add eth --spec prover.json \
  --elf /path/to/program.elf --class latency --anchor exclusive --backend cuda
fleetctl --token-file zkm-owner.token --namespace zkm app enable model
fleetctl --token-file zkm-owner.token --namespace zkm app disable model
```

An app starts disabled and on no node. Its first `app enable` places it: the
scheduler picks a ready node big enough for all its replicas, preferring one where
they fit now, has that node add the entry to its `apps.yaml`, and starts it. Users
don't choose the node, and app names are unique across the fleet.

Disabling drains the replicas and releases their resources after cleanup; the app's
durable data stays. An interrupted EthProofs block is retried from its saved cursor,
and its [shutdown rules](../apps/ethproofs/README.md#operate-and-recover) say how to
finish an in-flight proof first. `node drain` stops new proof and batch placements,
but not an integrated app's own block stream.

### Batches

Register a batch's image and policy once, then submit jobs to it:

```bash
fleetctl --token-file pi-owner.token --namespace pi batch add gpu-test \
  --image nvidia/cuda:12.8.0-base-ubuntu22.04 --priority 10
# An administrator approves host storage and a runtime user before jobs mount it:
fleetctl --namespace pi batch update gpu-test --allow /data/models --user 1000:1000
fleetctl --token-file pi-owner.token batch submit gpu-test --gpus 1 --cpus 2 \
  --mount /data/models:ro -- nvidia-smi
fleetctl --token-file pi-owner.token batch gpu-test             # the batch and its newest jobs
fleetctl --token-file pi-owner.token job --type batch -a        # queued and running batch jobs
fleetctl --token-file pi-owner.token batch logs JOB_ID -f
fleetctl --token-file pi-owner.token batch cancel JOB_ID
fleetctl --token-file pi-owner.token batch priority gpu-test 20
```

The namespace must exist first ([administrator workflow](reference.md#administrator-workflow)).
Each job gets a workspace in its node's `FLEET_JOBS_DIR/JOB_ID/`, normally
`~/app/fleet/work/jobs/JOB_ID/`, with `FLEET_OUT` as its output directory; Fleet
supplies status and logs, and the job's results live in the owner's storage. Nodes
can also configure batches in `etc/batches.yaml`, as apps in `etc/apps.yaml`, where
the scheduler writes the apps it places. SDK clients use their app's endpoint;
`fleetctl` and MCP use the control-plane URL.

## Logs and recovery

Read an app's log from anywhere with control-plane access. The scheduler asks the
app's node in its next heartbeat, so nothing is stored centrally and the answer takes
a moment:

```bash
fleetctl app logs ethproofs-z4 -n 200         # the last 200 lines (default 100, at most 2000)
fleetctl app logs APP --node NODE             # when the app runs on several machines
```

It needs the same access as `fleetctl app scale APP` and changes nothing. An offline
node, or one busy for more than 15 seconds, answers with an error. A reply carries at
most 256 KiB, from the first replica's `<entry>-0.node.log` in the node's log
directory. MCP clients use `fleet_app_logs`.

When `fleetctl app` shows an app `starting`, DETAIL says why:

- `waiting for capacity`, with what it needs and what its node has free: the start
  is held until it fits, and `app disable` or a smaller `app scale` replaces it;
- `starting on NODE`, once the node has the request;
- `waiting for NODE to report it ready`.

On the node itself:

```bash
tail -f ~/app/fleet/work/logs/node.log
journalctl -u zkm-fleet-node.service -f              # with a systemd supervisor
tail -f ~/app/fleet/work/logs/ethproofs-v5-gpu-0.node.log   # the EthProofs worker
```

For an offline node, check its supervisor, `ray.log`, `watchdog.log`, the gateway
URL, the cluster token and CA trust. For a stalled app, read its worker or container
logs and reporting errors. EthProofs retries failed blocks: diagnose the error rather
than move the cursor ahead. [Maintenance](maintenance.md#troubleshooting) has more.
