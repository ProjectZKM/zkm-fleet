# zkm-fleet

[![CI](https://github.com/ProjectZKM/zkm-fleet/actions/workflows/ci.yml/badge.svg)](https://github.com/ProjectZKM/zkm-fleet/actions/workflows/ci.yml)

Fleet runs apps and batch jobs on a shared pool of GPU and CPU machines. A control host
decides what runs where; each machine runs it in containers through
[trusted-shim](#how-it-works).

## What it runs

- **Apps** keep replicas running: a model server, a web service, or a Ziren prover
  (`kind: ziren-gpu` or `ziren-cpu`) with its program.
- **Batches** queue finite jobs. Each job runs once on one machine and frees its
  resources when its cleanup is confirmed.

Work belongs to a **namespace**. Its owners create apps and batches and set their
priority, enabled state and scale, within the quotas of their namespace and tag.
Administrators manage namespaces, quotas, access, host storage and each app's execution
settings. The [code model](docs/reference.md#code-names) follows Kubernetes' jobs,
deployments and controllers, without Kubernetes.

## How it works

![Fleet architecture: control host, data router and execution machines](images/diagram.svg)

The numbers in the diagram match these steps.

1. **Clients** (`fleetctl`, the proof SDK, the HTTP API, optional MCP) call the control
   host over authenticated HTTPS on port 8905.
2. **The scheduler** admits work against quotas and priority, picks a node with room and
   keeps the records. It drains an app before it changes or stops it.
3. **Each node** runs `fleet-node`, its own local Ray and the bridge. The agent sends
   heartbeats out to the control host, and the replies carry its commands. Nothing
   connects in to a node, and adding one needs only the control-plane URL and the
   cluster token.
4. **On the node**, the bridge reserves CPUs, GPUs and memory in Ray and runs every app
   replica and batch job in a container of trusted-shim, on its Docker, OpenShell, TEE or
   TEE-simulation driver. The shim runs as its own service, so a node upgrade leaves
   running containers alone.
5. **App traffic** reaches an app only through the
   [data router](https://github.com/attestx/trusted-data-router/tree/main/deploy), a
   separate deployment: an app's ports are published on node loopback, and the router
   carries its declared services over the node's outbound tunnel. A prover's node entry
   can instead set `network: host` to listen on the node's own port.
6. **Data** stays with the app. Fleet copies nothing an app keeps on a node's disks, so an
   app that keeps data on a node is anchored there with `machine` in that node's
   `apps.yaml`.

| Where | Component | Role |
|---|---|---|
| Control host (one Compose project) | `fleet-scheduler` + Postgres | Admission, quotas, placement, lifecycle and records |
| | HTTPS gateway | TLS for clients and nodes on port 8905 |
| | `fleet-mcp`, OpenShell gateway, Trustee AS | Optional Compose profiles |
| Every node | `fleet-node`, local Ray, `bridge/fleet_bridge` | Heartbeats, resource grants, launches and confirmed cleanup |
| | `trusted-shim-runtime` (+ Chisel tunnel helper) | Runs the containers; carries the data router's streams |
| | `fleet-proof-app`, `fleet-ziren-node` | Optional: a proof app's SDK endpoint, and fetching its stored proofs |
| Elsewhere | Data router | Routes clients to apps' declared services |

[Reference: architecture](docs/reference.md#architecture) describes each part in detail.

## Apps in this repository

| App | What it does |
|---|---|
| [EthProofs](apps/ethproofs/README.md) | Prepares witnesses, proves and reports every Ethereum block in sequence, resuming from its saved cursor |
| [GOAT proof builder](apps/goat/README.md) | Serves the Bitcoin node proof API, proves its five circuits and keeps its task database and proofs in app storage |

Both run their worker beside a Ziren GPU server in the stock GPU image, with
[`apps/gpu-worker.sh`](apps/gpu-worker.sh) as the container's entrypoint.

## Access, policy and TEEs

- **OpenShell** is the access layer for the Docker and TEE drivers; without user rules
  it adds no limits beyond the driver's. Its gateway is the `openshell` Compose profile
  ([installation](docs/installation.md#openshell-gateway)).
- **Policy**: the scheduler can embed Rail's policy evaluation, with proofs from a remote
  proof guest ([central policy ledger](docs/reference.md#central-policy-ledger),
  [upgrades](docs/maintenance.md#policy-evaluator)).
- **TEE nodes** check CoCo evidence and a remote attestation receipt in the shim. TDX
  DCAP admission verifies the original evidence in a Ziren guest (Automata DCAP);
  SNP and sample profiles use the optional Trustee AS
  ([TEE runtimes](docs/reference.md#node-configured-tee-runtimes)). Docker workloads are
  not attested.

## Get started

1. [Install the control host and add a node](docs/installation.md).
2. [Create namespaces, scale apps and run batches](docs/operations.md).
3. [Upgrade, change capacity and recover](docs/maintenance.md).

| Guide | Covers |
|---|---|
| [Installation](docs/installation.md) | Dependencies, the control host, execution nodes and their shim service, registration |
| [Operations](docs/operations.md) | Daily commands, namespaces and tags, node capacity, apps, batches and logs |
| [Maintenance](docs/maintenance.md) | Upgrades and rollback, capacity changes, self-built programs, liveness and troubleshooting |
| [Reference](docs/reference.md) | Architecture, workloads, proofs, access, quotas, APIs, data, ports and TLS |
