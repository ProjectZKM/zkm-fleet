# zkm-fleet

[![CI](https://github.com/ProjectZKM/zkm-fleet/actions/workflows/ci.yml/badge.svg)](https://github.com/ProjectZKM/zkm-fleet/actions/workflows/ci.yml)

Fleet schedules apps and batch jobs on shared GPU/CPU machines.

- **Apps** keep proof workers or serving containers running.
- **Batches** run finite commands and release resources when cleanup finishes.

## Architecture

![Fleet architecture: apps and batches share machine resources](images/diagram.svg)

The control host runs the Rust scheduler, Postgres and an HTTPS gateway. MCP is
optional. Each execution machine runs `fleet-node` and its own local Ray instance.
Agents connect outward and register automatically. Fleet manages admission,
quotas, placement and lifecycle; workers own their application data and pipelines.
The [code model](docs/reference.md#code-names) uses jobs, app deployments and a
workload controller. Shared records come from protobuf; Kubernetes is not required.

App owners manage workload metadata, mount roots, priority, enable/disable and
scaling with namespace tokens holding the `manage` permission. Administrators
manage namespace quotas and access; owner changes remain subject to namespace and
tag limits.

EthProofs prepares witnesses, proves locally and reports from one integrated app
worker. It resumes its saved cursor and proves every block in sequence.
GOAT keeps `bitvm-node / proof-builder-rpc → zkm-sdk → Fleet gateway → proving`.
TODO: evaluate moving GOAT preparation into an app worker after the EthProofs
migration is validated.

## Get started

1. [Install the control host and add a node](docs/installation.md).
2. [Inspect resources, scale apps and run batches](docs/operations.md).
3. [Adjust machine capacity, update and recover](docs/maintenance.md).

## Documentation

| Guide | Covers |
|---|---|
| [Installation](docs/installation.md) | Dependencies, binaries, control host, execution nodes and registration |
| [Operations](docs/operations.md) | Namespace commands, tags, node capacity, app scaling, batches and logs |
| [Maintenance](docs/maintenance.md) | Pause/resume, Ray capacity changes, upgrades, runtime files and backups |
| [Reference](docs/reference.md) | Protocols, namespaces, tokens, quotas, APIs, storage and ports |
| [EthProofs](apps/ethproofs/README.md) | Pinned source, worker build, sequential proving and durable recovery |
