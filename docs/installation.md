# Installation

`deploy/setup.sh` downloads a GitHub Release package, verifies its SHA-256
checksums and runs `deploy/install.sh` with the packaged binaries.

| Machine | Required software |
|---|---|
| Control host | Docker with Compose v2, curl |
| Execution node | Docker, Python ≥ 3.10 with `venv`, cron, curl |
| GPU node | Execution node software, NVIDIA driver ≥ 570, NVIDIA container toolkit |

## Control host

```bash
curl -fsSL https://raw.githubusercontent.com/ProjectZKM/zkm-fleet/v0.1.6/deploy/setup.sh | bash -s -- \
  --version v0.1.6 control --public-url https://fleet.example:8905 \
  --tls-names stage,fleet.example --prefix "$HOME/app/fleet" --etc "$HOME/app/fleet/etc"
```

This pulls the release's scheduler image from GHCR and starts the scheduler, Postgres, the HTTPS
gateway and MCP with Docker Compose. Add `--no-mcp` to leave MCP out. The
installer creates the cluster token, a private TLS CA and the first
administrator token in `etc/admin.token`, and puts `fleetctl` on the PATH.
Scheduler settings are in `etc/fleet.yaml`
(see [fleet.example.yaml](../deploy/fleet.example.yaml)).

## Execution node

Copy the control host's `etc/cluster.token` to the node (mode 600), and its
`etc/tls/ca.pem` when using the private CA. Then run on the node:

```bash
curl -fsSL https://raw.githubusercontent.com/ProjectZKM/zkm-fleet/v0.1.6/deploy/setup.sh | bash -s -- \
  --version v0.1.6 node --name gpu-1 --scheduler https://fleet.example:8905 \
  --cluster-token "$HOME/cluster.token" --scheduler-ca "$HOME/ca.pem" \
  --prefix "$HOME/app/fleet" --etc "$HOME/app/fleet/etc" --cuda 0,1,2,3 --cpus 32
```

Omit `--cuda` for a CPU-only node and `--cpus` to let Ray detect capacity. The
node needs outbound HTTPS (HTTP/2) to port 8905. A cron watchdog keeps Ray and
the agent running; pass `--no-cron` to supervise `deploy/node-start.sh` yourself.
Configure the node's apps and batches in `etc/apps.yaml` and `etc/batches.yaml`
(see the [app](../deploy/apps.example.yaml) and
[batch](../deploy/batches.example.yaml) examples).

Verify on the control host:

```bash
fleetctl node gpu-1
```

## Options

Fetch `setup.sh` from the release you are installing: the script and the package it
downloads move together. Omit `--version` to install the latest release.
`--keep-package` keeps the downloaded package for inspection. To install from a
source checkout instead, build the binaries and run `deploy/install.sh control|node
--bin target/release` with the same options.

See [operations](operations.md) for workloads, and [maintenance](maintenance.md)
for upgrades, day-2 work and the one-time migrations between older releases.
