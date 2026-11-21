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

### Nodes with persistent shim services

For a package containing `trusted-shim-runtime`, add `--with-shim` to the node
installation command. It requires systemd and sudo access. This installs three
independent units: `zkm-fleet-node.service`, `zkm-fleet-ray.service`, and
`zkm-fleet-shim.service`; it does not install a new cron entry. Existing watchdog
entries read `FLEET_NODE_SUPERVISED=1` from the same `watchdog.env`.

The installer adds `shim_binary`, `shim_state_dir`, and `shim_daemon: true` to
the existing `apps.yaml` only when shim has not been configured. The executable
is pinned under `libexec/trusted-shim/<sha256>/`. Reinstalling stages new artifacts
without switching the running shim or restarting its service. Keep the state path
short enough that `state/control.sock` is under Linux's 108-byte Unix socket limit.
Configure `shim_network` in that same file for service forwarding; runtime drivers
and network credentials retain their existing validation and authorization.

Legacy apps keep their executor. An existing stdio shim allocation also keeps its
transport and executable: the installer refuses to silently convert it to daemon
mode. Introduce daemon mode on a new node, or explicitly drain and clean up those
shim allocations first. Existing Ray processes are left running; moving Ray itself
under a different service supervisor is a separate maintenance operation.

## Options

Fetch `setup.sh` from the release you are installing: the script and the package it
downloads move together. Omit `--version` to install the latest release.
`--keep-package` keeps the downloaded package for inspection. To install from a
source checkout instead, build the binaries and run `deploy/install.sh control|node
--bin target/release` with the same options.

See [operations](operations.md) for workloads, and [maintenance](maintenance.md)
for upgrades, day-2 work and the one-time migrations between older releases.
