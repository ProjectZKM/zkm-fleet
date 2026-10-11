# Installation

Run build and installation commands from the repository root. Use prebuilt
binaries with `--bin DIR`, or build them for each machine's operating system:

```bash
cargo build --locked --release --bins
```

For an execution agent alone, build `cargo build --locked --release -p fleet-node`.
Generic Fleet builds and installations do not need the EthProofs submodule.

| Machine | Required software |
|---|---|
| Control host | Docker and Docker Compose |
| Execution node | Docker, Python ≥ 3.10 with `venv`, cron; installer adds Ray and PyYAML |
| GPU node | Execution dependencies, NVIDIA driver ≥ 570 and NVIDIA container toolkit |

SDK-driven proving also needs `fleet-ziren-node`, built explicitly with
`cargo build --locked --release -p fleet-ziren-node` and its upstream build
dependencies. Build `fleet-proof-app` where an SDK endpoint is hosted.
Integrated workers package their own app binary and GPU image.

## Control host

```bash
deploy/install.sh control --public-url https://fleet.example:8905 \
  --tls-names stage,fleet.example --no-mcp \
  --prefix "$HOME/app/fleet" --etc "$HOME/app/fleet/etc"
```

Omit `--no-mcp` to enable MCP at `/mcp`. The installer creates credentials and puts
`fleetctl` on the control host's PATH. Operator commands use the administrator
token; namespace users receive their own scoped tokens.

Use [fleet.example.yaml](../deploy/fleet.example.yaml) for the scheduler settings
and proof tenant configuration. Fleet creates schema 14 for a fresh database;
an incompatible database requires a metadata reset and new namespace/token setup.

The installer runs `fleetctl init token` and `fleetctl init tls` only when their
files are missing. `init tls` creates a private CA and server certificate for the
Fleet backend; `--tls-names` supplies the certificate's hostnames and IPs.
These are installation commands. Normal namespace operations use the existing
server certificate and copy its public CA into client bundles when needed.

## Execution node

Securely copy the existing control host's `etc/cluster.token` to the new machine,
readable only by the installation account. That account needs access to Docker.
For a private gateway CA, also copy its public CA certificate. Run on the node:

```bash
deploy/install.sh node --name gpu-2 \
  --scheduler https://fleet.example:8905 \
  --cluster-token "$HOME/cluster.token" \
  --prefix "$HOME/app/fleet" --etc "$HOME/app/fleet/etc" \
  --cuda 0,1,2,3 --cpus 32
```

Add `--scheduler-ca ca.pem` for a private CA. Omit `--cuda` for a CPU-only node.
Omit `--cpus` to let Ray detect CPU capacity. Use `--bin DIR` for prebuilt binaries.
The node needs outbound HTTPS with HTTP/2 on port 8905.

The installer starts Ray and the agent with a cron watchdog. Use `--no-cron` when
providing your own supervisor for `deploy/node-start.sh` and local Ray.
It preserves existing app and batch configuration. A new node starts empty:
configure `etc/services.yaml` and `etc/jobs.yaml` using the
[app example](../deploy/services.example.yaml) and
[batch example](../deploy/jobs.example.yaml).

On the control host, verify registration:

```bash
fleetctl node
fleetctl node gpu-2
```

Expect the selected capacity, a fresh heartbeat and ready status. No separate
node registration command is required.

After registration, use the [operations guide](operations.md) to inspect nodes,
configure workloads and adjust app allocations. Use [node maintenance](maintenance.md)
when changing an existing machine’s Ray capacity.
