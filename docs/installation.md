# Installation

Run build and installation commands from the repository root. Use prebuilt
binaries with `--bin DIR`, or build them for each machine's operating system:

```bash
cargo build --locked --release \
  -p fleet-scheduler -p fleetctl -p fleet-mcp -p fleet-node
```

For an execution agent alone, build `cargo build --locked --release -p fleet-node`.
Generic Fleet builds and installations do not need the app submodules under
`third_party/`.

| Machine | Required software |
|---|---|
| Control host | Docker and Docker Compose |
| Execution node | Docker, Python ≥ 3.10 with `venv`, cron; installer adds Ray and PyYAML |
| GPU node | Execution dependencies, NVIDIA driver ≥ 570 and NVIDIA container toolkit |

Building from source also requires Rust and `protoc`. The control-host Docker
build supplies them inside its build image. Ray runs only on execution nodes.

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
`fleetctl` on the control host's PATH. Administrators use their token for namespace
quotas, access and node administration. Workload owners receive scoped owner
tokens (`read`, `submit` and `manage`) for app and batch metadata, priority,
enable/disable and scaling. Administrators approve batch mount roots and runtime
users, and service execution settings. Tokens without `manage` can use their granted
workloads but cannot change their metadata. See [workload ownership](reference.md#workload-ownership).

Use [fleet.example.yaml](../deploy/fleet.example.yaml) for the scheduler settings.
Proof apps and their placement policy are registered through the API, not
configured there. Fleet creates schema 16 for a fresh database and migrates a
schema 14 or 15 database at start; any other database requires a metadata reset
and new namespace/token setup.

The installer runs `fleetctl init token` and `fleetctl init tls` only when their
files are missing. Once Compose is up, it issues the first administrator token into
`etc/admin.token` with `fleet-scheduler -c fleet.yaml issue-admin --label install
--out FILE`, which writes the token's record into the database. `fleetctl` reads
that file by default. `init tls` creates a private CA and server certificate for the
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
When systemd supervises the agent, set `FLEET_NODE_SUPERVISED=1` in
`etc/watchdog.env`; the watchdog may still keep Ray running. Use only one agent
supervisor.
It preserves existing app and batch configuration. A new node starts empty:
configure `etc/services.yaml` and `etc/batches.yaml` using the
[app example](../deploy/services.example.yaml) and
[batch example](../deploy/batches.example.yaml).

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
Use the [upgrade steps](maintenance.md#update-fleet) for an existing installation.
