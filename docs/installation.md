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
curl -fsSL https://raw.githubusercontent.com/ProjectZKM/zkm-fleet/v0.2.0/deploy/setup.sh | bash -s -- \
  --version v0.2.0 control --public-url https://fleet.example:8905 \
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
curl -fsSL https://raw.githubusercontent.com/ProjectZKM/zkm-fleet/v0.2.0/deploy/setup.sh | bash -s -- \
  --version v0.2.0 node --name gpu-1 --scheduler https://fleet.example:8905 \
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

For the first supervisor conversion, stop only the existing control agent before
running the installer; leave Ray, shim and application containers running. The
installer takes `node-agent.lock` before replacing files and refuses a live agent.
After conversion, use `fleet-ctl node upgrade` for upgrades. Failed service handover
restores `apps.yaml` and `watchdog.env`, keeping the previous watchdog ownership.
Existing systemd units and drop-ins are preserved. Review any operator-written
units separately; the installer never replaces their user, environment or command.

The installer adds `shim_binary`, `shim_state_dir`, and `shim_daemon: true` to
the existing `apps.yaml` only when shim has not been configured. The executable
is pinned under `libexec/trusted-shim/<sha256>/`. Reinstalling stages new artifacts
without switching the running shim or restarting its service. Keep the state path
short enough that `state/control.sock` is under Linux's 108-byte Unix socket limit.
Configure `shim_network` in that same file for service forwarding; runtime drivers
and network credentials retain their existing validation and authorization.
The installer preserves existing YAML text and mode, validates the pinned binary's
hash and publishes it atomically. The Ray service runs its watchdog every 30 seconds,
including temp-link repair, while the node waits for shim readiness before connecting.

Legacy apps keep their executor. An existing stdio shim allocation also keeps its
transport and executable: the installer refuses to silently convert it to daemon
mode. Introduce daemon mode on a new node, or explicitly drain and clean up those
shim allocations first. Existing Ray processes are left running; moving Ray itself
under a different service supervisor is a separate maintenance operation.

### OpenShell gateway

Fleet 0.2.0 includes an optional `openshell` profile in its existing Docker Compose
file. This runs the unmodified upstream gateway beside the scheduler. Both use the
same UID and a private shared compute socket; only the node's shim/driver creates
workloads. The gateway has no Docker socket or Fleet administrator credentials.
The profile is off by default, so existing applications keep their executors.
It is mandatory for a deployment using `driver: openshell`: an unavailable gateway
blocks native creation/publication, with no automatic fallback to Docker.

The release workflow publishes `ghcr.io/projectzkm/zkm-fleet-openshell` at the
OpenShell revision pinned in `Cargo.lock`. Record its registry digest in
`FLEET_OPENSHELL_IMAGE` in the existing `compose.env` before production use. The
default revision tag is a build reference; its presence in Compose does not mean
that a release has already been published. To package a locally built, unchanged
upstream `openshell-gateway`, use `deploy/Dockerfile.openshell` with a minimal build
context containing only that Dockerfile and binary. Ubuntu 24.04 builds require the
matching runtime base supplied by that Dockerfile.

Prepare these files under the existing `$FLEET_ETC/openshell/` directory, owned by
the configured `FLEET_UID:FLEET_GID`: `server.pem`, `server.key`, `ca.pem`, the
Ed25519 launch-signing bundle (`jwt.key`, `jwt.pub`, `jwt.kid`), and the upstream
`gateway.toml` below. Private keys and the directory must be mode 0600 and 0700,
respectively. Issue a distinct Fleet client certificate with `OU=operator` and
client-auth usage; put its certificate and key outside the gateway's mount, directly
under `$FLEET_ETC`. The server certificate needs both `openshell-gateway` and the
public DNS/IP name in its SANs. Keep the CA private key offline. Do not regenerate
launch-signing keys or CA material during an upgrade.

```toml
[openshell]
version = 2
[openshell.gateway.gateway_jwt]
signing_key_path = "/etc/openshell/jwt.key"
public_key_path = "/etc/openshell/jwt.pub"
kid_path = "/etc/openshell/jwt.kid"
gateway_id = "fleet"
```

Use the reviewed proof guest and remote proof endpoint in the existing `fleet.yaml`
`policy_gateway` block; add its `openshell` settings as described in the
[runtime reference](reference.md#proofs). Set `compute_socket` to
`/run/zkm-fleet/openshell/compute.sock` and `gateway.endpoint` to
`https://openshell-gateway:17670`. Its CA and operator certificate/key paths must
refer to files under `/etc/zkm-fleet`. Admission policy must match the node driver.
Do not enable this block until the approved guest and remote proof worker are ready.

Initialize the two new named volumes **once**, before enabling the scheduler's
compute socket. Set `FLEET_UID` and `FLEET_GID` to the exact values in `compose.env`:

```bash
COMPOSE=(docker compose --env-file "$FLEET_PREFIX/compose.env" -p zkm-fleet
  -f "$FLEET_PREFIX/deploy/docker-compose.yml")
# Include the site's docker-compose.override.yml as a second -f when present.
"${COMPOSE[@]}" --profile openshell run --rm --no-deps --user 0:0 \
  --cap-add CHOWN --cap-add FOWNER --cap-add DAC_OVERRIDE \
  --entrypoint install openshell-gateway -d -m 700 \
  -o "$FLEET_UID" -g "$FLEET_GID" /run/zkm-fleet/openshell /var/lib/openshell
"${COMPOSE[@]}" up -d --no-deps scheduler
"${COMPOSE[@]}" --profile openshell up -d --no-deps --wait openshell-gateway
```

Add `openshell` to `COMPOSE_PROFILES` in `compose.env` (for example
`mcp,openshell`) to include it in subsequent whole-stack operations. Regular
`fleet-ctl scheduler upgrade` still updates only scheduler/MCP; gateway image
upgrades and rollback are explicit, with its database and keys preserved.
Do not use `docker compose down -v`.

The gateway defaults to host loopback port 17670 with mTLS enabled. For remote
supervisors, set `FLEET_OPENSHELL_BIND` to the control host's reachable interface
and configure their `grpc_endpoint` to that certificate-covered HTTPS address,
or use an existing TCP/TLS passthrough ingress. HTTP/2 must be preserved. Health
port 8081 is container-only; it is not published. Nodes need outbound connections
only. Application streams continue through the independent data router.

Back up `openshell-data` (SQLite plus its WAL, with the gateway stopped or via a
consistent SQLite backup), launch-signing keys/PKI, and Fleet's policy ledger
together during maintenance. Keep their current identities on recovery; never
restore only one side of a delivered allocation. `openshell-run` holds the socket
and its lock, not durable application data. Start the scheduler before the gateway
after recovery. Rail remains a library/proof guest and needs no separate service.

## Options

Fetch `setup.sh` from the release you are installing: the script and the package it
downloads move together. Omit `--version` to install the latest release.
`--keep-package` keeps the downloaded package for inspection. To install from a
source checkout instead, build the binaries and run `deploy/install.sh control|node
--bin target/release` with the same options.

See [operations](operations.md) for workloads, and [maintenance](maintenance.md)
for upgrades, day-2 work and the one-time migrations between older releases.
