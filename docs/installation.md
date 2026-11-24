# Installation

`deploy/setup.sh` downloads a GitHub Release package, verifies its SHA-256
checksums and runs `deploy/install.sh` with the packaged binaries.

| Machine | Required software |
|---|---|
| Control host | Docker with Compose v2, curl, OpenSSL, Python 3 |
| Execution node | Docker, Python ≥ 3.10 with `venv`, cron, curl |
| GPU node | Execution node software, NVIDIA driver ≥ 570, NVIDIA container toolkit |

## Control host

```bash
curl -fsSL https://raw.githubusercontent.com/ProjectZKM/zkm-fleet/v0.2.6/deploy/setup.sh | bash -s -- \
  --version v0.2.6 control --public-url https://fleet.example:8905 \
  --tls-names stage,fleet.example --prefix "$HOME/app/fleet" --etc "$HOME/app/fleet/etc"
```

This pulls the release's scheduler image from GHCR and starts the scheduler, Postgres, the HTTPS
gateway and MCP with Docker Compose. Add `--no-mcp` to leave MCP out. The OpenShell
gateway and Trustee AS are optional Compose profiles, set up [below](#trustee-attestation-service).
Keep `stage` in `--tls-names`: the HTTPS gateway requires that name on the scheduler's
certificate. The installer creates the cluster token, a private TLS CA and the first
administrator token in `etc/admin.token`, installs `deploy/fleet-ctl.sh` under the
prefix, and puts `fleetctl` on the PATH. That `fleet-ctl.sh` is the version from the
last install; for upgrades, use the new release's copy ([maintenance](maintenance.md#upgrading)).
It links `~/.local/bin/fleetctl` and `~/.config/zkm-fleet/admin.token` to this
installation only when they are missing or dangling. Links to another
installation are kept, so a second prefix (a rehearsal or a test) never
redirects your existing `fleetctl`. The installer prints the command to switch.
Scheduler settings are in `etc/fleet.yaml`
(see [fleet.example.yaml](../deploy/fleet.example.yaml)). Re-running the installer keeps
the `compose.env` settings it doesn't own, such as `FLEET_OPENSHELL_*` and the
`openshell` and `trustee` profiles, and applies `deploy/docker-compose.override.yml`.

## Execution node

Copy the control host's `etc/cluster.token` to the node (mode 600), and its
`etc/tls/ca.pem` when using the private CA. Then run on the node:

```bash
curl -fsSL https://raw.githubusercontent.com/ProjectZKM/zkm-fleet/v0.2.6/deploy/setup.sh | bash -s -- \
  --version v0.2.6 node --name gpu-1 --scheduler https://fleet.example:8905 \
  --cluster-token "$HOME/cluster.token" --scheduler-ca "$HOME/ca.pem" \
  --prefix "$HOME/app/fleet" --etc "$HOME/app/fleet/etc" --cuda 0,1,2,3 --cpus 32
```

Omit `--cuda` for a CPU-only node and `--cpus` to let Ray detect capacity. The
node needs outbound HTTPS (HTTP/2) to port 8905. A cron watchdog keeps Ray and
the agent running. With `--no-cron` the installer adds no watchdog and starts neither:
supervise both yourself, as `deploy/fleet-watchdog.sh` does (it starts Ray with
`deploy/ray-start.sh` and the agent with `deploy/node-start.sh`). Re-running the
installer keeps the node's settings in `etc/watchdog.env` (name, CUDA devices, CPUs,
CA, work and jobs directories) unless you pass their flags again.
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

For the first supervisor conversion, comment out the node's watchdog line with
`crontab -e` (cron would restart the agent within a minute), then stop only the
existing control agent with SIGTERM to `fleet-node` and wait for it to exit; leave
Ray, shim and application containers running. The installer takes `node-agent.lock`
before replacing files and refuses a live agent. Restore the cron line afterwards:
with `FLEET_NODE_SUPERVISED=1` in `watchdog.env` it leaves the agent to systemd.
After conversion, use `fleet-ctl node upgrade` for upgrades. A failed service handover
restores `apps.yaml` and `watchdog.env` and disables the units it enabled, keeping
the previous watchdog ownership: restoring the cron line resumes it.
Existing systemd units and drop-ins are preserved. Review any operator-written
units separately; the installer never replaces their user, environment or command.

The installer adds `shim_binary`, `shim_state_dir`, and `shim_daemon: true` to
the existing `apps.yaml` only when shim has not been configured. The executable
is pinned under `libexec/trusted-shim/<sha256>/`. Reinstalling stages new artifacts
without switching the running shim or restarting its service. Keep the state path
short enough that `state/control.sock` is under Linux's 108-byte Unix socket limit.
Configure `shim_network` in that same file for service forwarding; runtime drivers
and network credentials retain their existing validation and authorization.
A release package also carries the pinned Chisel tunnel helper
(`trusted-router-tunnel`, built from the Router revision the bundled shim locks and
recorded in `VERSION`). The installer stages it under
`libexec/trusted-router-tunnel/<sha256>/` and prints that path, but never selects
it: without `shim_network.tunnel_binary` the existing transport is kept. Selecting
it is a [maintenance step](maintenance.md#execution-node).
The installer preserves existing YAML text and mode, validates the pinned binary's
hash and publishes it atomically. The Ray service runs its watchdog every 30 seconds,
including temp-link repair, while the node waits for shim readiness before connecting.

Legacy apps keep their executor. An existing stdio shim allocation also keeps its
transport and executable: the installer refuses to silently convert it to daemon
mode. Introduce daemon mode on a new node, or explicitly drain and clean up those
shim allocations first. Existing Ray processes are left running; moving Ray itself
under a different service supervisor is a separate maintenance operation.

### Trustee Attestation Service

The optional `trustee` Compose profile includes the unmodified, digest-pinned
`trustee-as` image. To enable it, add `trustee` to `COMPOSE_PROFILES` in
`$FLEET_PREFIX/compose.env` (`FLEET_PREFIX` is the control host's `--prefix`), for
example `COMPOSE_PROFILES=mcp,trustee`, so later full `up -d` runs and re-installs
keep it, and start it with the installed stack:

```bash
COMPOSE=(docker compose --env-file "$FLEET_PREFIX/compose.env" -p zkm-fleet
  -f "$FLEET_PREFIX/deploy/docker-compose.yml")
# Add -f "$FLEET_PREFIX/deploy/docker-compose.override.yml" when the site has one.
"${COMPOSE[@]}" up -d trustee-as
```

TDX DCAP ZK admission does not require AS; legacy SNP/sample profiles and explicitly configured prechecks still use it. It verifies TEE evidence; Attestation Agent remains inside the
node's confidential VM. The AS does not need TEE devices or a Docker socket.
Compose explicitly selects upstream `LocalFs` storage for policies and reference
values via the upstream service config `deploy/trustee/as.json`, using the
`trustee-data` volume; upstream defaults to memory storage.
Preserve this volume in backups; do not remove it during upgrades. Release packages
include the AS configuration; scheduler upgrades back it up and rollbacks restore
it. Scheduler-only upgrades do not restart AS. To introduce or update AS on an
existing control host, apply the installed Compose stack's `trustee-as` service
separately after reviewing site overrides.

AS listens at `trustee-as:50004` on the private Compose network with no host
port. Fleet's existing HTTPS ingress on port 8905 forwards only
`/attestation.AttestationService/AttestationEvaluate`. This route requires a client
certificate signed by Fleet's TLS CA with a URI SAN matching
`spiffe://fleet/trustee/node/<node-id>` (letters, digits, underscores and hyphens).
Other AS methods, including policy writes, are denied. Existing Fleet routes keep
their current application authentication; clients without certificates still work.
Forwarded identity headers cannot substitute for a validated certificate.

Provision a distinct, short-lived client certificate for each node. Generate its
private key and CSR on the node:

```bash
umask 077
openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -keyout node.key -out node.csr -subj /CN=gpu-1
```

Have the operator sign the CSR with client-auth usage and the approved node URI
SAN using the existing Fleet PKI. The installer leaves that CA's private key at
`$FLEET_ETC/tls/ca.key` on the control host (`FLEET_ETC` is its `--etc`), where the
scheduler and HTTPS gateway containers can read it, since they mount all of
`$FLEET_ETC`. Nothing reads it after the install: sign with it, then keep it offline.
For example, after reviewing
`node.csr`, the operator can sign it with OpenSSL (the node ID is an approved
enrollment identity):

```bash
printf '%s\n' 'extendedKeyUsage=clientAuth' \
  'subjectAltName=URI:spiffe://fleet/trustee/node/gpu-1' > node.ext
openssl x509 -req -in node.csr -CA "$FLEET_ETC/tls/ca.pem" \
  -CAkey "$FLEET_ETC/tls/ca.key" -CAcreateserial -days 1 \
  -extfile node.ext -out node.pem
```

Deliver only `node.pem` and the CA certificate to the node. In its existing
`apps.yaml`, set `shim_tee.trustee_endpoint: https://<fleet-host>:8905` and
`shim_tee.trustee_tls: {ca_file: NODE_ETC/tls/ca.pem, cert_file: NODE_ETC/tls/node.pem, key_file: NODE_ETC/tls/node.key}`,
with `NODE_ETC` written out as the node's absolute `--etc` directory.
The endpoint must match the gateway server certificate SAN. The node private key
must be owned by the shim UID with mode 0600; paths must be absolute and files
must not be symlinks. Restart shim after renewal to reload credentials; its runtime
containers survive that restart. This certificate authorizes AS appraisal only,
not Fleet scheduling, application access or launch admission. Fleet token
revocation does not revoke certificates; use short lifetimes and PKI rotation.
On an existing deployment, `fleet-ctl.sh scheduler upgrade` re-renders `gateway.yaml`
with this route and recreates the HTTPS gateway ([maintenance](maintenance.md#control-host)).

The opt-in `deploy/tests/test_trustee_ingress.py`, in a source checkout, exercises
the packaged Envoy configuration and stock AS with temporary certificates. Run it with
`FLEET_TEST_TRUSTEE_INGRESS=1` and `TRUSTEE_PROTO` pointing to the pinned
`trusted-shim/proto/attestation.proto`; it requires Docker, OpenSSL, protoc and
test-only grpcio/protobuf. If Docker cannot access `/tmp`, give pytest a
`--basetemp` directory on a Docker-visible filesystem. No prover is started.

The packaged configuration supports local sample-evidence acceptance.
Hardware rollout also requires operator-approved verifier/collateral and reference
value settings. The packaged service already runs
`grpc-as --socket 0.0.0.0:50004 --config-file /etc/trustee/as.json` with
`deploy/trustee/as.json` mounted read-only there, and upgrades replace that file: don't
edit it, but mount a site-owned `as.json` at `/etc/trustee/as.json` through
`deploy/docker-compose.override.yml`, keeping its storage paths under the persistent
volume. Supply persistent signing-key settings if AS tokens have external consumers.
A Trustee response is a precheck; shim still needs a verified remote RA receipt
before creation. No KBS, separate RVPS, local prover or TEE is launched here.

### OpenShell gateway

Fleet 0.2 ships the unmodified OpenShell gateway as the optional `openshell` Docker
Compose profile beside the scheduler; the control host install leaves it out.
OpenShell is the shared access layer for all runtime drivers. Without user rules it
adds no restrictions beyond driver limits; configured rules require native
enforcement and never fall back to that default. The gateway and scheduler use the
same UID and a private compute socket; only the node's shim/driver creates workloads.
The gateway has no Docker socket or Fleet administrator credentials. It can start
only once the scheduler serves that socket, which needs the `policy_gateway.openshell`
settings below. This does not change existing workload executors.

The release workflow publishes `ghcr.io/projectzkm/zkm-fleet-openshell` once for the
OpenShell revision pinned in `Cargo.lock`. `fleet-ctl.sh openshell up` records its
registry digest in `FLEET_OPENSHELL_IMAGE` in `compose.env`, unless that already names
a digest. The default revision tag is a build reference; its presence in Compose does
not mean that a release has already been published. To package a locally built,
unchanged upstream `openshell-gateway`, use `deploy/openshell/Dockerfile` from a source
checkout with a minimal build context containing only that Dockerfile and binary.
Ubuntu 24.04 builds require the matching runtime base supplied by that Dockerfile.
Set `FLEET_OPENSHELL_IMAGE` to that local tag. `openshell up` uses a local image that is
in no registry as is, and warns that it is not pinned. Push it to a registry to pin it by digest.

After the control host install, create `$FLEET_ETC/openshell/` (`mkdir -m 700`) and
prepare these files in it, none empty and all owned by the `FLEET_UID:FLEET_GID` in
`compose.env`: `server.pem`, `server.key`, `ca.pem`, the Ed25519 launch-signing bundle
(`jwt.key`, `jwt.pub`, `jwt.kid`), and the upstream `gateway.toml` below. Private keys
and the directory must be mode 0600 and 0700, respectively. Issue a distinct Fleet
client certificate with `OU=operator` and client-auth usage; put its certificate and
key outside the gateway's mount, directly under `$FLEET_ETC`. The server certificate
needs both `openshell-gateway` and the public DNS/IP name in its SANs. Fleet's CA key
is `$FLEET_ETC/tls/ca.key` ([above](#trustee-attestation-service)); keep it offline
once these are signed. Do not regenerate launch-signing keys or CA material during an
upgrade.

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
For native GPUs or durable `mounts`, run the node driver with an admission policy
that sets `allow_driver_config: true` and use the same acknowledgement here; see
[GPUs and durable storage](reference.md#proofs).
Do not enable this block until the approved guest and remote proof worker are ready.

Then start the gateway, on a new or an upgraded control host:

```bash
"$FLEET_PREFIX/deploy/fleet-ctl.sh" -n openshell up   # print the steps, change nothing
"$FLEET_PREFIX/deploy/fleet-ctl.sh" openshell up
```

A host installed before v0.2.4 has no copy of `fleet-ctl.sh` there; run the release's
`deploy/fleet-ctl.sh` with `FLEET_PREFIX` set instead ([maintenance](maintenance.md#upgrading)).

`openshell up` checks the files above and the `policy_gateway.openshell` block, pins the image
digest, initializes the `openshell-run` and `openshell-data` volumes for
`FLEET_UID:FLEET_GID`, recreates the scheduler and waits for its health check, starts
`openshell-gateway`, and adds `openshell` to `COMPOSE_PROFILES` in `compose.env` so
later full `up -d` runs include it. Scheduler upgrades and rollbacks leave the gateway
alone. To change its image, set `FLEET_OPENSHELL_IMAGE` in `compose.env` to the new
digest and run `openshell up` again; its database and keys are kept.
Do not use `docker compose down -v`.

The gateway publishes host loopback port 17670 with mTLS enabled; `FLEET_OPENSHELL_BIND`
and `FLEET_OPENSHELL_PORT` in `compose.env` set the address and port. For remote
supervisors, set `FLEET_OPENSHELL_BIND` to the control host's reachable interface
and configure their `grpc_endpoint` to that certificate-covered HTTPS address,
or use an existing TCP/TLS passthrough ingress. HTTP/2 must be preserved. Health
port 8081 is container-only; it is not published. Nodes need outbound connections
only. Application streams continue through the independent data router.

Back up `openshell-data` (SQLite plus its WAL, with the gateway stopped or via a
consistent SQLite backup), launch-signing keys/PKI, and Fleet's policy ledger
together during maintenance. Keep their current identities on recovery; never
restore only one side of a delivered allocation. `openshell-run` holds the socket
and its lock, not durable application data. After recovery, `openshell up` starts the
scheduler before the gateway. Rail remains a library/proof guest and needs no
separate service.

## Options

Fetch `setup.sh` from the release you are installing: the script and the package it
downloads move together. Omit `--version` to install the latest release.
`--keep-package` keeps the downloaded package for inspection. To install from a
source checkout instead, build the binaries and run `deploy/install.sh control|node
--bin target/release` with the same options. A Fleet build has no `trusted-shim-runtime`,
so for `--with-shim` copy it (and `trusted-router-tunnel`) from the release package's
`bin/` into that directory.

See [operations](operations.md) for workloads, and [maintenance](maintenance.md)
for upgrades, day-2 work and the one-time migrations between older releases.
