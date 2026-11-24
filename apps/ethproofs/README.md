# EthProofs app worker

The EthProofs worker runs as a container app on the shim. One image holds the worker
and the Ziren GPU server it proves against; the image's entrypoint
([`deploy/images/gpu-worker`](../../deploy/images/gpu-worker)) starts the server,
starts the worker once the server answers ready, and ends the container when either
process ends.

Fleet pins the worker in the `third_party/reth-processor` Git submodule
([pinned source](https://github.com/ProjectZKM/reth-processor/tree/a75cb337305eac262a31346b4b3086146fed8297)).
The worker migration is based on
[reth-processor PR #23](https://github.com/ProjectZKM/reth-processor/pull/23),
`chore/ziren-v2-stateless`, which includes the Fleet worker integration and
support for structured or RLP-encoded execution-witness headers. Host and guest
trie updates preserve untouched storage roots and remove empty accounts. The guest
accepts omitted storage only when the authenticated account is absent or its
storage root is empty; missing non-empty storage remains an error.
If the witness omits contract bytecode, the host fetches it at the parent block,
checks its hash against the authenticated account, and includes the verified code
in the guest input. A missing or mismatched response fails preparation and keeps
the cursor on that block.
The host RPC provider advertises gzip support and decodes compressed responses
through reqwest. Witness providers can return compressed or plain JSON; the
decoded witness, guest ELF and proof semantics remain the same.
Host and guest Ziren are locked to
`0dd65a15db3d7bad5cf582c5dbc7492440ea2830`. Initialize it with
`git submodule update --init --recursive`.
GOAT retains its existing SDK and gateway architecture.

The worker selects Ethereum mainnet blocks using HTTP head polling,
prepares witnesses, and proves directly against the GPU server in its container.
Proofs, public values and the block cursor stay in the app's persistent directory.
Fleet manages lifecycle and resource allocation without interpreting the checkpoint.
EthProofs API reporting runs from a durable outbox with retry and backpressure.

The deployment uses one machine-pinned replica and strict sequential proving.
`--block-interval 1 --prepare-concurrency 1 --prefetch-depth 1` prepares each
successive block and lets preparation overlap the current GPU proof through a
bounded queue. GPU proving remains one block at a time. The worker resumes its
saved cursor, retries preparation failures for that same block, and advances only
after the proof and reporting outbox are committed. It does not skip lagging
blocks, replace prepared witnesses with newer ones, or jump forward after a
restart. The deployment does not enable `--latest-block` or a nonzero `--max-lag`.
Completed outputs and pending reports survive restarts. Earlier skips made while
latest-head mode was enabled remain historical gaps; this policy applies from the
saved cursor onward.
The app holds its GPUs and CPUs for as long as it is enabled; Fleet sets no hard CPU
or memory limit on the container unless the app has one. A filesystem lock
prevents two processes from sharing one state directory. Cross-machine failover
requires explicit cleanup and transfer of this directory.

## Build and configuration

Use a guest ELF built from this revision’s locked Ziren 2.0 graph and validated
with its matching GPU image:

```bash
apps/ethproofs/build.sh /absolute/path/to/validated/program.elf /tmp/ethproofs-package
```

The script requires committed submodule source and installed upstream Rust
build dependencies. It packages the executable, existing ELF, source revision
and SHA-256 hashes; it does not build a new guest. `ETH_PROOFS_BUILD_ELF` supplies
the existing ELF to the upstream host build. A new guest requires the Ziren MIPS
toolchain and the locked guest dependencies, including PR #23's precompile pins.

For an image update, pull `ghcr.io/projectzkm/ziren-gpu:latest`, validate it with
the host and guest, then pin its resolved digest in the app's `apps.yaml` entry. The current
validated image is `sha256:d4b8ef78d59935d024c034f11f28c26718a1890181f42be2954f31b74e252655`.
Its Ziren revision label matches the locked host and guest revision.

Build the image on the GPU node from the package and the validated GPU image. The
package keeps its installed path inside the image, so the worker's command and
`ETH_PROOFS_ELF_FILE` name the same paths:

```bash
docker build -t ethproofs-worker:TAG -f deploy/images/gpu-worker/Dockerfile \
  --build-arg GPU_IMAGE=ghcr.io/projectzkm/ziren-gpu@sha256:DIGEST \
  --build-arg PACKAGE_PATH=/opt/zkm-fleet/apps/ethproofs \
  --build-context package=/opt/zkm-fleet/apps/ethproofs deploy/images/gpu-worker
```

Adapt [`apps.example.yaml`](apps.example.yaml) to the node's `apps.yaml`, using the
node's machine, the namespace, the guest's ELF digest, the image and the state
directory. The example starts disabled and sets `machine`, which keeps the app on the
node that holds its state. Its `env` has two parts:

- **The GPU server's**: the container's environment, as the entry lists it.
- **The worker's**: the entrypoint gives the worker `PATH`, `HOME`, the lines of the
  private file `WORKER_ENV_FILE`, every `FLEET_*` value, `FLEET_GPU_ENDPOINT` and
  `RAYON_NUM_THREADS` (from `WORKER_THREADS`), and nothing else of the container's
  environment. The file cannot replace a `FLEET_*` value.

Put API credentials and RPC URLs in the private file described by
[`worker.env.example`](worker.env.example), readable only by the account in the
entry's `user`, and mount it read-only. Each line is `KEY=VALUE` with one plain word
or one quoted string; nothing is expanded, and any other line ends the container
naming its line number. The worker's log level is `RUST_LOG` in that file.

The worker reads its app name from `FLEET_APP`. Its checkpoint identity names the
app; a version-1 checkpoint naming a tenant is upgraded in place when the rest of its
identity matches. To continue an existing stream under a new app name, set `FLEET_APP`
in the entry to the name the checkpoint holds.

A new guest has a new identity: use a new app name and state directory. Keep the
previous image and package until proving and reporting are verified. Keep GOAT's
entry and image unchanged.

Before enabling a new worker, validate proving on a dedicated Fleet staging
worker and API reporting with test credentials. For an existing stream, preserve its next unproved block and stop
the previous worker before starting the replacement. Bootstrap a new state
directory with `--start-block N`; a checkpoint takes precedence on subsequent
restarts. Run only one worker for the stream and preserve GOAT's entry.

## Production deployment

The worker runs on one GPU node as one replica of app `ethproofs-z4`. Inspect its current GPU/CPU
allocation with `fleetctl app scale`; resource settings can change independently
of the worker package.
The node pool contains eight GPUs and 128 CPUs. Preparation concurrency remains one;
GPU allocation and preparation concurrency are separate settings.
This app does not use the external EthProofs client (reth-processor's `eth-proofs`);
it calls its local GPU server directly. An SDK client proves through an
[app-owned proof endpoint](../../docs/reference.md#app-owned-proof-endpoint).
GOAT keeps its existing endpoint and configuration. App enable/disable state is
an app-owner decision; inspect it with `fleetctl --namespace zkm app` using an
owner token for namespace `zkm`.

Validate each new package and record its guest, image digest and `source-revision`.

For example, request five GPUs while retaining the current CPU and replica settings:

```bash
fleetctl --token-file zkm-owner.token --namespace zkm app scale ethproofs-z4 --gpus 5
fleetctl --token-file zkm-owner.token --namespace zkm app scale ethproofs-z4
```

Inspect `checkpoint.json` in the state directory for the saved next block. Verify
proofs and reporting acknowledgments with `fleetctl app logs NAME`, which shows the
worker's and the GPU server's output. Do not edit the checkpoint
to bypass a failed block.

## Operate and recover

The storage root contains `checkpoint.json`, checksummed prepared `inputs/`,
`outputs/BLOCK/{proof.bin,public_values.bin,manifest.json}`, pending `outbox/`,
and acknowledged `reported/`. Output and report records are synced before the
cursor advances. Restart recovers committed outputs. Sequential mode also reuses prepared inputs;
the optional latest-head mode can skip an old unproved input. Production runs
sequential mode. A failed
proof leaves its input and cursor intact for the container's restart. Disabling the
app stops its container: the worker gets SIGTERM and drains its current proof while
the GPU server stays alive, and the server stops once the worker has exited. A worker
still running after the entry's `stop_timeout_s` is killed; set it from the longest
proof. Forced stops recover completed outputs from durable state. Sequential mode
retries the unfinished block.

Reporting acknowledges HTTP success, or a duplicate HTTP 409 only after an
authenticated lookup matches the cluster, block, and exact downloaded proof
bytes. Delivery is at least once: an API success followed by a crash before the
local acknowledgement may resend the same block. Reporting
backlog pauses proving at the configured limit (128 by default). Historical
outputs and reports need an explicit retention policy.

The worker checks a prepared block's canonical hash immediately before proving.
A mismatch discards queued inputs and restarts from the durable cursor. A reorg
of an already committed block requires operator reconciliation; this first phase
does not rewind previously reported proofs automatically.

Source entry points are `bin/eth-proofs/src/main.rs` (pipeline), `latest.rs`
(bounded preparation, including sequential mode), `state.rs`
(recovery), `eth_proofs.rs` (reporting), and
`crates/executor/host/src/full_executor.rs` (preparation/proving) in the submodule.
Publish source changes first, then commit the exact new gitlink in Fleet.
Deployment and CI must use the committed pin, never `git submodule update --remote`.

For allocation changes, use `fleetctl app scale` with the app owner's token.
Namespace and tag quotas remain administrator-managed. For node capacity or installed
Fleet changes, follow the [maintenance procedure](../../docs/maintenance.md).
After any restart, check that the first proof equals the saved next block, outputs
are checksummed, reports are acknowledged, and the outbox drains. A witness or
proving failure keeps the cursor on that block: inspect ordinary worker/container
logs and fix the cause instead of enabling latest-head mode or editing the cursor.
