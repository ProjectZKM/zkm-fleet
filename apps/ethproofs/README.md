# EthProofs app worker

Fleet pins the worker in the `third_party/reth-processor` Git submodule
([pinned source](https://github.com/ProjectZKM/reth-processor/tree/85a0c17ea6a60f0cdad0920a653ae8ca9b4ad8f9)).
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

The integrated worker selects Ethereum mainnet blocks using HTTP head polling,
prepares witnesses, and proves directly against its loopback Ziren GPU server.
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
Fleet rejects SDK submissions to an enabled
worker-owned stream, reserves the whole worker allocation, and refuses a second
owner until the previous owner has been cleaned up. A filesystem lock also
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
the host and guest, then pin its resolved digest in the service config. The current
validated image is `sha256:d4b8ef78d59935d024c034f11f28c26718a1890181f42be2954f31b74e252655`.
Its Ziren revision label matches the locked host and guest revision.

Install the package at `/opt/zkm-fleet/apps/ethproofs`. Adapt
[`services.example.yaml`](services.example.yaml) to the existing node config,
using the node's Ray hostname/IP, namespace, actual ELF digest, validated GPU
image, and durable storage path. The example starts disabled. Put API credentials
and RPC URLs in the private file described by
[`worker.env.example`](worker.env.example), readable only by the worker account.
Fleet supplies the scoped identity, state root, and loopback GPU endpoint. The
worker reads its app name from `FLEET_APP`. Its checkpoint identity names the app;
a version-1 checkpoint naming a tenant is upgraded in place when the rest of its
identity matches.

A new guest has a new app identity. Register it under a new NAME with
`fleetctl --token-file zkm-owner.token --namespace zkm app add NAME --elf /path/to/program.elf --class latency --anchor exclusive --backend cuda`,
then stage its service disabled with `app: NAME` and the guest's `elf_id`, and
wait for the node heartbeat to advertise it.
Use a new service name and state directory. Keep a temporary rollback package
until proving and reporting are verified, then remove retired deployment records
according to the app's retention policy. Keep GOAT's service and image unchanged.

Before enabling a new worker, validate local proving and API reporting with test
credentials. For an existing stream, preserve its next unproved block and stop
the previous worker before starting the replacement. Bootstrap a new state
directory with `--start-block N`; a checkpoint takes precedence on subsequent
restarts. Run only one worker for the stream and preserve GOAT's service.

## Production deployment

The worker runs on `ant-5090-2` as one replica of app `ethproofs-z4`. Inspect its current GPU/CPU
allocation with `fleetctl app scale`; resource settings can change independently
of the worker package.
The node pool contains eight GPUs and 128 CPUs. Preparation concurrency remains one;
GPU allocation and preparation concurrency are separate settings.
This app no longer uses the external EthProofs client (reth-processor's `eth-proofs`);
it calls its local GPU server directly. reth-processor itself still proves through
the scheduler's SDK stage API, which is kept but deprecated; see
[Scheduler stage API (deprecated)](../../docs/reference.md#scheduler-stage-api-deprecated).
GOAT keeps its existing endpoint and configuration. App enable/disable state is
an app-owner decision; inspect it with `fleetctl --namespace zkm app` using an
owner token for namespace `zkm`.

The package records worker source commit
`85a0c17ea6a60f0cdad0920a653ae8ca9b4ad8f9` on
`chore/ziren-v2-stateless`, with PR #23 based on `stateless`. The guest was built
from `fd9589cd63c15c3671c643c1a34de7341a2201ff`; its SHA-256 is
`334b45b88d6c60e3354e983a5b26ef39b18feee65da828f277a93e646c78c73b`.
Worker updates preserve this guest and app identity. The image digest is listed
above; package checksums and `source-revision` record the worker separately.

For example, request five GPUs while retaining the current CPU and replica settings:

```bash
fleetctl --token-file zkm-owner.token --namespace zkm app scale ethproofs-z4 --gpus 5
fleetctl --token-file zkm-owner.token --namespace zkm app scale ethproofs-z4
```

Inspect `~/app/fleet/work/apps/ethproofs/v5-worker/checkpoint.json` for the saved
next block. Verify proofs and reporting acknowledgments in
`~/app/fleet/work/logs/ethproofs-v5-gpu-0.node.log`. Do not edit the checkpoint
to bypass a failed block.

## Operate and recover

The storage root contains `checkpoint.json`, checksummed prepared `inputs/`,
`outputs/BLOCK/{proof.bin,public_values.bin,manifest.json}`, pending `outbox/`,
and acknowledged `reported/`. Output and report records are synced before the
cursor advances. Restart recovers committed outputs. Sequential mode also reuses prepared inputs;
the optional latest-head mode can skip an old unproved input. Production runs
sequential mode. A failed
proof leaves its input and cursor intact for the actor's restart. Signaling the
worker with SIGTERM drains its current proof while its GPU server stays alive.
Fleet app disable kills the actor first; an unfinished block is retried after
resume. To finish the current proof before disabling, pause the node agent, ask
the Ray actor to shut down gracefully, remove it, then queue an app-disable
request and restart the agent so it can acknowledge that request. Keep the GPU server alive until the worker exits; signaling the worker
alone lets Ray restart its actor. Do not stop Ray or the GPU container first. Forced stops recover completed
outputs from durable state. Sequential mode retries the unfinished block.

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
