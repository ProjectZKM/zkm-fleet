# EthProofs app worker

Fleet pins the worker in the [`third_party/reth-processor`](../../third_party/reth-processor)
Git submodule. The worker migration is based on
[reth-processor PR #23](https://github.com/ProjectZKM/reth-processor/pull/23),
`chore/ziren-v2-stateless`, which includes the Fleet worker integration and
support for structured or RLP-encoded execution-witness headers. Host and guest
trie updates preserve untouched storage roots and remove empty accounts. The guest
accepts omitted storage only when the authenticated account is absent or its
storage root is empty; missing non-empty storage remains an error.
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
build dependencies. The former production ELF and GPU image are a different
revision; validate the new guest and pull `ghcr.io/projectzkm/ziren-gpu:latest`,
then pin its resolved digest for the EthProofs service. It packages the executable,
ELF, source revision, and SHA-256 hashes. `ETH_PROOFS_BUILD_ELF` bypasses the upstream guest build;
building a new guest requires the installed Ziren MIPS toolchain and its
locked guest dependencies. Guest builds retain PR #23's precompile pins. The example pins the image resolved
from `latest` on 2026-10-11: `sha256:d4b8ef78d59935d024c034f11f28c26718a1890181f42be2954f31b74e252655`.
Its Ziren revision label matches the locked host and guest revision.

Install the package at `/opt/zkm-fleet/apps/ethproofs`. Adapt
[`services.example.yaml`](services.example.yaml) to the existing node config,
using the node's Ray hostname/IP, namespace, actual ELF digest, validated GPU
image, and durable storage path. The example starts disabled. Put API credentials
and RPC URLs in the private file described by
[`worker.env.example`](worker.env.example), readable only by the worker account.
Fleet supplies the scoped identity, state root, and loopback GPU endpoint.

A new guest has a new app identity. Stage its new service disabled alongside the
legacy service and wait for the node heartbeat to advertise it. Then register `ethproofs/<new ELF digest>` with
`fleetctl app add ethproofs/ --elf /path/to/program.elf --namespace zkm`, use a
new service name and state directory, and retain the disabled old app and its
history for rollback. Keep GOAT's service and image unchanged.

Before live cutover, validate local proving against the deployed GPU image and
EthProofs reporting with the appropriate test credentials. Drain the external
client, record its next unproved block, and stop the old app endpoint/prover
before enabling the replacement service. Bootstrap with `--start-block N`;
a checkpoint takes precedence on subsequent restarts. Preserve GOAT's service.
Do not run the external client and integrated worker for the same stream together.

## Production deployment

The worker runs on `ant-5090-2` as one replica with six GPUs and 64 CPUs. The node
pool contains eight GPUs and 64 CPUs. Preparation concurrency remains one;
GPU allocation and preparation concurrency are separate settings.
The old EthProofs endpoint is disabled. GOAT keeps its existing endpoint and
configuration.

The package records worker source commit
`26ebf2a4adb5412a765f0dd1ed2cf6ba9bbd5502` on
`chore/ziren-v2-stateless`, with PR #23 based on `stateless`. The guest was built
from `fd9589cd63c15c3671c643c1a34de7341a2201ff`; its SHA-256 is
`334b45b88d6c60e3354e983a5b26ef39b18feee65da828f277a93e646c78c73b`.
Worker updates preserve this guest and app identity. The image digest is listed
above; package checksums and `source-revision` record the worker separately.

```bash
fleetctl --namespace zkm app scale ethproofs/334b45b8 \
  --gpus 6 --cpus 64 --replicas 1 --node ant-5090-2
fleetctl --namespace zkm app scale ethproofs/334b45b8
```

The 64-CPU allocation resumed from saved block 26,167,298 after block 26,167,297
finished during graceful shutdown. Verify new proofs and reporting acknowledgments
in `~/app/fleet/work/logs/ethproofs-v5-gpu-0.node.log`.

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
(bounded preparation and newest-ready selection), `state.rs`
(recovery), `eth_proofs.rs` (reporting), and
`crates/executor/host/src/full_executor.rs` (preparation/proving) in the submodule.
Publish source changes first, then commit the exact new gitlink in Fleet.
Deployment and CI must use the committed pin, never `git submodule update --remote`.

For allocation changes, use the administrator CLI. For node capacity or installed
Fleet changes, follow the [maintenance procedure](../../docs/maintenance.md).
After any restart, check that the first proof equals the saved next block, outputs
are checksummed, reports are acknowledged, and the outbox drains. A witness or
proving failure keeps the cursor on that block: inspect ordinary worker/container
logs and fix the cause instead of enabling latest-head mode or editing the cursor.
