# GOAT proof builder app worker

The GOAT proof builder (`proof-builder-rpc`) runs as its proof app's prover entry on the
shim, in the GPU image's own container: the worker package lies in the app's storage root,
which is mounted in, and its `gpu-worker` script ([`apps/gpu-worker.sh`](../gpu-worker.sh))
is the container's entrypoint. It starts the GPU server, starts the worker once the server
answers ready, and ends the container when either process ends. The worker does three
things on one machine-pinned replica:

- It serves the Bitcoin node proof API (`/v1/proofs/*`, signed requests).
- It proves the header, commit and state chains continuously, and operator and watchtower
  proofs on demand.
- It proves directly against the GPU server in its container.

Its task database and proofs stay in the app's storage root. Fleet manages lifecycle,
resources and exclusive use of the GPU server; it does not interpret the worker's state. The
worker's own behaviour is documented in `proof-builder-rpc/README.md` ("Fleet app worker mode")
in the source tree.

Fleet pins the worker in the `third_party/bitvm-node` Git submodule. It tracks
`feat/fetch-state-chain` of the `blake-pro/bitvm-node` fork and is pinned at
`152ada4c49418fd7043c1ad95b1e7ea0e6ffe57e`. Once the branch lands in
`GOATNetwork/bitvm-node`:

1. Switch the submodule URL to `GOATNetwork/bitvm-node`.
2. Re-pin it to the merged commit.

Initialize it with `git submodule update --init third_party/bitvm-node`. Deployment and
packaging use the committed pin; never use `git submodule update --remote`. Host and guests
use Ziren `3cf393cdb2c817a16646481a5d9d761478d3e908`.

## App identity

Each package is a Fleet proof app: a namespace app with its own name, for example
`goat-proof-builder-v1`, whose `elf_id` is the package's bundle id. The bundle id covers all
five guest programs:

1. List each program as `<name> <ELF SHA-256>`, sorted by name: commit-chain, header-chain,
   operator, state-chain, watchtower.
2. Take the SHA-256 of those lines.

`proof-builder-rpc --print-programs` prints the ELF digests, program IDs, verifying-key
hashes, wrap mode and bundle id; the package stores this output as `programs.json`.

At startup the worker checks that its embedded programs, `programs.json`, `ZKM_IMM_WRAP_VK`
and Fleet's app id all agree. After each program's GPU setup it also checks the verifying
key.

`BITCOIN_NETWORK` is compiled into the host and the guests, so each network has its own
bundle id. Any guest change produces a new app identity. For a new identity:

- create a new app name, and use a new `apps.yaml` entry name and storage root;
- follow the cutover below;
- keep the previous package until the new one has been verified.

A worker-only change keeps the identity and storage root.

For an existing deployment, preserve the task database, proofs and header-chain
progress. Use the [maintenance guide](../../docs/maintenance.md) for backups and
cutover.

Fleet reserves the GPU server for exactly the app named by the entry's `app` and `elf_id`.
No SDK submission reaches that app, because the bundle id is not the digest of any ELF.
Fleet does not know which ELFs the bundle contains, so it still accepts SDK submissions to
separate per-program apps registered with a member ELF's `elf_id`. While the bundle worker is
enabled, operators must therefore ensure:

- every per-program GOAT SDK app is disabled;
- the previous proof builder is stopped;
- no other GOAT bundle is enabled.

Check with `fleetctl --namespace NAMESPACE --json app` before and after every change.

## Build

The package must contain the guests that are deployed on chain. Every build compares its
`programs.json` against a committed baseline and fails on any difference:

```bash
apps/goat/build.sh testnet4 apps/goat/baseline/testnet4.json /tmp/goat-package
```

Building `proof-builder-rpc` compiles the five guest circuits from the submodule source with
the Ziren MIPS toolchain (`~/.zkm-toolchain/env` is sourced when present). `--elf-dir` reads
validated `<program>.elf` files for the five programs instead, embeds them and skips guest
compilation. Prefer it on a machine whose source build may not reproduce the baseline's ELFs.
Only the shared `circuits` guest workspace with unique `<program>-guest` binary targets is
supported. Sync the pinned submodule before building. The script resolves its target directory
through Cargo metadata; by default the ELFs are under
`third_party/bitvm-node/circuits/target/elf-compilation/mipsel-zkm-zkvm-elf/release/`.
Packaging verifies the ELF paths reported by this build's `ZKM_ELF_<program>-guest` directives,
recorded in `build-messages.jsonl` and `guest-elf-paths.json` in the output directory.

When the guests change, create the baseline from the same build with `--new-baseline`. The
baseline must not exist yet:

```bash
apps/goat/build.sh testnet4 apps/goat/baseline/testnet4.json /tmp/goat-package --new-baseline
```

It writes `testnet4.json`, in the format of [`baseline.example.json`](baseline.example.json),
and `testnet4.operator-vk-hash`, the operator VK hash the contracts must hold, and packages the
build. The header, state, commit and watchtower program IDs authorized on chain
(`sequencer-set-publish --program-id-manifest`) must equal the baseline's `program_id` values.
Review both files and commit them under `baseline/`.

The script:

- requires committed submodule source and a clean tree;
- sets `BITCOIN_NETWORK`, and `ZKM_IMM_WRAP_VK=1` (the deployed wrap mode);
- builds `proof-builder-rpc` with `--locked`;
- checks each embedded ELF digest and the bundle id;
- writes `proof-builder-rpc`, `programs.json`, `fleet-app-id`, `gpu-worker`,
  `source-revision` and `SHA256SUMS`.

## Install and configure

1. Create the storage root and install the package in its `metadata/` directory:

   ```text
   /srv/zkm-fleet/apps/goat/            proof_storage_root (FLEET_APP_STORAGE_ROOT)
     metadata/                          the package: proof-builder-rpc, programs.json, gpu-worker
     proof-builder.lock                 exclusive worker lock
     run/                               the container's workdir: proof-builder.toml, .env, the
                                        sqlite database and *.ckpt files
     circuits/data/{header-chain,commit-chain,state-chain,operator,watchtower}/
   ```

   The working directory is `run/`. Relative `../circuits/data/...` paths in the database,
   the config and state-chain snapshots therefore resolve inside the root. The worker
   refuses to start when its working directory or database is outside the root. The
   account in the entry's `user` owns the root.
2. Pre-install the Groth16 circuit artifacts for that account. The Groth16 wrap and the
   local verification run on the worker's CPU. With `ZKM_IMM_WRAP_VK=1` the SDK reads them
   from `~/.zkm/circuits/groth16/imm-wrap-vk/v2.0.0`. Copy them from the current proof
   builder host, which already uses them, and do not rely on a download during proving.
   The entry mounts the account's `~/.zkm`; where its entries are symbolic links, mount the
   directory they point into as well.
3. Adapt [`apps.example.yaml`](apps.example.yaml) to the node's `apps.yaml`. Fill
   in the namespace, the app name, `elf_id`, the machine and the resources. `elf_id` is the
   content of the package's `fleet-app-id`; YAML does not expand `$(cat ...)`. Size `cpus` for the
   Groth16 wrap as well as the GPUs. Keep `--drain-timeout-secs` below `stop_timeout_s`,
   and set both from the longest proof measured in staging. The entry's `env` has two
   parts:

   - **The GPU server's**: the container's environment, as the entry lists it.
   - **The worker's**: the entrypoint gives the worker only `PATH`, `HOME`, every
     `FLEET_*` value, `FLEET_GPU_ENDPOINT` and `RAYON_NUM_THREADS` (from
     `WORKER_THREADS`).

   The worker reads its Fleet settings from that environment before it loads `.env`, so
   they belong in the entry, and `PROOF_BUILDER_PROGRAMS_FILE` on its command line. Among
   them is `FLEET_APP_TENANT`, which the pinned worker requires and only uses to label its
   log; without it the worker exits at startup with
   `FLEET_APP_TENANT is required when Fleet launches the proof builder`.
4. Put credentials and RPC endpoints in `.env` in `run/`, as
   [`worker.env.example`](worker.env.example) describes, readable only by the account in
   the entry's `user`. The worker reads it at startup. Do not set `ZKM_PROVER`, `ENDPOINT`,
   `FLEET_TOKEN_FILE` or `FLEET_EXIT_WITH_PARENT`.
5. The worker's API listens on port 8080 in the container, which also answers the health
   check (`GET /`); the shim publishes it on a loopback port of the node. The entrypoint's
   own health responder takes `HEALTH_PORT` 8081, and metrics are on `127.0.0.1:9200` in
   the container. Set `PROOF_SERVER_URL` on the Operator and Challenger nodes to where they
   reach the API.

The example follows the selected `ghcr.io/projectzkm/ziren-gpu:latest` image. Pull it on the
GPU node before restarting this app and record its resolved digest for recovery. The tag
can move; this configuration does not establish compatibility with the current `3cf393cd`
SDK pin. Verify Groth16 proofs against the deployed host and guests before resuming all
tasks. The entry's `image` field selects the GPU image; SDK Docker image environment
variables do not select it.

## Cutover

Stop the old path before starting the new one, so the GPUs and quota are free and no proof
runs twice.

1. Build the package and confirm it against the baseline. Install it and the Groth16
   artifacts. Create the bundle's app under a new name, with a spec file holding the GPU
   image (`{"image": "ghcr.io/projectzkm/ziren-gpu:latest"}`):

   ```bash
   fleetctl --namespace NAMESPACE app add goat-proof-builder-v1 --spec goat.json \
     --elf-id "$(cat /srv/zkm-fleet/apps/goat/metadata/fleet-app-id)"
   ```

   Use `--elf-id`, not `--elf`: the bundle id is not the digest of a single ELF. Then add the
   entry to `apps.yaml` disabled, with `app` set to that name and `elf_id` to the
   bundle id, and wait for the node heartbeat to advertise it. Enable the app only after
   that: enabling an app no node reports yet places a plain prover, without the worker,
   on a node the scheduler picks.

2. Stop every submission source: stop the old proof builder, and archive its database
   read-only.
3. Disable and drain each per-program SDK app, then confirm that its GPU container is gone
   and its resources are released. `fleetctl --namespace NAMESPACE --json app` shows each
   app's `proof.elf_id`; match them against `programs.json`:

   ```bash
   fleetctl --namespace NAMESPACE app disable SDK_APP_NAME
   ```

4. Copy the database, `circuits/data/**` (including state-chain `.args`/`.ctx` snapshots)
   and `proof-builder.toml` into the storage root. Every proven proof needs the sidecars its
   readers use: `.public_inputs.bin`, `.vk_hash.bin` and `.zkm_version.bin`, plus `.blocks`
   for header-chain and `.commits` for commit-chain proofs.
5. Enable and scale the app:

   ```bash
   fleetctl --namespace NAMESPACE app enable goat-proof-builder-v1
   ```

   Watch `<log_dir>/goat-proof-builder-v1-0.node.log` until the first proof has been committed.
6. Switch `PROOF_SERVER_URL`. Confirm that nodes receive operator and watchtower proofs.
7. To roll back: disable the bundle app and confirm its GPUs are released. Then
   restore the SDK apps and the old proof builder, in reverse order, from the cutover
   snapshot.

## Operate and recover

The database and the published proof files are the worker's whole state; restarts resume
from them.

- **Atomic publication.** A proof and its sidecars are published through synced temporary
  files. A `<proof>.manifest.json`, written last, marks the set complete. The database
  records a proof only after publication, so nodes never read partial files.
- **Interrupted proofs.** If the process stops after publishing but before the database
  commit, the proof is redone on restart and atomically replaces the files.
- **Shared GPU.** The GPU server runs one proof at a time in FIFO order. An operator or
  watchtower task waits at most for one proof of each other task.
- **Timeouts win.** An operator or watchtower task stays `New` while it queues and proves.
  Its result is written only while it is still `New`, so a timeout from the node wins.
- **Failures:**
  - A GPU server that stops reporting ready, or a proof that cannot be published, leaves
    the task pending. The worker exits, its container ends, and Fleet starts it again with
    a new GPU server.
  - A failed GPU call whose guest still executes on the CPU is a prover or transport fault.
    The proof is retried in place, and the failure does not count.
  - A guest that rejects its input, or a proof that does not verify, marks the task `Failed`
    after `--max-proof-attempts` consecutive failures (default 2).
- **Shutdown.** SIGTERM stops new proofs. The proof in progress may finish publishing and
  committing within `--drain-timeout-secs`; after that the process exits regardless. Exit
  codes: 0 after a clean stop, 1 after a task failure or an abandoned proof, 101 after a
  panic.
- **Disabling the app.** `fleetctl app disable` stops the container with its
  `stop_timeout_s` grace, so the worker shuts down as above; any unfinished proof is redone
  after the app is enabled again.

Inspect progress through the worker log and its metrics. Each proof attempt is counted with
an outcome: `success`, `failed`, `transient`, `unavailable` or `cancelled`.

For staging recovery tests, build with the `fault-injection` feature (outside `build.sh`). Setting
`PROOF_BUILDER_FAULT=after-publish` then exits right after a proof is published and before
its database commit.

For node capacity and installed Fleet changes, follow the
[maintenance procedure](../../docs/maintenance.md).
