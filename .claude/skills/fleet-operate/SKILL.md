---
name: fleet-operate
description: >-
  Operate a zkm-fleet cluster: services.yaml and apply (provers, GPU apps, the runs
  API), nodes (list, describe, drain), adding or removing a machine, tenants and
  credentials (signing keys, mTLS), logs, upgrading the scheduler and its database
  schema. Use for cluster and operations questions rather than jobs or apps.
---

# Operating zkm-fleet

Paths below are the installer's defaults (`/opt/zkm-fleet`, `/etc/zkm-fleet`); a
rootless install has them under `--prefix` / `--etc`. `fleetctl` prints a formatted
view; `--json` prints the data.

## Services (provers, GPU apps, the runs API)

`/etc/zkm-fleet/services.yaml` (example: `deploy/services.example.yaml`) lists the
long-running services; `/opt/zkm-fleet/deploy/apply.sh` makes the Ray actors match it
(start, stop, replace on a spec change such as a new image digest; a replaced prover is
drained first). `python -m fleet_bridge apply --dry-run --services …` only lists what an
apply would do; run it before an apply on a live cluster.

| kind | what it is |
|---|---|
| `ziren-gpu` / `ziren-cpu` | a prover node (`fleet-ziren-node`) with its GPU server container on `gpus` cards, registered with the scheduler |
| `app` | any GPU container serving on a port (`port_base` + replica index; ComfyUI, an inference server); `python -m fleet_bridge render --services … --actor <name>-0 --gpus <card>` prints its `docker run` to try by hand |
| `runs-api` | the one-shot runs API (`listen`, `profiles`, `runs_dir`), on the control machine |
| `hold` | `gpus: N`, no image: keeps the next free cards from the services listed after it while they start, so on one machine the file's order decides which cards each gets (Ray hands out the lowest free cards) |

A replica's cards, CPUs and memory come from one machine; `machine: <host name or IP>`
pins it. Actors are detached: Ray restarts them on failure; after a head restart the
watchdog re-applies the file.

## Nodes

```
fleetctl node                 # BACKEND, GPUS, MACHINE, READY (yes / no / drain), RESERVED, BUSY WITH, HEARTBEAT
fleetctl node NAME            # the record, and the apps anchored on it
fleetctl node drain NAME      # takes no new job, finishes the current one
fleetctl node undrain NAME
```

A proof app is *anchored* on the node that last ran it (`fleetctl app`); an exclusive
tenant's anchor reserves the node.

## Add a machine

Every machine runs one Ray node; the control machine's Ray head places work on
whichever machine has the cards. A machine joins with one command; nothing on the
control machine changes until something is placed on it.

1. **Prerequisites** on the new machine: for a machine with cards, Docker with
   `nvidia-container-toolkit` and the NVIDIA driver ≥ 570 (the GPU server image is
   pulled from ghcr.io on first use, no login); Python ≥ 3.10 with `venv`; `cron`. The
   binaries `fleet-ziren-node` and `fleetctl` built for its OS (one `ubuntu:22.04` build
   serves every machine of that OS), in `./target/release` or `--bin DIR`.
2. **Cluster token:** copy the control machine's `/etc/zkm-fleet/cluster.token` over
   (0600); it is what lets the machine's prover nodes talk to the scheduler.
3. **Install:**
   `deploy/install.sh node --head <control machine's cluster IP> --cluster-token cluster.token --cuda 0,1,2,3 [--cpus N] [--ip <this machine's cluster address>]`.
   `--cuda` is the cards Ray may hand out here (leave out any used by something else),
   `--cpus` how many CPUs (default all). Use the same `--prefix` / `--etc` as on the
   control machine: `services.yaml` names `node_binary` and `cluster_token_file` by
   path and every machine must find them there. The installer puts the binaries, the
   bridge and a venv with the pinned Ray in place, starts Ray as a worker of the head
   (`deploy/ray-start.sh worker`, fixed ports) and installs the cron watchdog that
   brings it back after a crash or a reboot.
4. **Network:** the machine must reach the head on 6379 and the scheduler's control
   port 51001 (bound to the control machine's cluster IP, the one given to
   `install.sh control --ip`); the other machines must reach it on 6380, 6381, 8080,
   52365 and 10002-19999 (Ray) and 31000-31099 (its prover nodes). GPU servers and apps
   bind 127.0.0.1 on their own machine.
5. **Shared storage for runs:** the runs dir (`install.sh --runs-dir`, default
   `<prefix>/work/runs`) must be the same shared path on every machine that runs them,
   and so must the roots a run app allows: a run's mounts are paths on the machine it
   lands on.

Check: on the control machine `/opt/zkm-fleet/venv/bin/ray status` lists the node with
its GPUs and CPUs; `fleetctl node NAME` shows a prover's `machine` once one runs there.

Place work on it: a prover or GPU app with `machine:` in `services.yaml` then
`apply.sh` (without `machine:` Ray picks any machine with the cards free); a run with
`fleetctl job submit --machine <host> …`, or `--nodes N` for one run over N machines.

**Remove a machine:** `fleetctl node drain NAME` for each prover on it, take its
services out of `services.yaml` (or their `machine:` pin) and `apply.sh`; then on the
machine drop the watchdog line from `crontab -e` and run `<prefix>/venv/bin/ray stop`.

## Tenants and credentials

- Tenants: `/etc/zkm-fleet/fleet.yaml` (class latency / batch, anchoring none / shared
  / exclusive, allowed backends, `nodes` name prefixes a tenant may run on, queue cap),
  then `docker compose -p zkm-fleet restart scheduler`. A tenant's programs are its
  proof apps (fleet-apps).
- A client: `fleetctl credential issue --tenant T --label L --endpoint https://<host>:51000 --tls-dir /etc/zkm-fleet/tls --out T.env`
  generates the signing key on the operator's machine (only its address is registered),
  issues a client certificate whose CN is that address (mutual TLS; the SDK only speaks
  TLS with a client certificate), and writes the client's env file (0600). The client
  runs `set -a; . ./T.env; set +a` and uses the SDK as before.
- `fleetctl credential` (the list), `credential ADDRESS` (one), `credential revoke ADDRESS`.
- With `client_cert_identity: true` (set by the installer with TLS) the scheduler answers
  `GetStatus` only for the tenant whose certificate submitted the proof; the public values
  URL carries a per-proof key.

## Logs

`docker compose -p zkm-fleet logs scheduler`; prover nodes `<workdir>/logs/<actor>.node.log`
(default `/opt/zkm-fleet/work/logs`); GPU servers `docker logs fleet-gpu-<actor>`; apps
`docker logs fleet-app-<actor>`; runs `fleetctl job logs ID`; the runs API actor's own log
through Ray (`ray logs actor --name runs-api-0`).

## Upgrading

Scheduler: `git pull`, rebuild, `docker compose -p zkm-fleet up -d --build scheduler`;
clients and running proofs ride through a restart (the gateway retries, the nodes hand
over results). On start the scheduler brings Postgres to its schema version in one
transaction per step (`fleet_schema` records them); an older scheduler refuses a newer
schema, so rolling back needs the database from before the upgrade. Provers: a new
image digest in `services.yaml`, then `apply.sh` (drain and replace, one at a time).
Bridge: copy `bridge/`, `apply --dry-run` (expect no planned change), swap, restart the
runs API actor; a restarted actor serves within seconds.

## Ports

See the README's Ports table: 51000 stage (external, mTLS), 9191 http (public values
and `/admin`; bind it to loopback and tunnel unless the network is trusted, the admin
token travels in clear otherwise), 9192 runs API (loopback), 51001 control, 31000-31099
prover nodes, 30100-30199 GPU servers (loopback), Ray's fixed ports.
