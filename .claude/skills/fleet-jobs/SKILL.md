---
name: fleet-jobs
description: >-
  Submit, watch, fetch, cancel and purge one-shot runs (training, batch jobs) on a
  zkm-fleet cluster with `fleetctl job`, and read proof jobs. Use when asked to run a
  script or a training job "on the fleet", to check or follow a job, or to clean up
  after one.
---

# Jobs on zkm-fleet

A job is one instance of an app (see the fleet-apps skill). Two kinds:

| kind | submitted by | what it is |
|---|---|---|
| proof job | a tenant's client through the Ziren SDK (`ZKM_PROVER=network`) | one proof on a prover node; `fleetctl` only reads them |
| run | `fleetctl job submit` (or `deploy/run.sh` on the control machine) | a command in a container on the cards Ray grants it; queued until they are free, capped at its CPUs, freed on exit |

Connection: `FLEET_URL` (scheduler, port 9191), `FLEET_RUNS_URL` (runs API, 9192) and
`FLEET_ADMIN_TOKEN_FILE`; the control machine's `fleetctl` finds them itself. From
another machine: `ssh -N -L 9191:127.0.0.1:9191 -L 9192:127.0.0.1:9192 <control machine>`
and the admin token in `~/.config/zkm-fleet/admin.token`. `--json` on any command.

## Listing and reading

- `fleetctl job`: every job still queued or running (both kinds, however old), then the
  newest `-n` (default 10) of each kind. Filters: `--active`, `--type proof|run`,
  `--app NAME`, `--tenant T`.
- `fleetctl job ID`: one job's record (either kind).
- `fleetctl job logs ID`: a run's output (`-f` follows until it ends, `-n N` lines of
  history, `-n 0` only what comes next, `--rank k` for a `--nodes` run). A proof job has
  no output of its own: `logs` shows its record; a failed attempt's reason is in `error`.
- **Take IDs from the action's own output.** Never act on "the newest job": other
  people's jobs share the list.

## Submitting a run

```
fleetctl job submit --app <run app> --name <short-name> --gpus 1 --cpus 8 --gpu-mem 6 \
  --code git:/path/to/repo@<commit-or-branch> \
  --mount /data/<dataset-or-models>:ro \
  --env HF_HOME={out}/hf_home \
  -- python3 {code}/train.py --output {out}
```

Prints `{"id": "<name>-<8hex>", "state": "queued", "out": ...}`; keep the id.

Rules:

1. **Every path the job touches is in the request** (`--mount PATH:ro|rw`, repeat);
   nothing implicit. A mount appears at the same path inside the container, must exist,
   and must lie under the app's allowed roots. Read-only for what the job only reads
   (code, venv, models, dataset), read-write only for its job dir and output dir.
   Paths are paths on the runs API's machine (shared storage when there are several).
2. **Outputs go to `{out}`** (`$FLEET_OUT`, the run's own `<runs dir>/<id>/out`, mounted
   read-write automatically); don't ask for rw on shared trees just to write results.
   `{out}` and `{code}` expand in the command, `--env` and `--workdir`.
3. **Pin the code**: `--code git:/repo@ref` (a repo on the server; the commit is recorded,
   working-tree edits are not included) or `--code FILE.tar.gz` (a local tarball is
   uploaded; sha256 recorded; files and directories only, no links). It appears
   read-only at `{code}` (`$FLEET_CODE`). The repo or tarball must lie under the app's
   allowed roots (an uploaded one goes to the runs dir's `.uploads/`).
4. **Never pick a card yourself.** Request `--gpus N`; inside the container the granted
   card is `cuda:0` (`CUDA_VISIBLE_DEVICES` is set). Never start GPU work on a machine
   outside the fleet: that is what makes other jobs run out of memory.
5. **A Ray grant is not proof the card is empty** (work started outside the fleet is
   invisible to Ray): pass `--gpu-mem <GiB>`, the job's peak, or the app's default
   applies (an app without one wants the whole card, 95 % free). Short of it the run
   sits in `waiting_vram` (its `detail` names who holds the card) and fails after
   `--vram-wait` seconds (default 1800).
6. Don't download models or large files inside a job without asking first; `stage-model`
   below is the way to fetch weights.

Options: `--gpus 0.5` shares a card with another half-card run when both `--gpu-mem`
fit, `--gpus 0` is CPU-only, `--cpus N` caps the job. `--after ID` starts only after
that run succeeded (fails otherwise, with the reason): use it for train then eval.
`--retry N` retries any failure, `--restart-on-oom N` only out-of-memory ones;
`$FLEET_ATTEMPT` (1, 2, …) tells the job which attempt it is, so it checkpoints into
`{out}` and resumes when `FLEET_ATTEMPT > 1`. **Handle SIGTERM:** cancel and preemption
send it, then SIGKILL the app's `stop_timeout_s` (default 30 s) later; save a checkpoint
then. A service that needs the cards preempts a run of a `preemptible` app once it has
run `min_runtime_s`: the run queues again with the same id and `out/`, `FLEET_ATTEMPT`
counts on, and no `--retry` is spent. Metrics: one JSON object per line
appended to `$FLEET_METRICS` (`{out}/metrics.jsonl`); the record shows the latest.
Output is unbuffered. `--machine HOST` pins the machine; `--nodes N` spreads one run
over N machines (`--gpus`/`--cpus` on each, one container per machine, started together,
all ended when one fails; each sees `FLEET_NNODES`, `FLEET_NODE_RANK`,
`FLEET_MASTER_ADDR`, `FLEET_MASTER_PORT`, also as `MASTER_ADDR`/`MASTER_PORT`/`NODE_RANK`/
`NNODES` and torchrun's `PET_*`, so `torchrun train.py` works unchanged; `--machine`
repeated N times pins them). `--log FILE` overrides the default log (must be in an rw
mount). Refusals are immediate and say why: unknown app (400), disabled app (409),
a mount outside the allowed roots or missing, too many GPUs, a fractional card without
a memory figure, an unknown `--after` run, a bad `--code`.

## Watching, fetching, cleaning up

```
fleetctl job ID                 # record: state, cards, attempts, cause, metrics, out dir
fleetctl job logs ID -f         # follow the output until the run ends
fleetctl job fetch ID --out F   # the run's out/ as a tar.gz (a transfer that breaks off is an error)
fleetctl job cancel ID          # SIGTERM, SIGKILL stop_timeout_s later; cards freed (a finished run: 409)
fleetctl job preempt ID         # the same stop, then it queues again (as a service needing the cards does)
fleetctl job purge ID           # delete a finished run's record, out/ included
fleetctl job purge --older-than DAYS
```

States: `waiting_dependency` (its `--after`), `queued` (no card yet), `waiting_vram`,
`running`, `retrying`, `preempted` (being stopped; then `queued` again, `preemptions`
counts them, the attempt's `cause` is `preempted`), then `succeeded` / `failed` (`cause`:
`gpu-oom`, `host-oom`, `exit-code`, per-attempt records) / `cancelled`, or `lost` (its actor is gone, e.g.
Ray restarted; `container` says whether the job still runs; `cancel` removes it; a lost
run whose container still runs cannot be purged until then). Don't poll faster than
about 15 s; `logs -f` is the way to watch progress.

**Cleanup:** `purge` once you have fetched what you need. Files a job wrote into other
rw mounts are yours to remove, by literal path.

## Models

`deploy/run.sh stage-model <hf-repo> [--revision R] [--include GLOB]` (on the control
machine; `POST /models` on the runs API) downloads a Hugging Face repo into the fleet's
shared read-only model store (`model_store:` in `runs.yaml`, `hf_token_file:` for gated
repos), as a queued CPU run you can follow with `logs`. Only when the user asks for
that model.

## After a run

Report: run id, granted card(s), exit code / error, where the outputs are. Then clean
up what the task created unless the user wants the outputs kept.
