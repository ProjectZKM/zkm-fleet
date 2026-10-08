---
name: fleet-jobs
description: >-
  Submit, inspect, follow, fetch, cancel, preempt and purge zkm-fleet jobs using
  fleetctl or the client HTTP API; inspect proof jobs submitted through the SDK.
---

# Jobs

A job is one execution of an existing app. Proofs are submitted through the Ziren
SDK network prover (`ZKM_PROVER=network`). One-shot runs use `fleetctl job submit`
or the runs HTTP API. Persistent apps use the app enable/disable interface.

## Client access

Use the API URLs and credential bundle supplied by an administrator:

```bash
export FLEET_URL=SCHEDULER_API_URL
export FLEET_RUNS_URL=RUNS_API_URL
export FLEET_TOKEN_FILE=/path/to/user.token
fleetctl app
```

The connection must already reach those URLs. Credentials authorize API requests;
they do not provide network access. `FLEET_TOKEN_FILE` takes precedence over
`FLEET_ADMIN_TOKEN_FILE`. Use `--json` for structured output.

Users submit only to granted apps in their namespace. Jobs, uploads, dependencies,
logs and outputs are isolated by namespace and optional tag. Credentials sharing
the same namespace/tag can access its work, including after credential refresh.
An administrator may select `--namespace NAME --tag TAG` when inspecting jobs.

## Submit a run

Choose an enabled one-shot app from `fleetctl app --type run`:

```bash
tar -czf job-code.tar.gz -C ./my-job .
fleetctl --json job submit --app training --name example \
  --gpus 1 --cpus 4 --gpu-mem 8 \
  --code ./job-code.tar.gz --mount /srv/datasets:ro \
  -- python3 '{code}/train.py' --output '{out}'
```

Keep the returned ID. Acceptance means the job is submitted, not that it has
finished or obtained resources. If the submission response is lost, inspect
existing jobs before retrying; run submission has no client idempotency key.

- `--code FILE.tar.gz` uploads a local archive of regular files/directories;
  links are refused. `{code}` is read-only; `{out}` is the job's writable output.
- `--mount PATH:ro|rw` refers to an existing execution-machine path under the
  app's approved roots. A local path in an HTTP request does not upload files.
- Request GPUs with `--gpus`; `--gpus 0` is CPU-only and fractions below one share
  a card. Fractional requests require `--gpu-mem` or an app memory default.
- `--gpu-mem GiB` declares required free GPU memory. Insufficient memory produces
  `waiting_vram`; `--vram-wait SECONDS` limits that wait.
- `--after ID` waits for a job in the same namespace/tag to succeed. `--retry N`
  retries failures; `--restart-on-oom N` adds retries for memory failures.
- `--env K=V` passes environment values. `{code}` and `{out}` also expand in
  environment values and `--workdir`.

User runs support one machine per job and have no network access. Dependencies
must be available in the approved image or supplied data. Requested units must
fit the app limits and namespace/tag quotas. Jobs inherit app priority; there is
no per-job priority override.

## Inspect and manage

```bash
fleetctl job
fleetctl job --active --app training
fleetctl job --type proof
fleetctl job JOB_ID
fleetctl job logs JOB_ID -f
fleetctl job fetch JOB_ID --out result.tar.gz
fleetctl job cancel JOB_ID
fleetctl job preempt JOB_ID
fleetctl job purge JOB_ID
```

`--active`, `--type proof|run`, `--app NAME`, `--tenant TENANT` and `-n N` filter
job lists. Use the literal returned job ID. Proof job inspection shows status and
errors; log/fetch, cancellation, manual preemption and purge actions apply to runs.
Bulk `fleetctl job purge --older-than DAYS` requires administrator access.
Purge deletes a finished run's outputs as well as its record.

| State | User-visible meaning |
|---|---|
| `waiting_dependency` | Waiting for the requested predecessor |
| `waiting_quota` | Waiting for physical capacity or namespace/tag units |
| `queued` | Accepted and awaiting execution |
| `waiting_vram` | Waiting for enough free GPU memory |
| `running`, `retrying` | Executing or retrying an attempt |
| `preempted` | Stopping before resource admission and requeue |
| `succeeded`, `failed`, `cancelled`, `lost` | Finished, failed, cancelled or execution status lost |

Cancellation and preemption send SIGTERM, then SIGKILL after the app's grace
period. Checkpoint into `{out}` and use `$FLEET_ATTEMPT` to resume. Preemption
preserves the job ID, output and inherited priority without spending a retry.
Automatic preemption stays inside the namespace and targets lower-priority
eligible runs. Active proofs are protected. Units remain reserved until cleanup
is confirmed; unreachable execution can delay release and purge.

## HTTP interface

Send `Authorization: Bearer <user-token>` on client requests. Scheduler reads
use `FLEET_URL`; run actions use `FLEET_RUNS_URL`.

| Operation | Request |
|---|---|
| Scheduler apps/jobs | `GET /admin/apps`, `GET /admin/jobs`, `GET /admin/jobs/ID` |
| Run apps/jobs | `GET /apps`, `GET /runs`, `GET /runs/ID` |
| Upload code | `PUT /uploads` with raw tar.gz bytes |
| Submit run | `POST /runs` with JSON |
| Logs and outputs | `GET /runs/ID/logs`, `GET /runs/ID/out.tar.gz` |
| Cancel or preempt | `POST /runs/ID/cancel`, `POST /runs/ID/preempt` |
| Purge finished run | `DELETE /runs/ID` |

Run submission JSON uses `profile` for CLI `--app` and a string array for
`command`, for example:

```json
{"profile":"training","gpus":1,"cpus":4,"command":["python3","--version"]}
```

Pass the upload response's `path` as `code`. Identity, namespace, tag and priority
come from the verified credential and app. Unknown apps, disabled apps, denied
grants and invalid paths/resources are refused. Oversized run requests return
422; occupied capacity or quota leaves accepted jobs waiting.

See [local submission](../../../docs/submit-jobs.md) for a complete HTTP example,
[user access](../../../docs/user-access.md) for client setup, and
[fleet-resources](../fleet-resources/SKILL.md) for access and quota semantics.
