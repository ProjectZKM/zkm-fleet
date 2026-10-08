---
name: fleet-apps
description: >-
  Use the zkm-fleet app interface to list and inspect approved apps, or administer
  app registration, priority, enable/disable and deletion with fleetctl.
---

# Apps

An app defines what may run. Every app belongs to an existing namespace; jobs
inherit that namespace and the app's priority at submission. App types are
`proof` and `run`. A persistent app is a run app with `persistent: true`.

| App | Name | Job submission |
|---|---|---|
| Proof | `TENANT/FULL_ELF_SHA256`; lists abbreviate the hash | Ziren SDK network prover |
| One-shot run | Profile name, such as `training` | `fleetctl job submit --app NAME` or `POST /runs` |
| Persistent run | App name, such as `comfyui` | Managed with enable/disable; not a submission profile |

Users can inspect granted apps in their credential's namespace. App creation,
policy changes and deletion require administrator access. Namespaces are created
separately; adding an app never creates a namespace. Tags are optional access
attributes, not another app type.

## Inspect

```bash
fleetctl app
fleetctl app --type proof
fleetctl app --type run
fleetctl --namespace NAME app APP
fleetctl --json app
```

Select `--namespace NAME` when an app name matches more than one namespace. Use
the full ELF hash when an abbreviated proof app name is ambiguous. Records show
namespace, priority, enabled state and job counts.

## Administrator: register and manage

```bash
fleetctl --namespace NAME app add TENANT/ --elf ./guest.elf --priority 100
fleetctl --namespace NAME app add training --image APPROVED_IMAGE \
  --allow /srv/training --max-gpus 2 --priority 10
fleetctl --namespace NAME app priority APP 20
fleetctl --namespace NAME app disable APP
fleetctl --namespace NAME app enable APP
fleetctl --namespace NAME app delete APP
fleetctl --namespace NAME app delete APP --yes
```

Register a new proof program before clients submit it. `--elf` computes the
program's SHA256 locally; a full hash may be supplied in the app name instead.
Proof registration requires an existing tenant. The ELF arrives with the first
proof submission. Registering the same app twice in a namespace returns 409.

A run app specifies an approved image, mount roots and GPU limit. Optional flags
include `--user UID:GID`, `--gpu-mem GiB`, `--shm-gb N` and repeated `--env K=V` or
`--allow ROOT`. Mount roots refer to execution-machine paths. Persistent app
creation/deletion is outside the current app interface.

| Action | Proof or one-shot app | Persistent app |
|---|---|---|
| Disable | Refuse new submissions; accepted jobs finish | Stop execution and release GPU reservations after cleanup |
| Enable | Accept submissions again | Start execution again |
| Delete | Refused while jobs are active | Unsupported through this interface |

Deleting a proof app removes its job history and results in the selected
namespace. Deleting a one-shot app keeps its past job records and outputs.
Without `--yes`, deletion describes the proposed change.

## Priority

Priority is a signed integer, default `0`; higher values take precedence. Only
administrators set it. Jobs keep the value inherited at submission across retries
and requeue; editing an app affects new submissions. Users cannot override a
job's priority. Pending jobs with equal priority use creation order, then ID.

Automatic preemption considers eligible lower-priority runs in the same namespace.
Equal/higher-priority runs, active proofs and persistent apps are protected.
Namespace and optional tag quotas still apply. Updating a persistent app's
priority alone preserves its current execution.

Use [fleet-jobs](../fleet-jobs/SKILL.md) for submission and job lifecycle,
[fleet-resources](../fleet-resources/SKILL.md) for namespaces and credentials,
and [local submission](../../../docs/submit-jobs.md) for client API examples.
