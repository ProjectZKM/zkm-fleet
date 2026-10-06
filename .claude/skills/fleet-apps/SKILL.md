---
name: fleet-apps
description: >-
  Manage zkm-fleet apps with `fleetctl app`: proof apps (a tenant's registered program)
  and run apps (runs.yaml profiles). List, describe, add, disable / enable, delete; the
  rule that a job runs only for an existing app; registering a new guest ELF before a
  client switches to it. Use for anything about what may run on the fleet.
---

# Apps on zkm-fleet

An **app** is what can run; a **job** is one instance of it (see fleet-jobs).
**A job runs only for an existing app**: the scheduler refuses a proof whose program is
not registered for its tenant, and the runs API refuses a run whose profile is not in
`runs.yaml`.

| app kind | what it is | named | defined by | its jobs |
|---|---|---|---|---|
| proof app | one tenant's program (an ELF; its id is the sha256 of the ELF) | `tenant/<first 8 hex of the elf id>` | `fleetctl app add tenant/<elf id>`; a row in Postgres (`fleet_apps`) | proofs through the stage API |
| run app | a profile: image, user, GPU cap, default GPU memory, shm, the roots a run may mount, env | the profile's name | `fleetctl app add NAME --image I --allow ROOT …`; a block in `/etc/zkm-fleet/runs.yaml` on the control machine (example: `deploy/runs.example.yaml`) | runs (`fleetctl job submit --app NAME …`) |

A proof refused before its app was known (bad signature, unknown program) is recorded
under `(rejected)`, which is not an app and takes nothing; `fleetctl app delete
'(rejected)' --yes` clears those records.

## Commands

```
fleetctl app                                   # both kinds: ENABLED, ACTIVE, JOBS, OK, FAILED, LAST JOB, detail (anchor node / image)
fleetctl app NAME                              # one app's record and its 5 newest jobs
fleetctl app add TENANT/<elf id>               # a proof app (TENANT/ --elf FILE hashes a local ELF instead)
fleetctl app add NAME --image I --allow ROOT [--allow ROOT …] [--user UID:GID] [--max-gpus N] [--gpu-mem GiB] [--shm-gb N] [--env K=V …]
fleetctl app disable NAME                      # no new jobs; the queued and running ones finish; history stays
fleetctl app enable NAME
fleetctl app delete NAME                       # says what it would delete
fleetctl app delete NAME --yes                 # deletes; refused while any of its jobs is queued or running
```

`--type proof|run` narrows the list; `--json` on any command. A proof app's name may
be given with the full elf id; when an 8-hex prefix matches two programs the command
refuses and lists them.

## Proof apps

- `add`: the tenant must be configured in `fleet.yaml` (404 otherwise); the elf id is
  64 hex digits (400 otherwise); an app registered twice is 409. The ELF itself is not
  uploaded: it arrives with the first accepted job (the SDK sends it when the scheduler
  does not have it yet).
- **A new program (a guest change produces a new elf id) must be registered before
  the client switches to it**, or every proof is refused with InvalidParameter
  "program … is not an app of tenant …". Compute the id (`sha256sum guest.elf`, or
  `fleetctl app add tenant/ --elf guest.elf` does it), register it, then switch the client.
  The old app stays until you disable or delete it.
- `delete` removes the registration, the jobs' records, inputs, proofs and public
  values, the anchor (and the node's reservation for an exclusive tenant), and the ELF
  unless another tenant has the same program registered.

## Run apps

- `add` writes the profile as a block into `runs.yaml`, re-reads the result, and writes
  it only when nothing else in the file changed; comments and the other profiles stay as
  they were. Every `--allow` root is an absolute path on the machine runs land on.
  Omitted flags take the server's defaults (max_gpus 8, shm 16 GiB, the whole card).
- `disable` is `disabled: true` in the profile (a submit answers 409); `enable` removes it.
- `delete` cuts the profile out of `runs.yaml`; its past runs stay listed (under a name
  that is no longer an app, shown as "removed") with their logs and outputs.
- Editing `runs.yaml` by hand is equivalent; the runs API reads the file on every request.

## Lower-risk removal

`disable`, wait for `fleetctl job --app NAME --active` to be empty, then `delete`.
Without `--yes`, `delete` only describes what would go.
