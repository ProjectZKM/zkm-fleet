# Migrations

One-time moves between specific older and newer releases. A routine upgrade (see
[maintenance](maintenance.md)) needs none of these; each section names the versions
it applies to.

## Machine report version 2 to 3

Upgrading from report version 2 changes the control host and every node at once.
Let in-flight commands finish, stop the scheduler and every node, and migrate their
stored state before starting the new binaries. The scheduler's per-node records move
from `workloads/services` to `workloads/deployments` (`service` becomes `spec`),
stored commands and `service:` reservations take the new names, and each registered
proof app needs its app record. A node's `etc/services.yaml` becomes
`etc/apps.yaml` (`services:` becomes `apps:`) and its `node-start.sh` passes
`--apps`. Back up `data_dir`, Postgres and the node's `etc/` first.

## The scheduler's SDK stage API (retired)

The release that moved to the app model retired the scheduler's SDK stage API
(`GenerateProof`/`GetStatus` on `stage_addr`): SDK clients prove through their
[app-owned proof endpoint](reference.md#app-owned-proof-endpoint). Remove
`stage_addr`, `tls`, `public_url`, `long_poll_ms` and `proof_output_dir` from
`fleet.yaml` first; the scheduler refuses to start with them. Let proof jobs
submitted through the stage API finish: one still queued or running fails when the
new scheduler starts. Remove the gateway's `/stage.v1.StageService/` route, which
has nothing behind it. The control host's `data_dir/elf`, `data_dir/jobs` and
`data_dir/proofs` held that API's payloads; delete them once rollback is no longer
needed. A node ignores `proof_output_dir` in `apps.yaml`.

## Database schema 14 or 15 to 16

Schema 16 installations retain namespaces and tokens across ordinary updates. A
schema 14 or 15 database migrates automatically when the scheduler starts, in one
transaction: each app takes a name (its tenant's when that tenant had one app in
the namespace, else `TENANT-<first 8 hex of the ELF id>`) and the default
placement policy, jobs name their app, `TENANT/ELF` grants become app names, and
namespace-less tenant signer tokens are removed. Update each prover's `apps.yaml`
entry to `app: NAME` plus `elf_id`. Remove `tenants`, `default_tenant`,
`require_signature` and `admin_token_file` from `fleet.yaml` first; the scheduler
refuses to start with them. Issue an administrator token with
`fleet-scheduler -c fleet.yaml issue-admin --label NAME --out FILE`, since the old
`admin_token_file` no longer applies. Only another schema requires an explicit
metadata reset. A reset invalidates namespace access, so recreate and redistribute
scoped credentials before resuming clients.

## Keeping state across any update

Keep the existing `etc/`, Postgres database, control `data_dir` and node `work/`.
Back them up before an update; the rollback procedures in
[maintenance](maintenance.md) restore programs but not data.
