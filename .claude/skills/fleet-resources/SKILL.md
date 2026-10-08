---
name: fleet-resources
description: >-
  Explain zkm-fleet resources and use fleetctl to administer namespaces, concurrent
  resource quotas, optional tags and user credentials, including refresh and revocation.
---

# Resources and access

| Resource | Meaning |
|---|---|
| Namespace | Administrator-created ownership and billing unit with a unique name and immutable ID |
| App | Approved proof program or run app belonging to one namespace |
| Job | Execution of an app, with submitting identity, namespace, tag and inherited priority |
| Credential | User access to one namespace, explicit app grants and an optional tag |

Tags are optional attributes, not separately created resources. There is no Group
resource. Credentials sharing a namespace/tag share its work; different tags
isolate jobs, uploads, dependencies and results. Untagged credentials share the
namespace's untagged work. App grants remain explicit per credential.

## Client identity and permissions

Use the administrator-provided connection settings and credential bundle.
`FLEET_TOKEN_FILE` selects a user token; `FLEET_ADMIN_TOKEN_FILE` selects the
administrator token when the user token is unset. `--json` returns structured
records. A credential grants API access once its endpoint is reachable.

Users can inspect granted apps, submit proofs/runs and manage their tag's runs.
The namespace and tag are inferred from their credential; selectors cannot grant
access elsewhere. Namespace creation, quotas, app policy and credentials require
administrator access. Administrators can filter with `--namespace NAME --tag TAG`.

## Administrator: namespaces and quotas

```bash
fleetctl namespace create team --gpus 8 --cpus 64
fleetctl namespace list
fleetctl namespace get team
fleetctl namespace quota team --gpus 4 --cpus 32
fleetctl namespace quota team --tag research --gpus 2 --cpus 16
fleetctl namespace get team --tag research
fleetctl namespace suspend team
fleetctl namespace resume team
```

Create the namespace before registering apps or issuing credentials. Names start
with a lowercase letter and contain lowercase letters, digits or hyphens, up to
63 characters. `system` and `admin` are reserved. Rename and deletion are not
currently supported.

Quotas cap concurrent GPU/CPU units across proofs and runs, with no duration
limit. Tags share the namespace cap; an optional tag cap additionally limits
that tag's total units. Fractional GPU caps are supported. Zero allows no units
of that resource. A quota update replaces both limits: an omitted limit becomes
unlimited. `fleetctl namespace quota team` clears both namespace caps; include
`--tag TAG` to clear that tag's caps instead.

Runs larger than a cap are refused; jobs that fit wait when units are occupied.
Proof admission requires the selected prover's full allocation to fit. Lowering
caps lets admitted work finish and restricts further admissions. Release occurs
after execution cleanup is confirmed. Credential refresh does not reset usage.
Suspension blocks all user credentials in a namespace without cancelling accepted
jobs; resuming does not reactivate individually revoked keys.

## Administrator: credentials

Register the approved apps first; see [fleet-apps](../fleet-apps/SKILL.md).
Issue a run-access bundle with the endpoint supplied for that deployment:

```bash
fleetctl credential issue --namespace team --tag research --tenant TENANT \
  --label alice --app training --endpoint STAGE_ENDPOINT --out alice.env
fleetctl credential
fleetctl credential ADDRESS
fleetctl credential refresh ADDRESS --endpoint STAGE_ENDPOINT --out alice-next.env
fleetctl credential revoke ADDRESS
```

`--tag` is optional. Repeat `--app` for each grant. Proof grants use
`TENANT/FULL_ELF_SHA256`; run grants use the profile name. Select an existing
tenant even for run-only access. The endpoint argument is required but unused
for run-only submission.

For proof access, issue the signing key and client mTLS certificate bundle using
the administrator's configured `--tls-dir TLS_DIR` and `--domain-name NAME` on
issue/refresh, and grant the proof app. Client bundles contain the API token,
signing key and optional certificates. Deliver privately and update local file
paths after copying. Client credentials are distinct from administrator access.

Refresh preserves namespace, optional tag, tenant, grants and label. Earlier
jobs and uploads remain accessible, and apps are not recreated. The old key
remains active: deliver and verify the replacement, then revoke the old address.
Revocation blocks subsequent requests without cancelling accepted work.

## HTTP resource interface

| Resource action | Request on scheduler API | Access |
|---|---|---|
| Current user scope | `GET /access` | User token |
| List/create namespaces | `GET` / `POST /admin/namespaces` | Administrator |
| Describe namespace | `GET /admin/namespaces/NAME` | Administrator |
| Replace quota | `PUT /admin/namespaces/NAME/quota` | Administrator |
| Suspend/resume | `PUT` / `DELETE /admin/namespaces/NAME/suspended` | Administrator |
| List/register credentials | `GET` / `POST /admin/credentials` | Administrator |
| Revoke credential | `DELETE /admin/credentials/ADDRESS` | Administrator |

Quota JSON uses `gpus` and `cpus` numbers or `null` for unlimited; add `?tag=TAG`
to inspect/update a tag's cap. Prefer `fleetctl credential issue/refresh` for
creating client bundles. User tokens authenticate client HTTP requests; proof
submissions also use the signing key and client certificate.

See [user access](../../../docs/user-access.md) for bundle usage,
[fleet-jobs](../fleet-jobs/SKILL.md) for submission, and the
[resource reference](../../../README.md#namespaces-access-and-quotas) for ownership.
