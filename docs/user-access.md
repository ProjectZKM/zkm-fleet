# User access to zkm-fleet

Administrators issue individual credentials for **proofs and one-shot run jobs**.
Users submit to explicitly approved apps and access jobs in their namespace and optional tag.
Tags remain isolated when they share a namespace, proof tenant or run profile. App types remain `proof`
and `run`.

Each credential has a stable namespace and an optional tag. Teammates and replacement keys
in the same tag share access to its jobs and uploads. Refreshing a
credential preserves jobs and uploads. See [namespace design](../README.md#namespaces-access-and-quotas)
for namespace administration.

## Request access

Send an access request to an administrator with your identity/team, proof tenant,
requested proof programs and run profiles, and resource needs. The administrator
reviews the request, registers necessary apps, and allocates a credential. An
issue or private operator channel is sufficient; there is no public signup API.

The scheduler and runs API stay private. The administrator also provides a private
connection or an SSH identity restricted to forwarding the required API ports.
A fleet credential does not itself grant SSH access. Ordinary users must not
receive an operator's SSH identity, shell access to fleet files, Ray access, the
admin token, cluster token or CA private key.

## Administrator: issue a credential

Use the administrator token on the control machine, or through your operator SSH
tunnel. Select an existing tenant even for a credential used only for runs. Grant
run profiles in the same namespace by name and proof programs by `tenant/FULL_ELF_SHA256` (64 hex digits,
not the abbreviated app display name). Repeat `--app` for every approved app.
At least one grant is required. A proof grant must already be registered; run
profile existence is checked when a job is submitted.

```bash
mkdir -m 700 alice-bundle
unset FLEET_TOKEN_FILE  # operator commands use FLEET_ADMIN_TOKEN_FILE
fleetctl namespace create alice --gpus 2 --cpus 16
fleetctl --namespace alice app add training --image APPROVED_IMAGE --allow /srv/training
fleetctl --namespace alice app add eth/FULL_ELF_SHA256
fleetctl credential issue --namespace alice --tenant eth --label alice \
  --app training --app eth/FULL_ELF_SHA256 \
  --endpoint https://127.0.0.1:51000 --domain-name stage \
  --tls-dir /etc/zkm-fleet/tls --out alice-bundle/alice.env
```

Replace the ELF and image placeholders, profile, tenant and paths with your deployment's
values. Adjust installer paths for your installation. The endpoint is the
client's **local forwarded** stage port. `--domain-name` must match the gateway
certificate (the installer default is `stage`). No public scheduler port is needed.
Proof user credentials require the mTLS gateway and `client_cert_identity: true`.
Do not forward the scheduler's raw stage backend that trusts gateway headers.

For run-only access, omit the proof `--app`, `--tls-dir` and `--domain-name`;
`--endpoint` is still required by the CLI but unused for run submission.

The bundle contains `alice.env`, `alice.token`, and, with TLS, `alice.ca.pem`,
`alice.client.pem` and `alice.client.key`, all created with mode 0600. Only the
signing address and API token hash are registered with the scheduler. Send the
bundle privately. The generated env file contains absolute paths from the issuing
machine; the recipient must update its token/certificate paths after copying it.
Never send the entire `--tls-dir` directory: it contains the CA private key.

## User: connect and submit

Keep this tunnel running with the restricted SSH identity supplied by your admin
(the SSH account/alias can differ):

```bash
ssh -N -o ExitOnForwardFailure=yes \
  -L 127.0.0.1:9191:127.0.0.1:9191 \
  -L 127.0.0.1:9192:127.0.0.1:9192 \
  -L 127.0.0.1:51000:127.0.0.1:51000 fleet-access@FLEET_CONTROL_HOST
```

The 51000 forward is only needed for proofs. Update `alice.env` with the bundle's
local paths and forwarded URLs, then load it:

```bash
set -a
. ./alice-bundle/alice.env
set +a
fleetctl app
fleetctl job submit --app training --gpus 1 --cpus 4 \
  --code ./job-code.tar.gz -- python3 '{code}/train.py' --output '{out}'
```

`FLEET_TOKEN_FILE` (or `--token-file`) selects the user API token and takes
precedence over `FLEET_ADMIN_TOKEN_FILE`. The same token authorizes HTTP calls to
both the scheduler's read endpoints and the runs API. Proof submission continues
through the unchanged Ziren SDK using the bundle's signing key and mTLS files.
See [local CLI and API submission](submit-jobs.md) for uploads and output retrieval.
The scheduler's `public_url` must also be reachable through the user's private
connection (for example the forwarded `http://127.0.0.1:9191`).

User run containers have **no network access** and support one machine per job
(multiple GPUs on that machine are allowed). This keeps submitted code away from
internal Ray and control services. Supply dependencies in the image, upload code,
and mount administrator-approved datasets/models. Shared model staging and
multi-machine/networked runs remain operator operations. Grants must name curated
profiles with appropriate images, environment variables and allowed filesystem
roots; granting a profile also grants access to those configured resources.
Mounts overlapping fleet configuration or managed job storage are refused even
if a profile has a broad allow-list. Local code uploads belong to the credential's tag; other tags cannot submit
their server paths. A replacement credential in the same namespace and tag can reuse them.

Namespace GPU/CPU caps apply to proofs and runs together across all tags.
A tag can also cap its own total units, and both caps apply to admission. They limit concurrent units and impose no duration limit. Runs that
fit the caps wait in `waiting_quota` when all units are occupied; oversized run
requests receive HTTP 422. Proofs wait for a prover node whose full allocation
fits. Administrators manage caps with `fleetctl namespace quota NAME --gpus N
--cpus N`; see [namespace quotas](../README.md#gpu-and-cpu-quotas).

## Permissions and revocation

| Action | User | Administrator |
|---|---|---|
| Submit proofs | Granted programs in the assigned tenant | Authorized tenants/programs |
| Submit one-shot runs | Granted profiles and their resource/path limits | Any configured profile |
| List apps and jobs | Granted apps; tag jobs and counts | All |
| Read status, logs and outputs | Tag jobs | All jobs |
| Cancel, preempt or purge runs | Tag jobs, subject to lifecycle rules | All runs |
| Use a run dependency | Tag runs | Any run |
| Manage apps, nodes, credentials or shared model staging | No | Yes |
| Bulk purge by age | No | Yes |

```bash
fleetctl credential
fleetctl credential ADDRESS
fleetctl credential revoke ADDRESS
```

Revocation blocks subsequent proof and run requests, including polling and output
fetches, without a restart. The runs API validates each user request against the
scheduler's configured private `scheduler_admin` address and fails closed if it
cannot validate it. Already accepted jobs keep running, and an output transfer
already in progress can finish; an administrator can stop accepted jobs explicitly.
Refresh the signing key, API token and optional mTLS certificate together:

```bash
fleetctl credential refresh OLD_ADDRESS \
  --endpoint https://127.0.0.1:51000 --domain-name stage \
  --tls-dir /etc/zkm-fleet/tls --out alice-bundle/alice-next.env
# Deliver the replacement privately and verify access to earlier jobs, then:
fleetctl credential revoke OLD_ADDRESS
```

Run these as an administrator with `FLEET_TOKEN_FILE` unset. Refresh copies the
old credential's namespace, tag, tenant, app grants and label. The old key remains
active until explicitly revoked, so the user can verify the replacement first.
Previously submitted jobs and uploaded archives remain accessible with the new
key. Old public-values URLs stop working after revocation; polling with the new
key returns a fresh URL. `fleetctl namespace suspend alice` blocks every key in
the namespace; `resume alice` restores keys that were not individually revoked.

Use a fresh database for this release. Incompatible development databases must be
reset; old group names and upload paths are unsupported. Operator proof credentials
without app grants have tenant-wide SDK access and no runs API token. Administrator
access remains unchanged. Upgrade the scheduler, bridge on every execution machine, runs API actor and local
CLI together to use user access. Restart stale Ray worker processes before enabling user
submissions so every run uses the updated isolation rules; do not issue a user an admin token as a fallback.

A namespace is the resource accounting and billing unit. A key may optionally have
a tag: `fleetctl credential issue --namespace NAME --tag TAG ...`. Tags isolate
jobs and uploads and need no separate creation step. An untagged credential shares
the namespace's untagged work.
Set an optional aggregate tag cap with
`fleetctl namespace quota NAME --tag TAG --gpus N --cpus N`; every tag also consumes
the namespace's shared quota. Refresh preserves the optional tag.
See [namespace design](../README.md#namespaces-access-and-quotas).
