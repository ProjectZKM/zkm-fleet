# Submit a job from your local machine

Use `fleetctl job submit` or the runs HTTP API to queue a command in a container.
Both use an existing **one-shot run profile**. A persistent app such as ComfyUI
is managed by `fleetctl app enable/disable`; it is not a job submission profile.
Proof jobs are submitted through the Ziren SDK network prover instead.

Use an administrator-issued user token for approved profiles and your own jobs.
See [user access](user-access.md) to request a credential and configure a private
connection. Operators can use the admin token for fleet-wide access.

## Connect to the control machine

Build the local CLI with `cargo build --release --locked -p fleetctl`, or install a
binary built for your local OS and architecture. Put `fleetctl` on your PATH.

Keep this SSH tunnel running in a separate terminal:

```bash
ssh -N -o ExitOnForwardFailure=yes \
  -L 127.0.0.1:9191:127.0.0.1:9191 \
  -L 127.0.0.1:9192:127.0.0.1:9192 FLEET_CONTROL_HOST
```

Obtain your individual credential bundle from an administrator, store its files
with mode `0600`, and set your token path and forwarded URLs:

```bash
export FLEET_URL=http://127.0.0.1:9191
export FLEET_RUNS_URL=http://127.0.0.1:9192
export FLEET_TOKEN_FILE="$HOME/.config/zkm-fleet/alice.token"
fleetctl app --type run
```

Ordinary users use the restricted SSH identity supplied by the administrator;
substitute its account/alias in the tunnel command. Use available local ports if
9191 or 9192 are occupied and update the URLs accordingly. APIs stay private.
Operators can set `FLEET_ADMIN_TOKEN_FILE` instead, with `FLEET_TOKEN_FILE` unset.
The proof signing key and client certificate are separate from the user HTTP token.

## Submit with fleetctl

Choose an enabled profile from `fleetctl app --type run`; persistent app records
have `persistent: true` and cannot be used as profiles. The examples below assume
an existing profile named `training`, with Python available in its configured
image. An administrator first creates a namespace, then registers a profile with
`fleetctl --namespace NAME app add APP --image IMAGE --allow ROOT`.
The app must belong to the submitting credential's namespace.

For code on your laptop, archive the files needed for the job:

```bash
tar -czf job-code.tar.gz -C ./my-job .
fleetctl --json job submit --app training --name local-train \
  --gpus 1 --cpus 4 --gpu-mem 8 \
  --code ./job-code.tar.gz \
  -- python3 '{code}/train.py' --output '{out}'
```

`fleetctl` uploads the archive, submits the job, and prints its ID. Archives may
contain directories and regular files, but not symlinks or hard links. Keep model
weights and datasets outside the archive. The upload request limit is 512 MiB.

- `{code}` is the captured code directory, mounted read-only.
- `{out}` is the job's output directory, mounted read-write automatically.
- `--gpus 0` requests a CPU-only run. Requested resources must fit the profile's cap
  and the selected machine; unavailable capacity leaves the job queued.
- `--mount /srv/datasets:ro` mounts a path on the **execution machines**, not a
  laptop directory. It must exist and be under the profile's allowed roots.
- `--code git:/srv/my-repo@COMMIT` captures a repository on the **runs API machine**;
  it does not upload a local Git checkout. The repository must be under an allowed
  root. Uploaded archives are allowed through the managed upload directory.
- The image and runtime dependencies come from the profile. Uploading Python code
  does not install its packages automatically.

Keep the returned ID and use it explicitly:

```bash
fleetctl job JOB_ID
fleetctl job logs JOB_ID -f
fleetctl job fetch JOB_ID --out result.tar.gz
fleetctl job cancel JOB_ID   # only if you want to stop it
```

## Submit through the HTTP API

Use the same tunnel and token. All endpoints below are on `FLEET_RUNS_URL` and
require `Authorization: Bearer <user token>` (or an operator admin token). `GET /healthz` is unauthenticated.

| Operation | Request |
|---|---|
| List profiles and persistent apps | `GET /apps` |
| Upload a local code archive | `PUT /uploads` with raw archive bytes |
| Queue a run | `POST /runs` with JSON |
| Read status | `GET /runs/JOB_ID` |
| Read log tail | `GET /runs/JOB_ID/logs?lines=100` |
| Fetch outputs | `GET /runs/JOB_ID/out.tar.gz` |
| Cancel | `POST /runs/JOB_ID/cancel` |

The JSON field is **`profile`**, corresponding to CLI `--app`. `command` is a
non-empty array of arguments, not a shell command string. Use `bash -lc` explicitly
if a job needs shell operators. Optional fields include `gpus`, `cpus`, `gpu_mem`,
`mount` (array), `env` (array of `K=V` strings), `code`, `workdir`, and `name`.

This Python example uploads local code and submits a run without putting the token
in a command-line argument:

```python
import json
import os
from pathlib import Path
from urllib.request import Request, urlopen

base = os.environ["FLEET_RUNS_URL"].rstrip("/")
token = Path(os.environ["FLEET_TOKEN_FILE"]).read_text().strip()


def request(method, path, data=None, content_type="application/json"):
    req = Request(
        base + path,
        data=data,
        method=method,
        headers={"Authorization": f"Bearer {token}", "Content-Type": content_type},
    )
    with urlopen(req, timeout=3600) as response:
        return json.load(response)


upload = request("PUT", "/uploads", Path("job-code.tar.gz").read_bytes(),
                 "application/octet-stream")
job = request("POST", "/runs", json.dumps({
    "profile": "training",
    "name": "api-train",
    "gpus": 1,
    "cpus": 4,
    "gpu_mem": 8,
    "code": upload["path"],
    "command": ["python3", "{code}/train.py", "--output", "{out}"],
}).encode())
print(job["id"])
```

`code` must contain the upload response's **server path**, not the laptop archive
path. Omit `code` for a command already supplied by the image or mounted server
files. Check HTTP errors: unknown profiles return 400, disabled profiles return
409, and invalid resource/path requests are refused. A queued response confirms
submission, not successful execution. If a submission response is lost, inspect
existing runs before retrying: the API does not accept a client idempotency key.
