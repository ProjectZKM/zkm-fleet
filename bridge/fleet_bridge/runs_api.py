"""One-shot runs over HTTP, for `fleetctl job`: the same actions as `run.sh`, behind
the admin token, so runs can be submitted from any machine that reaches the port.

    POST   /runs                     submit (JSON: the RunArgs fields marked `remote`, plus "command")
    GET    /runs?n=N[&active=1][&profile=P]  the newest runs (only the active ones, one app's)
    GET    /apps                     the run apps (runs.yaml profiles) and their runs
    DELETE /apps/{name}              remove a run app (its profile), refused while it has runs going
    GET    /runs/{id}                status
    GET    /runs/{id}/logs           ?rank=K&lines=N (the tail) or &offset=O (what came after O)
    POST   /runs/{id}/cancel
    DELETE /runs/{id}                purge one finished run
    DELETE /runs?older_than=DAYS     purge finished runs older than that
    GET    /runs/{id}/out.tar.gz     the run's out/
    PUT    /uploads                  a code tarball (body); the answer's path goes in "code"
    POST   /models                   stage a Hugging Face repo ({"repo", "revision", "include"})
    GET    /healthz                  no token

It runs as a `kind: runs-api` service (a Ray actor on the machine that holds the
profiles file and the runs dir), so a submit goes through the same code as run.sh:
paths in `mount`, `log`, `workdir` and a `git:` code source are paths on that
machine; a code tarball on the client's machine is uploaded first (fleetctl does it).
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import socket
import threading
import urllib.parse
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import ray

from . import runs
from .actors import _STOPPING, _stopping, _watch, spec_hash
from .model import Health, ModelError, Record, ReplicaSpec, RunArgs, Uploaded, from_dict

MAX_BODY = 512 << 20  # a code tarball


class ApiError(Exception):
    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status


class Api:
    """The actions, on a fixed profiles file and runs dir."""

    def __init__(self, profiles: str, runs_dir: str) -> None:
        self.profiles, self.runs_dir = profiles, runs_dir
        self.lock = threading.Lock()  # a submit and an app's delete do not interleave
        os.makedirs(os.path.join(runs_dir, runs.UPLOADS_DIR), exist_ok=True)

    def args(self, **changes: Any) -> RunArgs:
        """RunArgs for an action: the server's paths, the caller's choices."""
        return from_dict(RunArgs, {"action": "submit", "profiles": self.profiles, "runs_dir": self.runs_dir, **changes})

    def submit(self, body: dict[str, Any]) -> Record:
        command = body.pop("command", None)
        if not isinstance(command, list) or not command or not all(isinstance(c, str) for c in command):
            raise ApiError(400, "command: a non-empty list of strings")
        unknown = set(body) - RunArgs.remote_fields()
        if unknown:
            raise ApiError(400, f"unknown or server-side fields: {sorted(unknown)}")
        with self.lock:
            return runs.submit(self.args(**body), command, runs.load_config(self.profiles))

    def delete_app(self, name: str) -> Record:
        with self.lock:
            return runs.delete_app(self.args(), name)

    def upload(self, data: bytes) -> Uploaded:
        if not data:
            raise ApiError(400, "empty body")
        sha = hashlib.sha256(data).hexdigest()
        os.makedirs(os.path.join(self.runs_dir, runs.UPLOADS_DIR), exist_ok=True)
        path = os.path.join(self.runs_dir, runs.UPLOADS_DIR, f"{sha}.tar.gz")
        if os.path.exists(path):
            os.utime(path)  # uploaded again now: `purge --older-than` keeps it
        else:
            tmp = f"{path}.{uuid.uuid4().hex[:8]}.tmp"
            with open(tmp, "wb") as f:
                f.write(data)
            os.replace(tmp, path)
        return Uploaded(path=path, sha256=sha, bytes=len(data))

    def stage_model(self, body: dict[str, Any]) -> Record:
        repo = body.get("repo")
        if not isinstance(repo, str):
            raise ApiError(400, "repo: a Hugging Face repo id")
        a = self.args(action="stage-model", run_id=repo, revision=body.get("revision", "main"),
                      include=body.get("include", []), profile=body.get("profile"))
        return runs.stage_model(a, runs.load_config(self.profiles))


def _query(path: str) -> tuple[list[str], dict[str, str]]:
    url = urllib.parse.urlsplit(path)
    parts = [urllib.parse.unquote(p) for p in url.path.strip("/").split("/") if p]
    return parts, {k: v[-1] for k, v in urllib.parse.parse_qs(url.query).items()}


def _int(q: dict[str, str], key: str, default: int | None) -> int | None:
    try:
        v = int(q[key]) if key in q else default
    except ValueError:
        raise ApiError(400, f"{key}: an integer") from None
    if v is not None and v < 0:
        raise ApiError(400, f"{key}: not negative")
    return v


def _float(q: dict[str, str], key: str) -> float:
    try:
        return float(q[key])
    except ValueError:
        raise ApiError(400, f"{key}: a number") from None


class _Chunked:
    """A response body in HTTP/1.1 chunks: a stream that breaks off is seen by the client
    as broken (no last chunk), not as a short, complete file."""

    def __init__(self, w: Any) -> None:
        self.w = w

    def write(self, b: bytes) -> int:
        if b:
            self.w.write(f"{len(b):x}\r\n".encode() + bytes(b) + b"\r\n")
        return len(b)

    def flush(self) -> None:
        self.w.flush()

    def end(self) -> None:
        self.w.write(b"0\r\n\r\n")


def _flag(q: dict[str, str], key: str) -> bool:
    return q.get(key, "").lower() in ("1", "true", "yes")


def handler(api: Api, token: str) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "zkm-fleet-runs/1"
        protocol_version = "HTTP/1.1"  # for the chunked fetch; every connection still serves one request
        streaming = False  # the headers of a streamed answer are out: an error can only end it

        def log_message(self, fmt: str, *args: Any) -> None:  # the actor log, one line per call
            print(f"{self.address_string()} {fmt % args}", flush=True)

        def _send(self, status: int, body: bytes, ctype: str = "application/json") -> None:
            self.send_response(status)
            self.send_header("content-type", ctype)
            self.send_header("content-length", str(len(body)))
            self.send_header("connection", "close")  # a body left unread cannot become the next request
            self.close_connection = True
            self.end_headers()
            self.wfile.write(body)

        def _json(self, status: int, value: Any) -> None:
            data = value.dump() if isinstance(value, Record) else [v.dump() for v in value] if isinstance(value, list) else value
            self._send(status, json.dumps(data, indent=2).encode())

        def _body(self) -> bytes:
            n = int(self.headers.get("content-length") or 0)
            if n > MAX_BODY:
                raise ApiError(413, f"body over {MAX_BODY >> 20} MiB")
            return self.rfile.read(n)

        def _json_body(self) -> dict[str, Any]:
            try:
                body = json.loads(self._body() or b"{}")
            except ValueError as e:
                raise ApiError(400, f"not JSON: {e}") from None
            if not isinstance(body, dict):
                raise ApiError(400, "a JSON object")
            return body

        def _authorized(self) -> bool:
            got = self.headers.get("authorization", "")
            return hmac.compare_digest(got.encode(), f"Bearer {token}".encode())

        def _dispatch(self, method: str) -> None:
            parts, q = _query(self.path)
            if parts == ["healthz"]:
                return self._send(200, b"ok", "text/plain")
            if not self._authorized():
                return self._send(401, b"missing or wrong admin token", "text/plain")
            try:
                self._route(method, parts, q)
            except Exception as e:
                if self.streaming:  # a broken stream: close it without its last chunk
                    print(f"{self.path}: stream broken off: {type(e).__name__}: {e}", flush=True)
                    self.close_connection = True
                    return
                self._error(e)

        def _error(self, e: BaseException) -> None:
            if isinstance(e, ApiError):
                self._send(e.status, str(e).encode(), "text/plain")
            elif isinstance(e, (runs.NotFound, runs.Conflict)):
                self._send(404 if isinstance(e, runs.NotFound) else 409, str(e.code).encode(), "text/plain")
            elif isinstance(e, (SystemExit, ModelError)):  # the same refusals run.sh prints
                self._send(400, str(e.code if isinstance(e, SystemExit) else e).encode(), "text/plain")
            else:  # report, keep serving
                self._send(500, f"{type(e).__name__}: {e}".encode(), "text/plain")

        def _route(self, method: str, parts: list[str], q: dict[str, str]) -> None:
            head, n = (parts[0] if parts else ""), len(parts)
            if (method, head, n) == ("POST", "runs", 1):
                return self._json(201, api.submit(self._json_body()))
            if (method, head, n) == ("GET", "apps", 1):
                return self._json(200, runs.apps(api.args(), runs.load_config(api.profiles)))
            if (method, head, n) == ("DELETE", "apps", 2):
                return self._json(200, api.delete_app(parts[1]))
            if (method, head, n) == ("GET", "runs", 1):
                return self._json(200, runs.list_runs(api.args(), _int(q, "n", 20) or 0, active=_flag(q, "active"),
                                                      profile=q.get("profile")))
            if (method, head, n) == ("DELETE", "runs", 1) and "older_than" in q:
                return self._json(200, runs.purge(api.args(older_than=_float(q, "older_than")), None))
            if (method, head, n) == ("GET", "runs", 2):
                return self._json(200, runs.status(api.args(), parts[1]))
            if (method, head, n) == ("DELETE", "runs", 2):
                return self._json(200, runs.purge(api.args(), parts[1]))
            if (method, head, n) == ("POST", "runs", 3) and parts[2] == "cancel":
                return self._json(200, runs.cancel(api.args(), parts[1]))
            if (method, head, n) == ("GET", "runs", 3):
                return self._logs_or_out(parts, q)
            if (method, head, n) == ("PUT", "uploads", 1):
                return self._json(201, api.upload(self._body()))
            if (method, head, n) == ("POST", "models", 1):
                return self._json(201, api.stage_model(self._json_body()))
            return self._missing()

        def _logs_or_out(self, parts: list[str], q: dict[str, str]) -> None:
            if parts[2] == "logs":
                chunk = runs.log_chunk(api.runs_dir, parts[1], _int(q, "rank", 0) or 0, _int(q, "offset", None),
                                       _int(q, "lines", 50) or 0)
                return self._json(200, chunk)
            if parts[2] == "out.tar.gz":
                runs.RunDir(api.runs_dir, parts[1])  # 404 before the headers go out
                self.send_response(200)
                self.send_header("content-type", "application/gzip")
                self.send_header("transfer-encoding", "chunked")
                self.send_header("connection", "close")
                self.close_connection = True
                self.end_headers()
                self.streaming = True
                body = _Chunked(self.wfile)
                runs.fetch(api.args(), parts[1], out=body)
                body.end()
                return None
            return self._missing()

        def _missing(self) -> None:
            raise ApiError(404, f"no such endpoint: {self.command} {self.path.split('?')[0]}")

        def do_GET(self) -> None:
            self._dispatch("GET")

        def do_POST(self) -> None:
            self._dispatch("POST")

        def do_PUT(self) -> None:
            self._dispatch("PUT")

        def do_DELETE(self) -> None:
            self._dispatch("DELETE")

    return Handler


class _RunsApi:
    """The runs API as a fleet service: serves on `listen` on the machine it was placed
    on, with that machine's profiles file and runs dir."""

    def __init__(self, spec: ReplicaSpec) -> None:
        self.spec, self.name = spec, spec.name
        svc = spec.service
        if not svc.profiles or not svc.runs_dir:
            raise RuntimeError(f"{self.name}: a runs-api service needs `profiles` and `runs_dir`")
        with open(spec.cluster.admin_token_path) as f:
            token = f.read().strip()
        host, _, port = svc.listen.rpartition(":")
        self.port = int(port)
        self.server = ThreadingHTTPServer((host or "127.0.0.1", self.port), handler(Api(svc.profiles, svc.runs_dir), token))
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        _watch(self.thread.is_alive, self.shutdown)

    def health(self) -> dict[str, Any]:
        if _stopping():
            return Health(name=self.name, stopping=True).dump()
        return Health(name=self.name, spec=spec_hash(self.spec), machine=socket.gethostname(), port=self.port,
                      ready=self.thread.is_alive()).dump()

    def shutdown(self) -> None:
        _STOPPING["on"] = True
        self.server.shutdown()
        self.server.server_close()


RunsApi = ray.remote(_RunsApi)


def serve(profiles: str, runs_dir: str, listen: str, token_file: str) -> None:
    """Runs the API in the foreground (a driver of its own), for testing without a service."""
    with open(token_file) as f:
        token = f.read().strip()
    host, _, port = listen.rpartition(":")
    server = ThreadingHTTPServer((host or "127.0.0.1", int(port)), handler(Api(profiles, runs_dir), token))
    print(f"runs API on {listen}", flush=True)
    server.serve_forever()

