#!/usr/bin/env python3
"""One-shot migration of stored state from services to apps (node report version 3).

  migrate-apps.py [-n] scheduler DATA_DIR [DATABASE_URL_FILE]
                                     the scheduler's data_dir (holds workloads/) and, for a
                                     Postgres store, the file holding its URL
  migrate-apps.py [-n] node PREFIX [JOBS_DIR]
                                     a node's install dir (etc/, deploy/; JOBS_DIR defaults
                                     to PREFIX/work/jobs)

The scheduler's per-node records move from workloads/services to workloads/deployments,
their `service` becoming `spec`. Stored commands and a node's command journal take the new
parameter names: SET_APP's `service` becomes `app`, and APP's `service` becomes `entry`.
Reservations named `service:KEY` become `deployment:KEY`. Each registered proof app gains
the app record `POST /apps` creates: its container settings, taken from the entry a node
runs for it, and its program. A proof app no node runs keeps no settings; delete it, or
give it some when it is next created. A node's etc/services.yaml becomes etc/apps.yaml,
its `services:` list `apps:`, and node-start.sh passes it as `--apps`.

The database is reached with `psql`, or the command $PSQL names (on a host without one:
PSQL="docker run --rm -i --network host postgres:16 psql").

Run each with its service stopped and before starting the new binaries: the scheduler reads
only workloads/deployments and releases only `deployment:` reservations, and the new
fleet-node refuses `--services`. Let in-flight commands finish first. -n prints what would
change and changes nothing. Whatever a run changes is copied to a `.pre-app-model-<UTC
time>` sibling first; a second run finds nothing left to change.
"""
from __future__ import annotations

import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import time
from pathlib import Path

STAMP = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
SET_APP, APP = 4, 7
# The settings `POST /apps` takes in an app's spec (RegisterAppRequest in models.rs).
SPEC_SETTINGS = ["kind", "image", "replicas", "gpus", "cpus", "shm_gb", "memory_gb", "env", "volumes",
                 "ready_timeout_s", "stop_timeout_s", "command", "port_base", "health_path", "workdir", "user",
                 "proof_storage_root", "backend", "rust_log"]


def renamed(record: dict, names: dict[str, str]) -> bool:
    """Renames keys in place; True when one was renamed."""
    found = [old for old in names if old in record and names[old] not in record]
    for old in found:
        record[names[old]] = record.pop(old)
    return bool(found)


def command_parameters(command: dict) -> bool:
    parameters = command.get("parameters")
    if not isinstance(parameters, dict):
        return False
    if command.get("operation") == SET_APP:
        return renamed(parameters, {"service": "app"})
    if command.get("operation") == APP:
        return renamed(parameters, {"service": "entry"})
    return False


def psql(url: str, query: str) -> list[list[str]]:
    """Runs one statement; its rows, tab-separated fields."""
    out = subprocess.run([*shlex.split(os.environ.get("PSQL", "psql")), url, "-X", "-At", "-F", "\t", "-v", "ON_ERROR_STOP=1", "-c", query],
                         check=True, capture_output=True, text=True).stdout
    return [line.split("\t") for line in out.splitlines() if line]


class Migration:
    def __init__(self, dry: bool) -> None:
        self.dry, self.changes = dry, 0
        self.before_first_write = None  # taken once, just before anything is written

    def backup(self, path: Path) -> None:
        target = path.with_name(f"{path.name}.pre-app-model-{STAMP}")
        (shutil.copytree if path.is_dir() else shutil.copy2)(path, target)

    def first_write(self) -> None:
        if self.before_first_write:
            self.before_first_write()
            self.before_first_write = None

    def write(self, path: Path, text: str, what: str) -> None:
        self.changes += 1
        print(("would write" if self.dry else "writing"), path, f"({what})")
        if not self.dry:
            self.first_write()
            temporary = path.with_name(path.name + ".migrate-tmp")
            temporary.write_text(text)
            if path.exists():
                shutil.copymode(path, temporary)
            temporary.replace(path)

    def records(self, directory: Path, change, what: str) -> None:
        for path in sorted(directory.glob("*.json")):
            record = json.loads(path.read_text())
            if change(record):
                self.write(path, json.dumps(record), what)

    def scheduler(self, data_dir: Path, database: str | None) -> None:
        workloads = data_dir / "workloads"
        if not workloads.is_dir():
            sys.exit(f"{workloads} is not a scheduler workloads directory")
        self.before_first_write = lambda: self.backup(workloads)
        old, new = workloads / "services", workloads / "deployments"
        deployments = {}
        for path in sorted(old.glob("*.json")) if old.is_dir() else []:
            record = json.loads(path.read_text())
            renamed(record, {"service": "spec"})
            deployments[path.stem] = record
            if (new / path.name).exists():
                sys.exit(f"{new / path.name} exists as well as {path}; which one is current?")
            self.changes += 1
            print(("would move" if self.dry else "moving"), path, "->", new / path.name, "(service -> spec)")
            if not self.dry:
                self.first_write()
                new.mkdir(exist_ok=True)
                (new / path.name).write_text(json.dumps(record))
                path.unlink()
        if old.is_dir() and not self.dry and not any(old.iterdir()):
            old.rmdir()
        for path in sorted(new.glob("*.json")) if new.is_dir() else []:
            deployments.setdefault(path.stem, json.loads(path.read_text()))
        self.records(workloads / "commands", command_parameters, "parameter names")
        if database is None:
            print("no database given: proof app records and reservations left as they are")
            return
        self.proof_apps(workloads / "apps", deployments, database)
        self.reservations(database)

    def proof_apps(self, templates: Path, deployments: dict[str, dict], database: str) -> None:
        """Gives each registered proof app the record `create_app` makes for it."""
        rows = psql(database, "SELECT namespace, name, elf_id, priority FROM fleet_apps ORDER BY namespace, name")
        for namespace, name, elf_id, priority in rows:
            path = templates / f"{name}.json"
            if path.exists():
                existing = json.loads(path.read_text())
                if not existing.get("removed"):
                    if existing.get("persistent") and existing.get("namespace") == namespace:
                        continue
                    print(f"skipping proof app {name}: a {'batch' if not existing.get('persistent') else 'app'} "
                          f"of another record has its name")
                    continue
            runs = sorted((key, d) for key, d in deployments.items()
                          if d["spec"].get("app") == name and d["spec"].get("namespace") == namespace)
            if not runs:
                print(f"skipping proof app {name}: no node runs it, so it has no container settings")
                continue
            entry = runs[0][1]["spec"]
            spec = {"name": name, "kind": entry.get("kind", "ziren-gpu"), "namespace": namespace, "disabled": True,
                    "priority": int(priority), "replicas": 1, "gpus": 1.0, "cpus": 1}
            if entry.get("tag") is not None:
                spec["tag"] = entry["tag"]
            spec.update({k: entry[k] for k in SPEC_SETTINGS if entry.get(k) is not None})
            spec["elf_id"] = elf_id
            workers = sorted({d["worker"] for _, d in runs})
            record = {"name": name, "namespace": namespace, "tag": entry.get("tag"), "spec": spec,
                      "persistent": True, "removed": False, "workers": workers}
            self.write(path, json.dumps(record), f"proof app {name}: settings of {runs[0][0]}")

    def reservations(self, database: str) -> None:
        rows = psql(database, "SELECT id FROM fleet_namespace_allocations WHERE id LIKE 'service:%' ORDER BY id")
        for (ident,) in rows:
            self.changes += 1
            print(("would rename" if self.dry else "renaming"), "reservation", ident, "->", "deployment:" + ident[len("service:"):])
        if rows and not self.dry:
            self.first_write()
            psql(database, "UPDATE fleet_namespace_allocations SET id = 'deployment:' || substr(id, 9) WHERE id LIKE 'service:%'")

    def node(self, prefix: Path, jobs_dir: Path) -> None:
        etc, old, new = prefix / "etc", prefix / "etc" / "services.yaml", prefix / "etc" / "apps.yaml"
        if old.exists() and not new.exists():
            text = old.read_text()
            self.changes += 1
            print(("would move" if self.dry else "moving"), old, "->", new, "(services: -> apps:)")
            if not self.dry:
                self.backup(old)
                new.write_text(re.sub(r"^services:", "apps:", text, count=1, flags=re.M))
                shutil.copymode(old, new)
                old.unlink()
        elif old.exists():
            sys.exit(f"both {old} and {new} exist; which one is current?")
        elif not new.exists():
            sys.exit(f"neither {old} nor {new} exists; is {etc} a node's etc?")
        start = prefix / "deploy" / "node-start.sh"
        if start.exists() and "--services" in (text := start.read_text()):
            if not self.dry:
                self.backup(start)
            self.write(start, text.replace('--services "$FLEET_ETC/services.yaml"', '--apps "$FLEET_ETC/apps.yaml"'),
                       "--services services.yaml -> --apps apps.yaml")
        journal = jobs_dir / ".node" / "commands"
        self.before_first_write = lambda: self.backup(journal)
        for path in sorted(journal.glob("*.json")):
            entry = json.loads(path.read_text())
            if command_parameters(entry.get("command") or {}):
                self.write(path, json.dumps(entry), "parameter names")


def main(argv: list[str]) -> None:
    dry = bool(argv) and argv[0] == "-n"
    args = argv[1:] if dry else argv
    migration = Migration(dry)
    if len(args) in (2, 3) and args[0] == "scheduler":
        database = Path(args[2]).read_text().strip() if len(args) == 3 else None
        migration.scheduler(Path(args[1]), database)
    elif len(args) in (2, 3) and args[0] == "node":
        prefix = Path(args[1])
        migration.node(prefix, Path(args[2]) if len(args) == 3 else prefix / "work" / "jobs")
    else:
        sys.exit(__doc__)
    print(f"{migration.changes} change(s){' to make' if dry else ''}")


if __name__ == "__main__":
    main(sys.argv[1:])
