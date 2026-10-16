#!/usr/bin/env python3
"""One-shot migration of stored state from batch "profiles" to batches.

  migrate-batches.py [-n] scheduler DATA_DIR      the scheduler's data_dir (holds workloads/)
  migrate-batches.py [-n] node PREFIX [JOBS_DIR] [--deleted-jobs FILE]
                                                  a node's install dir (etc/, deploy/; JOBS_DIR
                                                  defaults to PREFIX/work/jobs)

Deleted workloads are not migrated. The scheduler drops removed apps and batches, purged
jobs and the jobs of removed batches, and lists the dropped job ids in
DATA_DIR/workloads/deleted-jobs.txt. Given that file, a node sets those jobs' directories
aside under JOBS_DIR/.deleted-pre-batches/ (kept, never read again) instead of migrating them.

Run each with its service stopped and before starting the new binaries: the scheduler
refuses records that still say `profile`, the bridge refuses job specs that do, and the new
fleet-node refuses `--profiles`. -n prints what would change and changes nothing. Whatever
a run changes is copied to a `.pre-batches-<UTC time>` sibling first; a second run finds
nothing left to change.
"""
from __future__ import annotations

import json
import re
import shutil
import sys
import time
from pathlib import Path

STAMP = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
LAUNCH_JOB = 1


def renamed(record: dict, names: dict[str, str]) -> bool:
    """Renames keys in place; True when one was renamed."""
    found = [old for old in names if old in record and names[old] not in record]
    for old in found:
        record[names[old]] = record.pop(old)
    return bool(found)


def without_request_profile(record: dict) -> bool:
    """Old submissions kept the batch name as `profile` in the stored request."""
    request = record.get("request")
    return isinstance(request, dict) and request.pop("profile", None) is not None


def job(record: dict) -> bool:
    return renamed(record, {"profile": "batch"}) | without_request_profile(record)


def launch_parameters(command: dict) -> bool:
    if command.get("operation") != LAUNCH_JOB:
        return False
    parameters = command.get("parameters") or {}
    return renamed(parameters, {"profile_name": "batch", "profile": "policy"}) | without_request_profile(parameters)


class Migration:
    def __init__(self, dry: bool) -> None:
        self.dry, self.changes = dry, 0
        self.before_first_write = None  # taken once, just before anything is written

    def backup(self, path: Path) -> None:
        target = path.with_name(f"{path.name}.pre-batches-{STAMP}")
        (shutil.copytree if path.is_dir() else shutil.copy2)(path, target)

    def write(self, path: Path, text: str, what: str) -> None:
        self.changes += 1
        print(("would update" if self.dry else "updating"), path, f"({what})")
        if not self.dry:
            if self.before_first_write:
                self.before_first_write()
                self.before_first_write = None
            temporary = path.with_name(path.name + ".migrate-tmp")
            temporary.write_text(text)
            shutil.copymode(path, temporary)
            temporary.replace(path)

    def records(self, directory: Path, change, what: str, indent: int | None = None) -> None:
        for path in sorted(directory.glob("*.json")):
            record = json.loads(path.read_text())
            if change(record):
                self.write(path, json.dumps(record, indent=indent, separators=None if indent else (",", ":")), what)

    def drop(self, path: Path, what: str) -> None:
        self.changes += 1
        print(("would drop" if self.dry else "dropping"), path, f"({what})")
        if not self.dry:
            if self.before_first_write:
                self.before_first_write()
                self.before_first_write = None
            path.unlink()

    def scheduler(self, data_dir: Path) -> None:
        workloads = data_dir / "workloads"
        if not workloads.is_dir():
            sys.exit(f"{workloads} is not a scheduler workloads directory")
        self.before_first_write = lambda: self.backup(workloads)
        templates = {p: json.loads(p.read_text()) for p in sorted((workloads / "apps").glob("*.json"))}
        live = {t["name"] for t in templates.values() if not t.get("removed")}
        for path, template in templates.items():
            if template.get("removed"):
                self.drop(path, "deleted " + ("app" if template.get("persistent") else "batch"))
        deleted = []
        for path in sorted((workloads / "jobs").glob("*.json")):
            record = json.loads(path.read_text())
            batch = record.get("batch", record.get("profile"))
            if record.get("purged") or batch not in live:
                deleted.append(record["id"])
                self.drop(path, "purged job" if record.get("purged") else f"job of deleted batch {batch}")
        if deleted and not self.dry:
            (workloads / "deleted-jobs.txt").write_text("".join(f"{i}\n" for i in deleted))
        self.records(workloads / "apps", lambda r: renamed(r, {"profile": "spec"}), "profile -> spec")
        self.records(workloads / "jobs", job, "profile -> batch")
        self.records(workloads / "commands", launch_parameters, "launch parameters")

    def node(self, prefix: Path, jobs_dir: Path, deleted: set[str]) -> None:
        etc, old, new = prefix / "etc", prefix / "etc" / "jobs.yaml", prefix / "etc" / "batches.yaml"
        if old.exists() and not new.exists():
            text = old.read_text()
            self.changes += 1
            print(("would move" if self.dry else "moving"), old, "->", new, "(profiles: -> batches:)")
            if not self.dry:
                self.backup(old)
                new.write_text(re.sub(r"^profiles:", "batches:", text, count=1, flags=re.M))
                shutil.copymode(old, new)
                old.unlink()
        elif not new.exists():
            sys.exit(f"neither {old} nor {new} exists; is {etc} a node's etc?")
        start = prefix / "deploy" / "node-start.sh"
        if start.exists() and "--profiles" in (text := start.read_text()):
            if not self.dry:
                self.backup(start)
            self.write(start, text.replace('--profiles "$FLEET_ETC/jobs.yaml"', '--batches "$FLEET_ETC/batches.yaml"'),
                       "--profiles jobs.yaml -> --batches batches.yaml")
        aside = jobs_dir / ".deleted-pre-batches"
        for ident in sorted(deleted):
            if (jobs_dir / ident).is_dir():
                self.changes += 1
                print(("would set aside" if self.dry else "setting aside"), jobs_dir / ident, "(deleted job)")
                if not self.dry:
                    aside.mkdir(exist_ok=True)
                    (jobs_dir / ident).rename(aside / ident)
        for spec in sorted(jobs_dir.glob("*/spec.json")):
            record = json.loads(spec.read_text())
            if renamed(record, {"profile_name": "batch", "profile": "policy"}):
                if not self.dry:
                    self.backup(spec)
                self.write(spec, json.dumps(record, indent=2), "profile_name/profile -> batch/policy")
        journal = jobs_dir / ".node" / "commands"
        for path in sorted(journal.glob("*.json")):
            entry = json.loads(path.read_text())
            if launch_parameters(entry.get("command") or {}):
                self.write(path, json.dumps(entry), "launch parameters")


def main(argv: list[str]) -> None:
    dry = bool(argv) and argv[0] == "-n"
    args = argv[1:] if dry else argv
    migration = Migration(dry)
    if len(args) == 2 and args[0] == "scheduler":
        migration.scheduler(Path(args[1]))
    elif len(args) >= 2 and args[0] == "node":
        deleted: set[str] = set()
        if "--deleted-jobs" in args:
            at = args.index("--deleted-jobs")
            deleted = set(Path(args[at + 1]).read_text().split())
            args = args[:at] + args[at + 2:]
        if len(args) not in (2, 3):
            sys.exit(__doc__)
        prefix = Path(args[1])
        migration.node(prefix, Path(args[2]) if len(args) == 3 else prefix / "work" / "jobs", deleted)
    else:
        sys.exit(__doc__)
    print(f"{migration.changes} change(s){' to make' if dry else ''}")


if __name__ == "__main__":
    main(sys.argv[1:])
