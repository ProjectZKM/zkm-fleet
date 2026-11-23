#!/usr/bin/env python3
"""Audit the complete consumer graph with its pinned Rail evaluator's checks."""
import json
from pathlib import Path
import runpy
import subprocess
import sys

manifest = sys.argv[1] if len(sys.argv) > 1 else "Cargo.toml"
metadata = json.loads(subprocess.check_output([
    "cargo", "metadata", "--locked", "--all-features", "--format-version=1",
    "--manifest-path", manifest,
]))
rails = [p for p in metadata["packages"] if p["name"] == "trusted-rail"]
if len(rails) != 1:
    raise SystemExit("exactly one trusted-rail evaluator is required")
audit = Path(rails[0]["manifest_path"]).parent / "scripts/check_engine_features.py"
runpy.run_path(str(audit))["check"](metadata)
print("Pinned Rail evaluator source and feature profiles verified")
