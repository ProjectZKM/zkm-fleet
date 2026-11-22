#!/usr/bin/env python3
"""Fleet must execute precisely the evaluator feature profile its guest proves."""
import json
import subprocess

metadata = json.loads(subprocess.check_output([
    "cargo", "metadata", "--locked", "--all-features", "--format-version=1",
]))
packages = [p for p in metadata["packages"] if p["name"] == "regorus"]
assert len(packages) == 1 and packages[0]["version"] == "0.15.0", "unapproved evaluator version"
node = next(n for n in metadata["resolve"]["nodes"] if n["id"] == packages[0]["id"])
assert set(node["features"]) == {"no_std"}, f"unapproved Regorus features: {node['features']}"
print("Regorus deterministic feature profile verified")
