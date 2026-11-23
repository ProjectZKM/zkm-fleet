"""Run the release workflows' own scripts in a scratch tree, with stand-in tools."""
import os
from pathlib import Path
import re
import subprocess
import tarfile

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
WORKSPACE = ["fleet-mcp", "fleet-node", "fleet-proof-app", "fleet-proto", "fleet-scheduler",
             "fleet-ziren-node", "fleetctl"]


def workflow(name):
    data = yaml.safe_load((ROOT / ".github/workflows" / name).read_text())
    data["on"] = data.pop(True, data.get("on"))  # YAML 1.1 reads the key `on` as true
    return data


def step(name, job, title):
    return next(s for s in workflow(name)["jobs"][job]["steps"] if s.get("name") == title)


def bash(script, cwd, **env):
    environment = {**{k: v for k, v in os.environ.items() if not k.startswith(("FLEET_", "RELEASE_"))}, **env}
    return subprocess.run(["bash", "-eo", "pipefail", "-c", script], cwd=cwd, env=environment,
                          capture_output=True, text=True, timeout=60)


def executable(path, body):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("#!/bin/bash\n" + body)
    path.chmod(0o755)


def test_workspace_version_is_the_next_release():
    # No tomllib: CI also runs these tests on Python 3.10.
    manifest = (ROOT / "Cargo.toml").read_text()
    assert re.search(r'^\[workspace\.package\]\nversion = "([^"]+)"$', manifest, re.M)[1] == "0.2.4"
    lock = (ROOT / "Cargo.lock").read_text()
    for name in WORKSPACE:
        # Workspace crates are the entries without a source line.
        entries = re.findall(r'^\[\[package\]\]\nname = "%s"\nversion = "([^"]+)"\n(?!source)' % re.escape(name),
                             lock, re.M)
        assert entries == ["0.2.4"], name


@pytest.mark.parametrize("tag, ok", [("v0.2.4", True), ("v0.2.5", False), ("v0.2.4-rc.1", False)])
def test_release_requires_the_workspace_version_to_match_the_tag(tmp_path, tag, ok):
    # Another section's version comes first, so only [workspace.package] may count.
    (tmp_path / "Cargo.toml").write_text('[package]\nversion = "9.9.9"\n\n' + (ROOT / "Cargo.toml").read_text())
    check = step("release.yml", "build-release", "Require the workspace version to match the tag")
    result = bash(check["run"], tmp_path, RELEASE_TAG=tag, RELEASE_VERSION=tag[1:])
    assert (result.returncode == 0) == ok, result.stderr
    if not ok:
        assert f"workspace version is '0.2.4', but the tag {tag} needs {tag[1:]}" in result.stderr
    steps = [s.get("name") for s in workflow("release.yml")["jobs"]["build-release"]["steps"]]
    assert steps.index(check["name"]) < steps.index("Build release components"), "check before building"


def test_release_package_ships_docs_the_migrator_and_fleet_ctl(tmp_path):
    for name in ["deploy", "bridge", "docs", "README.md"]:
        (tmp_path / name).symlink_to(ROOT / name)
    for name in ["fleet-scheduler", "fleetctl", "fleet-mcp", "fleet-node", "fleet-proof-app"]:
        executable(tmp_path / "target/release" / name, "")
    for name in ["trusted-shim-runtime", "trusted-router-tunnel"]:
        executable(tmp_path / "target/shim/bin" / name, "")
    package = step("release.yml", "build-release", "Build release package")
    result = bash(package["run"], tmp_path, RELEASE_VERSION="9.9.9", TRUSTED_SHIM_REV="a" * 40,
                  TRUSTED_ROUTER_REV="b" * 40)
    assert result.returncode == 0, result.stderr
    top = "zkm-fleet-9.9.9-linux-x86_64"
    with tarfile.open(tmp_path / f"dist/{top}.tar.gz") as tar:
        names = set(tar.getnames())
    docs = sorted(p.name for p in (ROOT / "docs").glob("*.md"))
    assert "operations.md" in docs
    assert {f"{top}/docs/{d}" for d in docs} <= names
    assert f"{top}/README.md" in names
    assert not any(n.startswith(f"{top}/") and n.count("/") == 1 and n.endswith(".md") and not n.endswith("README.md")
                   for n in names), "the docs are under docs/, as README.md links them"
    for script in ["migrate-proof-runtime.py", "fleet-ctl.sh", "install.sh", "setup.sh"]:
        assert f"{top}/deploy/{script}" in names, script


@pytest.mark.parametrize("manifest, force, outcome", [
    ("present", "false", "skip=true"),
    ("present", "true", "skip=false"),
    ("absent", "false", "skip=false"),
    ("error", "false", None),
])
def test_openshell_image_tag_is_not_pushed_again_unless_forced(tmp_path, manifest, force, outcome):
    commands = tmp_path / "commands"
    answer = {"present": 'echo "{}"; exit 0',
              "absent": 'echo "no such manifest: $3" >&2; exit 1',
              "error": 'echo "unauthorized: authentication required" >&2; exit 1'}[manifest]
    executable(commands / "docker", f'echo "$*" >> {tmp_path}/docker.log\n'
                                    f'case "$1 $2" in "manifest inspect") {answer};; esac\n')
    output = tmp_path / "output"
    check = step("openshell-image.yml", "image", "Skip a revision that is already published")
    result = bash(check["run"], tmp_path, PATH=f"{commands}:{os.environ['PATH']}", FORCE=force,
                  GITHUB_OUTPUT=str(output), GITHUB_REPOSITORY="ProjectZKM/zkm-fleet", GITHUB_ACTOR="ci",
                  REGISTRY_TOKEN="token", OPENSHELL_REV="c" * 40)
    if outcome is None:
        assert result.returncode != 0 and "cannot tell whether" in result.stderr
        return
    assert result.returncode == 0, result.stderr
    assert output.read_text().splitlines() == [outcome]
    assert f"manifest inspect ghcr.io/projectzkm/zkm-fleet-openshell:{'c' * 40}" in (tmp_path / "docker.log").read_text()


def test_openshell_image_build_and_push_wait_for_the_published_check():
    data = workflow("openshell-image.yml")
    force = data["on"]["workflow_dispatch"]["inputs"]["force"]
    assert force["type"] == "boolean" and force["default"] is False
    steps = data["jobs"]["image"]["steps"]
    check = next(i for i, s in enumerate(steps) if s.get("id") == "published")
    assert "inputs.force" in steps[check]["env"]["FORCE"]
    for later in steps[check + 1:]:
        assert later.get("if") == "steps.published.outputs.skip != 'true'", later
    assert any("docker push" in s.get("run", "") for s in steps[check + 1:])
