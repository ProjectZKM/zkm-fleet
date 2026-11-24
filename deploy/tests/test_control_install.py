"""install.sh control and setup.sh, with stand-in docker, curl and fleetctl and no daemon."""
import hashlib
import io
import os
from pathlib import Path
import subprocess
import tarfile

import pytest

ROOT = Path(__file__).resolve().parents[2]
IMAGE = "ghcr.io/projectzkm/zkm-fleet-scheduler:9.9.9"


def executable(path, body):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("#!/bin/bash\nset -eu\n" + body)
    path.chmod(0o755)


def path_without(tmp_path, *names):
    """A PATH directory with the system's commands except NAMES (and docker, which is stubbed)."""
    directory = tmp_path / "system"
    directory.mkdir(exist_ok=True)
    for source in (Path("/usr/sbin"), Path("/usr/bin")):
        for entry in source.iterdir():
            target = directory / entry.name
            if entry.name not in (*names, "docker") and not target.exists():
                target.symlink_to(entry)
    return str(directory)


class Control:
    def __init__(self, tmp_path):
        self.tmp = tmp_path
        self.prefix = tmp_path / "fleet"
        self.etc = self.prefix / "etc"
        self.log = tmp_path / "docker.log"
        self.commands = tmp_path / "commands"
        binaries = tmp_path / "binaries"
        executable(binaries / "fleetctl", '''case "$1 $2" in
  "init token") echo cluster-token > "$4";;
  "init tls") mkdir -p "$4"; for f in ca.pem server.pem server.key; do echo "$f" > "$4/$f"; done;;
esac
''')
        executable(self.commands / "docker", f'''echo "$*" >> {self.log}
case "$*" in *issue-admin*) echo admin-token > {self.prefix}/data/admin.token;; esac
''')
        executable(self.commands / "curl", "exit 0\n")
        executable(self.commands / "sleep", "exit 0\n")
        self.args = ["control", "--public-url", "https://fleet.example:8905", "--tls-names", "stage,fleet.example",
                     "--prefix", str(self.prefix), "--etc", str(self.etc), "--bin", str(binaries),
                     "--ip", "192.0.2.10"]
        self.env = {k: v for k, v in os.environ.items() if not k.startswith(("FLEET_", "COMPOSE_"))}
        self.env.update(HOME=str(tmp_path / "home"), FLEET_SCHEDULER_IMAGE=IMAGE)

    def run(self, *extra, path=None):
        env = {**self.env, "PATH": f"{self.commands}:{path or os.environ['PATH']}"}
        return subprocess.run(["bash", str(ROOT / "deploy/install.sh"), *self.args, *extra],
                              env=env, capture_output=True, text=True, timeout=30)

    def compose_calls(self):
        if not self.log.exists():
            return []
        return [c for c in self.log.read_text().splitlines() if c.startswith("compose --env-file ")]

    def settings(self):
        return (self.prefix / "compose.env").read_text().splitlines()


@pytest.fixture
def control(tmp_path):
    return Control(tmp_path)


def test_control_install_needs_no_openshell_files_and_installs_fleet_ctl(control):
    result = control.run()
    assert result.returncode == 0, result.stderr
    assert not (control.etc / "openshell").exists()
    hint = [line for line in result.stdout.splitlines() if "openshell up" in line]
    assert hint == [f"[install] OpenShell gateway: off; run {control.prefix}/deploy/fleet-ctl.sh openshell up "
                    "once policy_gateway.openshell is configured"]
    ctl = control.prefix / "deploy/fleet-ctl.sh"
    assert ctl.read_bytes() == (ROOT / "deploy/fleet-ctl.sh").read_bytes() and os.access(ctl, os.X_OK)
    assert "COMPOSE_PROFILES=mcp" in control.settings()
    assert control.compose_calls()[0].endswith(f"-f {control.prefix}/deploy/docker-compose.yml up -d")
    password = control.etc / "pg_password"
    assert len(password.read_text().strip()) == 48 and password.stat().st_mode & 0o777 == 0o600
    assert not list(control.etc.glob("pg_password.*"))


@pytest.mark.parametrize("mcp", [True, False])
def test_reinstall_keeps_site_settings_and_profiles_it_does_not_own(control, mcp):
    assert control.run().returncode == 0
    env = control.prefix / "compose.env"
    site = ["# site settings", f"FLEET_OPENSHELL_IMAGE=ghcr.io/projectzkm/zkm-fleet-openshell@sha256:{'ab' * 32}",
            "FLEET_OPENSHELL_BIND=192.0.2.10", "FLEET_OPENSHELL_PORT=17671"]
    env.write_text(env.read_text().replace("COMPOSE_PROFILES=mcp", "COMPOSE_PROFILES=openshell,mcp,trustee")
                   + "\n".join(site) + "\n")
    env.chmod(0o640)
    control.env["FLEET_SCHEDULER_IMAGE"] = "ghcr.io/projectzkm/zkm-fleet-scheduler:9.9.10"
    result = control.run(*([] if mcp else ["--no-mcp"]))
    assert result.returncode == 0, result.stderr
    settings = control.settings()
    for line in site:
        assert line in settings
    profiles = [line for line in settings if line.startswith("COMPOSE_PROFILES=")]
    assert profiles == ["COMPOSE_PROFILES=openshell,trustee,mcp" if mcp else "COMPOSE_PROFILES=openshell,trustee"]
    assert [line for line in settings if line.startswith("FLEET_SCHEDULER_IMAGE=")] == [
        "FLEET_SCHEDULER_IMAGE=ghcr.io/projectzkm/zkm-fleet-scheduler:9.9.10"]
    assert sum(line.startswith("FLEET_ETC=") for line in settings) == 1
    assert env.stat().st_mode & 0o777 == 0o640
    assert "openshell up" not in result.stdout, "the gateway is already on"


def test_every_compose_call_applies_the_site_override(control):
    override = control.prefix / "deploy/docker-compose.override.yml"
    override.parent.mkdir(parents=True)
    override.write_text("services: {}\n")
    result = control.run()
    assert result.returncode == 0, result.stderr
    calls = control.compose_calls()
    assert calls[0].endswith(f"-f {override} up -d")
    assert any("issue-admin" in c for c in calls)
    release = f"-f {control.prefix}/deploy/docker-compose.yml -f {override} "
    for call in calls:
        assert release in call, call


@pytest.mark.parametrize("missing", ["openssl", "python3"])
def test_control_install_requires_openssl_and_python3_before_changing_anything(control, missing):
    result = control.run(path=path_without(control.tmp, missing))
    assert result.returncode != 0
    assert f"{missing} is required on the control host" in result.stderr
    assert not (control.prefix / "compose.env").exists()
    assert not (control.prefix / "deploy/docker-compose.yml").exists()
    assert not (control.etc / "pg_password").exists()


def test_a_failed_password_generation_leaves_no_password_file(control):
    executable(control.commands / "openssl", "exit 1\n")
    result = control.run()
    assert result.returncode != 0 and "could not generate the database password" in result.stderr
    assert not (control.etc / "pg_password").exists() and not list(control.etc.glob("pg_password.*"))
    assert not (control.etc / "database_url").exists()
    (control.commands / "openssl").unlink()
    result = control.run()
    assert result.returncode == 0, result.stderr
    password = (control.etc / "pg_password").read_text().strip()
    assert password and f"fleet:{password}@" in (control.etc / "database_url").read_text()


def test_an_empty_database_password_is_refused_with_instructions(control):
    control.etc.mkdir(parents=True)
    (control.etc / "pg_password").write_text("")
    result = control.run()
    assert result.returncode != 0
    assert f"{control.etc}/pg_password is empty" in result.stderr
    assert f"remove {control.etc}/pg_password and {control.etc}/database_url" in result.stderr
    assert not (control.prefix / "compose.env").exists() and control.compose_calls() == []


def release_server(tmp_path, version, install_body):
    """A fake GitHub release with a package whose install.sh runs INSTALL_BODY, and a curl that serves it."""
    top = f"zkm-fleet-{version}-linux-x86_64"
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
        for name, text, mode in [(f"{top}/deploy/install.sh", "#!/bin/bash\n" + install_body, 0o755),
                                 (f"{top}/bin/fleet-node", "#!/bin/bash\n", 0o755)]:
            data = text.encode()
            info = tarfile.TarInfo(name)
            info.size, info.mode = len(data), mode
            tar.addfile(info, io.BytesIO(data))
    releases = tmp_path / "releases"
    releases.mkdir()
    package = releases / f"{top}.tar.gz"
    package.write_bytes(buffer.getvalue())
    (releases / f"{top}.tar.gz.sha256").write_text(
        f"{hashlib.sha256(package.read_bytes()).hexdigest()}  {top}.tar.gz\n")
    commands = tmp_path / "commands"
    executable(commands / "curl", f'''out=; url=
while [ $# -gt 0 ]; do case "$1" in -o) out=$2; shift 2;; https://*) url=$1; shift;; *) shift;; esac; done
case "$url" in */releases/download/v{version}/*) cp "{releases}/${{url##*/}}" "$out";; *) exit 22;; esac
''')
    return commands


@pytest.mark.parametrize("keep, status", [(False, 0), (False, 3), (True, 0)])
def test_setup_removes_the_package_unless_kept_and_returns_the_installer_status(tmp_path, keep, status):
    calls = tmp_path / "install.args"
    commands = release_server(tmp_path, "9.9.9", f'printf "%s\\n" "$@" > {calls}\nexit {status}\n')
    tmpdir = tmp_path / "tmp"
    tmpdir.mkdir()
    env = {k: v for k, v in os.environ.items() if not k.startswith("FLEET_")}
    env.update(PATH=f"{commands}:{os.environ['PATH']}", TMPDIR=str(tmpdir))
    result = subprocess.run(["bash", str(ROOT / "deploy/setup.sh"), "--version", "v9.9.9",
                             *(["--keep-package"] if keep else []), "node", "--name", "gpu-1"],
                            env=env, capture_output=True, text=True, timeout=30)
    assert result.returncode == status, result.stderr
    assert calls.read_text().splitlines()[:3] == ["node", "--name", "gpu-1"]
    left = list(tmpdir.iterdir())
    if keep:
        assert len(left) == 1 and f"setup.sh: package kept in {left[0]}" in result.stdout
        assert (left[0] / "zkm-fleet-9.9.9-linux-x86_64.tar.gz").is_file()
    else:
        assert left == [], "the downloaded package was left behind"


def test_setup_needs_python3_to_resolve_the_latest_release(tmp_path):
    empty = tmp_path / "empty"
    empty.mkdir()
    result = subprocess.run(["/bin/bash", str(ROOT / "deploy/setup.sh"), "control"],
                            env={"PATH": str(empty), "HOME": str(tmp_path)}, capture_output=True, text=True, timeout=10)
    assert result.returncode == 1
    assert "python3 is required to resolve latest" in result.stderr
