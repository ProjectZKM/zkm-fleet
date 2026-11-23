"""fleet-ctl.sh's control-host commands, with stand-in docker, curl and release downloads."""
import hashlib
import io
import os
from pathlib import Path
import tarfile

import pytest

ROOT = Path(__file__).resolve().parents[2]
REPO = "ghcr.io/projectzkm/zkm-fleet-openshell"
DIGEST = "sha256:" + "ab" * 32
KEYS = ["server.key", "jwt.key"]
OPENSHELL_FILES = ["gateway.toml", "server.pem", "server.key", "ca.pem", "jwt.key", "jwt.pub", "jwt.kid"]
POLICY = """control_addr: 0.0.0.0:51001
policy_gateway:
  program_elf: /etc/zkm-fleet/policy.elf
  openshell:
    compute_socket: /run/zkm-fleet/openshell/compute.sock
    admission_policy: test
"""


def executable(path, body):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("#!/bin/bash\n" + body)
    path.chmod(0o755)


class Control:
    """A control prefix as install.sh leaves it, and fleet-ctl.sh run against it."""

    def __init__(self, tmp_path):
        self.tmp = tmp_path
        self.prefix = tmp_path / "fleet"
        self.etc = self.prefix / "etc"
        self.log = tmp_path / "docker.log"
        self.schema = tmp_path / "schema"  # what psql reports; absent means psql fails
        self.schema.write_text("20\n")
        self.releases = tmp_path / "releases"
        commands = tmp_path / "commands"
        (self.prefix / "deploy").mkdir(parents=True)
        (self.prefix / "deploy/docker-compose.yml").write_text("services: {}\n")
        (self.prefix / "deploy/gateway.yaml.in").write_text("old template @SCHEDULER_IP@\n")
        executable(self.prefix / "bin/fleetctl", "echo old-fleetctl\n")
        self.etc.mkdir(mode=0o750)
        (self.etc / "fleet.yaml").write_text(POLICY)
        (self.etc / "gateway.yaml").write_text("old template 172.28.250.10\n")
        self.compose_env = self.prefix / "compose.env"
        self.compose_env.write_text(
            f"FLEET_ETC={self.etc}\nFLEET_UID={os.getuid()}\nFLEET_GID={os.getgid()}\n"
            "FLEET_SCHEDULER_IP=172.28.250.10\nCOMPOSE_PROFILES=mcp\n"
            "FLEET_SCHEDULER_IMAGE=ghcr.io/projectzkm/zkm-fleet-scheduler:0.2.2\n"
            "FLEET_OPENSHELL_BIND=192.0.2.1\n")
        openshell = self.etc / "openshell"
        openshell.mkdir(mode=0o700)
        for name in OPENSHELL_FILES:
            (openshell / name).write_text(f"{name}\n")
            (openshell / name).chmod(0o600 if name in KEYS else 0o644)
        self.openshell = openshell
        self.image = f"{REPO}:ce58cf8e"
        # docker: logs every call; FAKE_FAIL names a substring of the one call that fails.
        executable(commands / "docker", f'''echo "$*" >> {self.log}
if [ -n "${{FAKE_FAIL:-}}" ] && [[ "$*" == *"$FAKE_FAIL"* ]]; then exit 1; fi
case "$*" in
  *" config --format json openshell-gateway")  # interpolated from compose.env, as Compose does
    image=$(sed -n 's/^FLEET_OPENSHELL_IMAGE=//p' {self.compose_env})
    printf '{{"services":{{"openshell-gateway":{{"image":"%s"}}}}}}\\n' "${{image:-$FAKE_OPENSHELL_IMAGE}}" ;;
  "image inspect "*) printf 'other.example/mirror@sha256:{"cd" * 32}\\n{REPO}@{DIGEST}\\n' ;;
  *" exec -T postgres psql "*) cat {self.schema} ;;
  *" ps -aq scheduler") echo abc123 ;;
  "inspect -f "*) echo "running 0" ;;
esac
''')
        # curl: healthz answers; release downloads come from self.releases.
        executable(commands / "curl", f'''out=; url=
while [ $# -gt 0 ]; do case "$1" in -o) out=$2; shift 2;; https://*) url=$1; shift;; *) shift;; esac; done
case "$url" in */releases/download/*) exec cp "{self.releases}/${{url##*/}}" "$out";; esac
''')
        executable(commands / "sleep", "")
        self.env = {k: v for k, v in os.environ.items() if not k.startswith(("FLEET_", "COMPOSE_"))}
        self.env.update(PATH=f"{commands}:{os.environ['PATH']}", FLEET_PREFIX=str(self.prefix),
                        FLEET_CTL_ENV=str(tmp_path / "none.env"), FLEET_NODE_SSH="", TMPDIR=str(tmp_path),
                        FAKE_OPENSHELL_IMAGE=self.image)

    def run(self, *args, **env):
        import subprocess
        return subprocess.run(["bash", str(ROOT / "deploy/fleet-ctl.sh"), *args], env={**self.env, **env},
                              capture_output=True, text=True, timeout=60)

    def calls(self):
        return self.log.read_text().splitlines() if self.log.exists() else []

    def release(self, version, template):
        """Publish a fake release package with this gateway.yaml.in."""
        top = f"zkm-fleet-{version}-linux-x86_64"
        files = {f"{top}/bin/fleetctl": "#!/bin/bash\necho new-fleetctl\n",
                 f"{top}/deploy/docker-compose.yml": "services: {} # new\n",
                 f"{top}/deploy/gateway.yaml.in": template,
                 f"{top}/deploy/trustee/as.json": "{}\n",
                 f"{top}/VERSION": f"version={version}\n"}
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
            for name, text in files.items():
                data = text.encode()
                info = tarfile.TarInfo(name)
                info.size, info.mode = len(data), 0o755
                tar.addfile(info, io.BytesIO(data))
        self.releases.mkdir(exist_ok=True)
        package = self.releases / f"{top}.tar.gz"
        package.write_bytes(buffer.getvalue())
        digest = hashlib.sha256(package.read_bytes()).hexdigest()
        (self.releases / f"{top}.tar.gz.sha256").write_text(f"{digest}  dist/{top}.tar.gz\n")


@pytest.fixture
def control(tmp_path):
    return Control(tmp_path)


def compose(control, *rest):
    return " ".join(["compose", "--env-file", str(control.compose_env), "-p", "zkm-fleet",
                     "-f", str(control.prefix / "deploy/docker-compose.yml"), *rest])


def test_openshell_up_checks_pins_prepares_and_starts_in_order(control):
    result = control.run("openshell", "up")
    assert result.returncode == 0, result.stderr
    uid, gid = os.getuid(), os.getgid()
    calls = [c for c in control.calls() if " ps -aq " not in c and not c.startswith("inspect -f")]
    assert calls == [
        compose(control, "--profile openshell config --format json openshell-gateway"),
        f"pull {control.image}",
        f"image inspect --format {{{{range .RepoDigests}}}}{{{{println .}}}}{{{{end}}}} {control.image}",
        compose(control, "--profile openshell run --rm --no-deps --user 0:0 --cap-add CHOWN --cap-add FOWNER",
                "--cap-add DAC_OVERRIDE --entrypoint install openshell-gateway -d -m 700",
                f"-o {uid} -g {gid} /run/zkm-fleet/openshell /var/lib/openshell"),
        compose(control, "up -d --no-deps --force-recreate scheduler"),
        compose(control, "--profile openshell up -d --no-deps --wait openshell-gateway"),
    ]
    settings = control.compose_env.read_text().splitlines()
    assert f"FLEET_OPENSHELL_IMAGE={REPO}@{DIGEST}" in settings
    assert "COMPOSE_PROFILES=mcp,openshell" in settings
    assert "FLEET_OPENSHELL_BIND=192.0.2.1" in settings
    # Running it again changes nothing in compose.env: the image is pinned and the profile is on.
    before = control.compose_env.read_text()
    assert control.run("openshell", "up").returncode == 0
    assert control.compose_env.read_text() == before


def test_openshell_up_dry_run_prints_the_steps_and_changes_nothing(control):
    before = control.compose_env.read_text()
    result = control.run("-n", "openshell", "up")
    assert result.returncode == 0, result.stderr
    assert control.compose_env.read_text() == before
    assert [c.split(" --profile openshell ")[-1] for c in control.calls()] == ["config --format json openshell-gateway"]
    steps = [f"+ docker pull {control.image}",
             f"+ set FLEET_OPENSHELL_IMAGE={REPO}@sha256:<registry digest of {control.image}>",
             "run --rm --no-deps --user 0:0", "up -d --no-deps --force-recreate scheduler",
             "--profile openshell up -d --no-deps --wait openshell-gateway", "+ set COMPOSE_PROFILES=mcp,openshell"]
    positions = [result.stdout.find(step) for step in steps]
    assert -1 not in positions and positions == sorted(positions), result.stdout


@pytest.mark.parametrize("damage, message", [
    (lambda c: (c.openshell / "jwt.kid").unlink(), "jwt.kid is missing or empty"),
    (lambda c: (c.openshell / "ca.pem").write_text(""), "ca.pem is missing or empty"),
    (lambda c: (c.openshell / "server.key").chmod(0o640), "server.key is a private key and must be mode 0600"),
    (lambda c: (c.openshell / "jwt.key").chmod(0o644), "jwt.key is a private key and must be mode 0600"),
    (lambda c: c.openshell.chmod(0o750), "openshell must be mode 0700"),
    (lambda c: c.compose_env.write_text(c.compose_env.read_text().replace(
        f"FLEET_UID={os.getuid()}", f"FLEET_UID={os.getuid() + 1}")), "gateway.toml must be owned by"),
])
def test_openshell_up_refuses_unprepared_files_before_touching_anything(control, damage, message):
    damage(control)
    before = control.compose_env.read_text()
    result = control.run("openshell", "up")
    assert result.returncode != 0
    assert message in result.stderr and "nothing changed" in result.stderr, result.stderr
    assert control.calls() == [] and control.compose_env.read_text() == before


@pytest.mark.parametrize("fleet_yaml", [
    "control_addr: 0.0.0.0:51001\n",
    "policy_gateway:\n  program_elf: /x\n  # openshell:\n  #   compute_socket: /s\n",
    "policy_gateway:\n  openshell:\n  program_elf: /x\n",
])
def test_openshell_up_refuses_without_policy_gateway_openshell(control, fleet_yaml):
    (control.etc / "fleet.yaml").write_text(fleet_yaml)
    result = control.run("openshell", "up")
    assert result.returncode != 0
    assert "has no policy_gateway.openshell block" in result.stderr, result.stderr
    assert control.calls() == []


def test_openshell_up_keeps_an_image_pinned_by_digest(control):
    pinned = f"{REPO}@sha256:" + "ef" * 32
    result = control.run("openshell", "up", FAKE_OPENSHELL_IMAGE=pinned)
    assert result.returncode == 0, result.stderr
    assert not any(c.startswith(("pull ", "image inspect ")) for c in control.calls())
    assert "FLEET_OPENSHELL_IMAGE" not in control.compose_env.read_text()


def test_openshell_up_leaves_the_profile_off_when_the_gateway_fails(control):
    result = control.run("openshell", "up", FAKE_FAIL="--wait openshell-gateway")
    assert result.returncode != 0
    assert "OpenShell gateway is not healthy" in result.stderr
    assert "COMPOSE_PROFILES=mcp\n" in control.compose_env.read_text()


def test_upgrade_rerenders_the_live_gateway_config_and_rollback_restores_it(control):
    control.release("9.9.9", "new template @SCHEDULER_IP@ with ledger route\n")
    live = control.etc / "gateway.yaml"
    live.chmod(0o640)
    result = control.run("scheduler", "upgrade", "v9.9.9")
    assert result.returncode == 0, result.stderr
    assert live.read_text() == "new template 172.28.250.10 with ledger route\n"
    assert live.stat().st_mode & 0o777 == 0o640
    backup = next((control.prefix / "backup").glob("control-*"))
    assert (backup / "gateway.yaml").read_text() == "old template 172.28.250.10\n"
    assert (backup / "schema_version").read_text() == "20\n"
    ups = [c for c in control.calls() if " up " in c]
    assert ups == [compose(control, "up -d --no-deps --force-recreate scheduler mcp"),
                   compose(control, "up -d --no-deps --force-recreate gateway")]

    # Site settings added after the upgrade survive the rollback; only the image goes back.
    with control.compose_env.open("a") as settings:
        settings.write(f"FLEET_OPENSHELL_IMAGE={REPO}@{DIGEST}\n")
    control.log.unlink()
    result = control.run("scheduler", "rollback")
    assert result.returncode == 0, result.stderr
    assert live.read_text() == "old template 172.28.250.10\n"
    assert [c for c in control.calls() if " up " in c] == [
        compose(control, "up -d --no-deps --force-recreate scheduler mcp gateway")]
    settings = control.compose_env.read_text().splitlines()
    assert "FLEET_SCHEDULER_IMAGE=ghcr.io/projectzkm/zkm-fleet-scheduler:0.2.2" in settings
    assert f"FLEET_OPENSHELL_IMAGE={REPO}@{DIGEST}" in settings
    assert (control.prefix / "bin/fleetctl").read_text().endswith("echo old-fleetctl\n")


def test_upgrade_leaves_envoy_alone_when_its_config_is_unchanged(control):
    control.release("9.9.9", "old template @SCHEDULER_IP@\n")
    result = control.run("scheduler", "upgrade", "v9.9.9")
    assert result.returncode == 0, result.stderr
    assert "gateway config unchanged" in result.stdout
    assert not any(c.endswith(" gateway") for c in control.calls())
    control.log.unlink()
    assert control.run("scheduler", "rollback").returncode == 0
    assert [c for c in control.calls() if " up " in c] == [
        compose(control, "up -d --no-deps --force-recreate scheduler mcp")]


def test_upgrade_leaves_a_site_managed_gateway_config_alone(control):
    (control.etc / "gateway.yaml").unlink()
    control.release("9.9.9", "new template @SCHEDULER_IP@\n")
    result = control.run("scheduler", "upgrade", "v9.9.9")
    assert result.returncode == 0, result.stderr
    assert "gateway's config is site-managed" in result.stdout
    assert not (control.etc / "gateway.yaml").exists()
    assert not any(c.endswith(" gateway") for c in control.calls())


def test_a_failed_upgrade_keeps_the_old_gateway_config(control):
    control.release("9.9.9", "new template @SCHEDULER_IP@\n")
    executable(control.tmp / "commands/curl", f'''out=; url=
while [ $# -gt 0 ]; do case "$1" in -o) out=$2; shift 2;; https://*) url=$1; shift;; *) shift;; esac; done
case "$url" in */releases/download/*) exec cp "{control.releases}/${{url##*/}}" "$out";; esac
exit 1  # the new scheduler never answers
''')
    result = control.run("scheduler", "upgrade", "v9.9.9", FLEET_CTL_HEALTH_WAIT="0")
    assert result.returncode != 0 and "scheduler rollback" in result.stderr
    assert (control.etc / "gateway.yaml").read_text() == "old template 172.28.250.10\n"


@pytest.mark.parametrize("now, recorded, reason", [
    ("21\n", "20\n", "the database is at schema 21, but was at schema 20 before the upgrade"),
    ("20\n", None, "records no database schema version"),
    (None, "20\n", "cannot read the database schema version"),
])
def test_rollback_refuses_unless_the_schema_is_unchanged_or_forced(control, now, recorded, reason):
    backup = control.prefix / "backup/control-20260101T000000Z"
    for name in ["docker-compose.yml", "gateway.yaml.in", "compose.env"]:
        (backup / name).parent.mkdir(parents=True, exist_ok=True)
        (backup / name).write_text((control.prefix / "deploy" / name).read_text()
                                   if name != "compose.env" else control.compose_env.read_text())
    executable(backup / "fleetctl", "echo old-fleetctl\n")
    executable(control.prefix / "bin/fleetctl", "echo new-fleetctl\n")
    if recorded is not None:
        (backup / "schema_version").write_text(recorded)
    if now is None:
        control.schema.unlink()
    else:
        control.schema.write_text(now)
    result = control.run("scheduler", "rollback")
    assert result.returncode != 0
    assert reason in result.stderr and "Restore the Postgres and data_dir backups" in result.stderr, result.stderr
    assert "--force" in result.stderr
    assert (control.prefix / "bin/fleetctl").read_text().endswith("echo new-fleetctl\n"), "nothing was restored"
    assert not any(" up " in c for c in control.calls())

    result = control.run("scheduler", "rollback", "--force")
    assert result.returncode == 0, result.stderr
    assert reason in result.stderr and "rolling back anyway (--force)" in result.stderr
    assert (control.prefix / "bin/fleetctl").read_text().endswith("echo old-fleetctl\n")


def test_upgrade_warns_when_it_cannot_record_the_schema(control):
    control.release("9.9.9", "old template @SCHEDULER_IP@\n")
    control.schema.unlink()
    result = control.run("scheduler", "upgrade", "v9.9.9")
    assert result.returncode == 0, result.stderr
    assert "a rollback to" in result.stderr and "will need --force" in result.stderr
    backup = next((control.prefix / "backup").glob("control-*"))
    assert not (backup / "schema_version").exists()


def test_rollback_rejects_unknown_arguments(control):
    result = control.run("scheduler", "rollback", "--forse")
    assert result.returncode != 0 and "usage: scheduler rollback [--force]" in result.stderr
