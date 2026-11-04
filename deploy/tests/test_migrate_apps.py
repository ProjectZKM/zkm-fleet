"""Exercise the services-to-apps migration on scratch state, with a stand-in psql."""
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "deploy" / "migrate-apps.py"
NS = "5" * 32
ELF = "a" * 64


def migrate(*args, env=None):
    return subprocess.run([sys.executable, str(SCRIPT), *map(str, args)], capture_output=True, text=True, env=env)


def fake_psql(tmp_path, rows):
    """A psql that answers the proof app query with `rows` and logs every statement."""
    log = tmp_path / "psql.log"
    script = tmp_path / "psql"
    script.write_text(f"""#!{sys.executable}
import sys
query = sys.argv[-1]
open({str(log)!r}, "a").write(query + "\\n")
if "FROM fleet_apps" in query:
    print({rows!r})
elif query.startswith("SELECT id FROM fleet_namespace_allocations"):
    print("service:gpu-prover-gpu")
""")
    script.chmod(0o755)
    return script, log


def scheduler_state(tmp_path):
    workloads = tmp_path / "data" / "workloads"
    for name in ["apps", "commands", "services"]:
        (workloads / name).mkdir(parents=True)
    entry = {"name": "prover-gpu", "kind": "ziren-gpu", "namespace": NS, "app": "prover", "elf_id": ELF,
             "image": "ziren-gpu:1", "gpus": 2.0, "cpus": 16, "worker_command": ["/opt/w"], "ready": 1, "tag": "scope=a"}
    (workloads / "services" / "gpu-prover-gpu.json").write_text(json.dumps(
        {"worker": "gpu", "service": entry, "disabled": False, "scale": None}))
    commands = {"set": {"operation": 4, "parameters": {"service": entry, "disabled": False}},
                "entry": {"operation": 7, "parameters": {"service": {"name": "prover-gpu"}, "set": {"image": "x"}}},
                "batch": {"operation": 7, "parameters": {"batch": {"name": "b"}, "remove": True}},
                "launch": {"operation": 1, "parameters": {"service": "untouched"}}}
    for ident, command in commands.items():
        (workloads / "commands" / f"{ident}.json").write_text(json.dumps({"id": ident, "done": True, **command}))
    (tmp_path / "database_url").write_text("postgres://test\n")
    return workloads


def test_scheduler_records_move_and_take_the_new_names(tmp_path):
    workloads = scheduler_state(tmp_path)
    dry = migrate("-n", "scheduler", tmp_path / "data")
    assert dry.returncode == 0, dry.stderr
    assert "3 change(s) to make" in dry.stdout and (workloads / "services" / "gpu-prover-gpu.json").exists()
    assert migrate("scheduler", tmp_path / "data").returncode == 0
    record = json.loads((workloads / "deployments" / "gpu-prover-gpu.json").read_text())
    assert sorted(record) == ["disabled", "scale", "spec", "worker"] and record["spec"]["app"] == "prover"
    assert not (workloads / "services").exists()
    command = lambda ident: json.loads((workloads / "commands" / f"{ident}.json").read_text())["parameters"]
    assert command("set")["app"]["name"] == "prover-gpu" and "service" not in command("set")
    assert command("entry") == {"entry": {"name": "prover-gpu"}, "set": {"image": "x"}}
    assert command("batch") == {"batch": {"name": "b"}, "remove": True}
    assert command("launch") == {"service": "untouched"}
    backups = list(workloads.parent.glob("workloads.pre-app-model-*"))
    assert len(backups) == 1 and (backups[0] / "services" / "gpu-prover-gpu.json").exists()
    again = migrate("scheduler", tmp_path / "data")
    assert "0 change(s)" in again.stdout


def test_a_proof_app_gains_the_record_create_app_makes_and_reservations_are_renamed(tmp_path):
    workloads = scheduler_state(tmp_path)
    psql, log = fake_psql(tmp_path, f"{NS}\tprover\t{ELF}\t3\n{NS}\tidle\t{'b' * 64}\t0")
    env = {"PATH": "/usr/bin:/bin", "PSQL": str(psql)}
    result = migrate("scheduler", tmp_path / "data", tmp_path / "database_url", env=env)
    assert result.returncode == 0, result.stderr
    assert "skipping proof app idle: no node runs it" in result.stdout
    template = json.loads((workloads / "apps" / "prover.json").read_text())
    assert template == {"name": "prover", "namespace": NS, "tag": "scope=a", "persistent": True, "removed": False,
        "workers": ["gpu"], "spec": {"name": "prover", "kind": "ziren-gpu", "namespace": NS, "disabled": True,
        "priority": 3, "replicas": 1, "gpus": 2.0, "cpus": 16, "tag": "scope=a", "image": "ziren-gpu:1", "elf_id": ELF}}
    assert not (workloads / "apps" / "idle.json").exists()
    assert "UPDATE fleet_namespace_allocations SET id = 'deployment:' || substr(id, 9) WHERE id LIKE 'service:%'" in log.read_text()


def test_a_proof_app_keeps_a_name_another_record_holds(tmp_path):
    workloads = scheduler_state(tmp_path)
    batch = {"name": "prover", "namespace": NS, "tag": None, "spec": {}, "persistent": False, "removed": False, "workers": []}
    (workloads / "apps" / "prover.json").write_text(json.dumps(batch))
    psql, _ = fake_psql(tmp_path, f"{NS}\tprover\t{ELF}\t0")
    result = migrate("scheduler", tmp_path / "data", tmp_path / "database_url", env={"PATH": "/usr/bin:/bin", "PSQL": str(psql)})
    assert "skipping proof app prover: a batch of another record has its name" in result.stdout
    assert json.loads((workloads / "apps" / "prover.json").read_text()) == batch


def test_node_config_start_script_and_journal_take_the_new_names(tmp_path):
    prefix = tmp_path / "fleet"
    (prefix / "etc").mkdir(parents=True)
    (prefix / "deploy").mkdir()
    (prefix / "etc" / "services.yaml").write_text("# node\nscheduler: x\nservices:\n  - name: web # kept\n")
    (prefix / "deploy" / "node-start.sh").write_text(
        'fleet-node --services "$FLEET_ETC/services.yaml" --batches "$FLEET_ETC/batches.yaml"\n')
    journal = prefix / "work" / "jobs" / ".node" / "commands"
    journal.mkdir(parents=True)
    (journal / "c1.json").write_text(json.dumps(
        {"command": {"id": "c1", "operation": 4, "parameters": {"service": {"name": "web"}}}, "done": False}))
    assert migrate("-n", "node", prefix).returncode == 0
    assert (prefix / "etc" / "services.yaml").exists()
    result = migrate("node", prefix)
    assert result.returncode == 0, result.stderr
    assert (prefix / "etc" / "apps.yaml").read_text() == "# node\nscheduler: x\napps:\n  - name: web # kept\n"
    assert not (prefix / "etc" / "services.yaml").exists()
    assert '--apps "$FLEET_ETC/apps.yaml"' in (prefix / "deploy" / "node-start.sh").read_text()
    assert json.loads((journal / "c1.json").read_text())["command"]["parameters"] == {"app": {"name": "web"}}
    assert "0 change(s)" in migrate("node", prefix).stdout


def test_node_refuses_two_configurations(tmp_path):
    etc = tmp_path / "fleet" / "etc"
    etc.mkdir(parents=True)
    (etc / "services.yaml").write_text("services: []\n")
    (etc / "apps.yaml").write_text("apps: []\n")
    result = migrate("node", tmp_path / "fleet")
    assert result.returncode != 0 and "which one is current" in result.stderr
