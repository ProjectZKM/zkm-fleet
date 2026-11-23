"""Capture installer commands without invoking host sudo or systemd."""
import os
from pathlib import Path
import shutil
import sys

root = Path(__file__).resolve().parent.parent
capture = root / "units"
with (root / "calls").open("a") as log:
    log.write(" ".join(sys.argv[1:]) + "\n")

if sys.argv[1] == "test":
    sys.exit(0 if (capture / Path(sys.argv[-1]).name).exists() else 1)
if sys.argv[1] == "cat":
    sys.stdout.write((capture / Path(sys.argv[-1]).name).read_text())
if sys.argv[1] == "rm":
    (capture / Path(sys.argv[-1]).name).unlink(missing_ok=True)
if sys.argv[1] == "install":
    if os.getenv("FAIL_INSTALL") == Path(sys.argv[-1]).name:
        sys.exit(1)
    shutil.copy2(sys.argv[-2], capture / Path(sys.argv[-1]).name)
if sys.argv[1:] == ["systemctl", "start", "zkm-fleet-node.service"]:
    sys.exit(int(os.getenv("FAIL_NODE", "0")))
# Enabled units persist in a file, so a test sees what a failed handover disabled again.
enabled = root / "enabled"
units = set(enabled.read_text().split()) if enabled.exists() else set()
if sys.argv[1:3] == ["systemctl", "is-enabled"]:
    sys.exit(0 if sys.argv[-1] in units else 1)
if sys.argv[1:3] in (["systemctl", "enable"], ["systemctl", "disable"]):
    names = {a for a in sys.argv[3:] if not a.startswith("-")}
    units = units | names if sys.argv[2] == "enable" else units - names
    enabled.write_text("\n".join(sorted(units)))
# Started Ray/shim units persist too; PREACTIVE names units already running before the install.
active_file = root / "active"
active = set(active_file.read_text().split()) if active_file.exists() else set(os.getenv("PREACTIVE", "").split())
if sys.argv[1:3] in (["systemctl", "start"], ["systemctl", "stop"]):
    names = {a for a in sys.argv[3:] if a != "zkm-fleet-node.service"}
    active = active | names if sys.argv[2] == "start" else active - names
    active_file.write_text("\n".join(sorted(active)))
if sys.argv[1:3] == ["systemctl", "is-active"] and sys.argv[-1] != "zkm-fleet-node.service":
    sys.exit(0 if sys.argv[-1] in active else 1)
if sys.argv[1:] == ["systemctl", "is-active", "--quiet", "zkm-fleet-node.service"]:
    # What the cron watchdog would read while systemd is still proving the agent runs.
    watchdog = root / "etc/watchdog.env"
    flag = watchdog.exists() and "FLEET_NODE_SUPERVISED=1" in watchdog.read_text()
    (root / "supervised-at-active").write_text("yes" if flag else "no")
    sys.exit(int(os.getenv("FAIL_ACTIVE", "0")))
