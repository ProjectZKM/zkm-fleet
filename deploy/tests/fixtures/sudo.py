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
