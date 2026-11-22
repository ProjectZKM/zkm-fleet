"""Control shim readiness failures while forwarding other Python commands."""
import os
import sys

if sys.argv[1:4] == ["-m", "fleet_bridge.shim", "check"]:
    sys.exit(int(os.getenv("FAIL_SHIM", "0")))

os.execv(sys.executable, [sys.executable, *sys.argv[1:]])
