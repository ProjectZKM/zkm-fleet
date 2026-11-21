#!/bin/bash
# Called by install.sh --with-shim. Keep control, runtime and Ray in separate
# service cgroups. Configuration stays in apps.yaml and watchdog.env.
set -euo pipefail
PREFIX=$1; ETC=$2; WORKDIR=$3; SHIM=$4
[[ "$PREFIX$ETC$WORKDIR" != *$'\n'* && "$PREFIX$ETC$WORKDIR" != *'%'* ]] || { echo "unsupported service path" >&2; exit 1; }
USER_NAME=$(id -un)
HASH=$(sha256sum "$SHIM" | cut -d ' ' -f 1)
DEST="$PREFIX/libexec/trusted-shim/$HASH/trusted-shim-runtime"
mkdir -p "$(dirname "$DEST")"
# Never overwrite the executable used by existing allocations.
if [ ! -e "$DEST" ]; then install -m 755 "$SHIM" "$DEST"; fi
"$PREFIX/venv/bin/python" - "$ETC/apps.yaml" "$DEST" "$WORKDIR/shim" <<'PY'
import os, sys, tempfile, yaml
from pathlib import Path
path, binary, state = sys.argv[1:]
data = yaml.safe_load(Path(path).read_text())
if data.get('shim_binary') or data.get('shim_state_dir'):
    if data.get('shim_daemon') is not True:
        raise SystemExit('Existing shim transport is preserved. Explicitly drain its allocations before enabling daemon mode.')
    # A package upgrade stages the new binary; it never switches a live daemon.
else:
    if len(os.fsencode(state + '/control.sock')) >= 108:
        raise SystemExit('shim state directory is too long for a Unix socket')
    data.update(shim_binary=binary, shim_state_dir=state, shim_daemon=True)
    with tempfile.NamedTemporaryFile(mode='w', dir=Path(path).parent, delete=False) as f:
        yaml.safe_dump(data, f, sort_keys=False)
        f.flush(); os.fsync(f.fileno())
        temporary = f.name
    os.replace(temporary, path)
    fd = os.open(Path(path).parent, os.O_RDONLY | os.O_DIRECTORY)
    try: os.fsync(fd)
    finally: os.close(fd)
PY
STAGE=$(mktemp -d)
trap 'rm -rf "$STAGE"' EXIT
# Quote literal unit paths; neither a shell nor environment expansion is used by
# systemd for ExecStart. Percent specifiers were rejected above.
"$PREFIX/venv/bin/python" - "$STAGE" "$PREFIX" "$ETC" "$USER_NAME" <<'PY'
import json, sys
from pathlib import Path
stage, prefix, etc, user = sys.argv[1:]
q = json.dumps
for name, description, script, kind, extra in [
    ('ray', 'Fleet local Ray', 'fleet-watchdog.sh', 'oneshot', 'RemainAfterExit=yes\nEnvironment=FLEET_NODE_SUPERVISED=1\n'),
    ('shim', 'Fleet persistent runtime shim', 'shim-start.sh', 'simple', 'Restart=on-failure\nRestartSec=2\n'),
    ('node', 'Fleet node control agent', 'node-start.sh', 'simple', 'Restart=on-failure\nRestartSec=2\n'),
]:
    ordering = 'Wants=zkm-fleet-ray.service zkm-fleet-shim.service\nAfter=zkm-fleet-ray.service zkm-fleet-shim.service\n' if name == 'node' else ''
    text = (f'[Unit]\nDescription={description}\nAfter=network-online.target\n{ordering}'
        f'\n[Service]\nType={kind}\nUser={user}\nEnvironment={q("FLEET_ENV=" + etc + "/watchdog.env")}\n'
        f'ExecStart=/bin/bash {q(prefix + "/deploy/" + script)}\nKillMode=control-group\n'
        f'TimeoutStopSec=180\n{extra}\n[Install]\nWantedBy=multi-user.target\n')
    Path(stage, f'zkm-fleet-{name}.service').write_text(text)
PY
sudo install -m 644 "$STAGE/"*.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable zkm-fleet-ray.service zkm-fleet-shim.service zkm-fleet-node.service
# Start is deliberately not restart: installing a package preserves live workers.
sudo systemctl start zkm-fleet-ray.service zkm-fleet-shim.service
(cd "$PREFIX/bridge" && "$PREFIX/venv/bin/python" -m fleet_bridge.shim check --apps "$ETC/apps.yaml" --wait-seconds 30)
sudo systemctl start zkm-fleet-node.service
