#!/bin/bash
# Install companions without converting live allocations or replacing operator units.
set -euo pipefail
PREFIX=$1; ETC=$2; WORKDIR=$3; SHIM=$4
die(){ echo "[node-services] $*" >&2; exit 1; }
for path in "$PREFIX" "$ETC" "$WORKDIR"; do
  [[ "$path" = /* && "$path" != *$'\n'* && "$path" != *$'\r'* && "$path" != *'%'* && "$path" != *'$'* ]] || die "unsupported service path"
done
# Hold node-start's lock until handover. Only the control agent must be stopped
# for first conversion; existing Ray actors, containers and shim stay alive.
mkdir -p "$WORKDIR"
if [ "$(readlink "/proc/$$/fd/9" 2>/dev/null || true)" != "$WORKDIR/node-agent.lock" ]; then
  exec 9>"$WORKDIR/node-agent.lock"
fi
flock -n 9 || die "control agent is running; use fleet-ctl node upgrade, or stop only the control agent before first supervisor conversion"
PYTHON="$PREFIX/venv/bin/python"
[ -x "$PYTHON" ] || PYTHON=python3
"$PYTHON" - "$ETC/apps.yaml" "$WORKDIR/shim" <<'PY'
import os, sys, yaml
from pathlib import Path
path = Path(sys.argv[1])
data = (yaml.safe_load(path.read_text()) or {}) if path.exists() else {}
if not isinstance(data, dict):
    raise SystemExit('apps.yaml must contain a mapping')
if data.get('shim_binary') or data.get('shim_state_dir') or data.get('shim_daemon'):
    if data.get('shim_daemon') is not True:
        raise SystemExit('Existing shim transport is preserved. Explicitly drain its allocations before enabling daemon mode.')
    if not all(isinstance(data.get(k), str) and os.path.isabs(data[k]) and '\0' not in data[k]
               for k in ('shim_binary','shim_state_dir')):
        raise SystemExit('shim daemon requires absolute shim_binary and shim_state_dir')
state = data.get('shim_state_dir') if data.get('shim_daemon') else sys.argv[2]
if len(os.fsencode(state + '/control.sock')) >= 108:
    raise SystemExit('shim state directory is too long for a Unix socket')
PY
[ "${5:-}" != --check ] || exit 0
STAGE=$(mktemp -d)
cp -p "$ETC/apps.yaml" "$STAGE/apps.before"
cp -p "$ETC/watchdog.env" "$STAGE/watchdog.before"
restore() {
  local status=$?
  if [ "$status" != 0 ]; then
    # Restore the previous watchdog authority without stopping live companions.
    cp -p "$STAGE/apps.before" "$ETC/apps.yaml.restore"
    mv "$ETC/apps.yaml.restore" "$ETC/apps.yaml"
    cp -p "$STAGE/watchdog.before" "$ETC/watchdog.env.restore"
    mv "$ETC/watchdog.env.restore" "$ETC/watchdog.env"
  fi
  rm -rf "$STAGE"
  [ -z "${TEMP:-}" ] || rm -f "$TEMP"
  exit "$status"
}
trap restore EXIT
USER_NAME=$(id -un)
HASH=$(sha256sum "$SHIM" | cut -d ' ' -f 1)
DEST="$PREFIX/libexec/trusted-shim/$HASH/trusted-shim-runtime"
mkdir -p "$(dirname "$DEST")"
# An interrupted copy cannot poison a content-addressed path permanently.
if [ ! -f "$DEST" ] || [ "$(sha256sum "$DEST" | cut -d ' ' -f 1)" != "$HASH" ]; then
  TEMP=$(mktemp "$DEST.XXXXXX")
  install -m 755 "$SHIM" "$TEMP"
  [ "$(sha256sum "$TEMP" | cut -d ' ' -f 1)" = "$HASH" ] || die "shim copy checksum mismatch"
  "$PYTHON" - "$TEMP" "$DEST" <<'PY'
import os, sys
from pathlib import Path
with open(sys.argv[1], 'rb') as f: os.fsync(f.fileno())
os.replace(sys.argv[1], sys.argv[2])
fd=os.open(Path(sys.argv[2]).parent, os.O_RDONLY | os.O_DIRECTORY)
try: os.fsync(fd)
finally: os.close(fd)
PY
fi
(cd "$PREFIX/bridge" && "$PYTHON" - "$ETC/apps.yaml" "$DEST" "$WORKDIR/shim" <<'PY'
import json, os, sys, yaml
from pathlib import Path
from fleet_bridge.node import _replace
path, binary, state = sys.argv[1:]
path=Path(path);text=path.read_text();data=yaml.safe_load(text) or {}
if not data.get('shim_daemon'):
    if len(os.fsencode(state + '/control.sock')) >= 108:
        raise SystemExit('shim state directory is too long for a Unix socket')
    settings=dict(shim_binary=binary, shim_state_dir=state, shim_daemon=True)
    text=''.join(f'{k}: {json.dumps(v)}\n' for k,v in settings.items()) + text
    if any(yaml.safe_load(text).get(k) != v for k,v in settings.items()):
        raise SystemExit('conflicting shim configuration keys')
    _replace(path,text)
PY
)
# Reject systemd percent/dollar substitutions; quote spaces and backslashes.
"$PYTHON" - "$STAGE" "$PREFIX" "$ETC" "$USER_NAME" <<'PY'
import json, sys
from pathlib import Path
stage,prefix,etc,user=sys.argv[1:];q=json.dumps
for name,description,script in [
    ('ray','Fleet local Ray watchdog','ray-watch.sh'),
    ('shim','Fleet persistent runtime shim','shim-start.sh'),
    ('node','Fleet node control agent','node-start.sh'),
]:
    ordering='Wants=zkm-fleet-ray.service zkm-fleet-shim.service\nAfter=zkm-fleet-ray.service zkm-fleet-shim.service\n' if name=='node' else ''
    text=(f'[Unit]\nDescription={description}\nAfter=network-online.target\nStartLimitIntervalSec=0\n{ordering}'
        f'\n[Service]\nType=simple\nUser={user}\nEnvironment={q("FLEET_ENV="+etc+"/watchdog.env")}\n'
        f'ExecStart=/bin/bash {q(prefix+"/deploy/"+script)}\nKillMode=control-group\n'
        'TimeoutStopSec=180\nRestart=on-failure\nRestartSec=2\n\n[Install]\nWantedBy=multi-user.target\n')
    Path(stage,f'zkm-fleet-{name}.service').write_text(text)
PY
for unit in "$STAGE/"*.service; do
  target="/etc/systemd/system/$(basename "$unit")"
  # Operator-written units and drop-ins remain authoritative.
  if ! sudo test -e "$target"; then sudo install -m 644 "$unit" "$target"; fi
done
sudo systemctl daemon-reload
# Start is deliberately not restart: existing runtime processes keep their PID.
sudo systemctl start zkm-fleet-ray.service zkm-fleet-shim.service
(cd "$PREFIX/bridge" && "$PYTHON" -m fleet_bridge.shim check --apps "$ETC/apps.yaml" --wait-seconds 30)
sudo systemctl enable zkm-fleet-ray.service zkm-fleet-shim.service zkm-fleet-node.service
(cd "$PREFIX/bridge" && "$PYTHON" - "$ETC/watchdog.env" <<'PY'
import sys
from pathlib import Path
from fleet_bridge.node import _replace
p=Path(sys.argv[1]);text=p.read_text()
if not text.rstrip().endswith('FLEET_NODE_SUPERVISED=1'):
    _replace(p,text.rstrip()+'\nFLEET_NODE_SUPERVISED=1\n')
PY
)
flock -u 9
sudo systemctl start zkm-fleet-node.service
sudo systemctl is-active --quiet zkm-fleet-node.service || die "node unit did not remain active"
