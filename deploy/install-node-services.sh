#!/bin/bash
# Install companions without converting live allocations or replacing operator units.
#   install-node-services.sh PREFIX ETC WORKDIR SHIM [--check]
#   install-node-services.sh PREFIX '' '' SHIM --stage-only
# --stage-only publishes SHIM and its bundled tunnel helper under PREFIX/libexec and nothing
# else: fleet-ctl node upgrade runs it beside a live agent and shim.
set -euo pipefail
PREFIX=$1; ETC=$2; WORKDIR=$3; SHIM=$4
die(){ echo "[node-services] $*" >&2; exit 1; }
# A release bundle carries the pinned Chisel helper next to the shim binary.
TUNNEL=
if [ -e "$(dirname "$SHIM")/trusted-router-tunnel" ]; then
  TUNNEL="$(dirname "$SHIM")/trusted-router-tunnel"
  [ -f "$TUNNEL" ] && [ -x "$TUNNEL" ] || die "bundled trusted-router-tunnel is not an executable file"
fi
PYTHON="$PREFIX/venv/bin/python"
[ -x "$PYTHON" ] || PYTHON=python3
# Publish SRC at $PREFIX/libexec/DIR/<sha256>/FILE and set PINNED to that path.
# Existing hashes are never replaced, so live processes keep their executable.
pin() {
  local src=$1 dir=$2 name=$3 hash dest
  hash=$(sha256sum "$src" | cut -d ' ' -f 1)
  dest="$PREFIX/libexec/$dir/$hash/$name"
  mkdir -p "$(dirname "$dest")"
  # An interrupted copy cannot poison a content-addressed path permanently.
  if [ ! -f "$dest" ] || [ "$(sha256sum "$dest" | cut -d ' ' -f 1)" != "$hash" ]; then
    TEMP=$(mktemp "$dest.XXXXXX")
    install -m 755 "$src" "$TEMP"
    [ "$(sha256sum "$TEMP" | cut -d ' ' -f 1)" = "$hash" ] || die "$name copy checksum mismatch"
    "$PYTHON" - "$TEMP" "$dest" <<'PY'
import os, sys
from pathlib import Path
with open(sys.argv[1], 'rb') as f: os.fsync(f.fileno())
os.replace(sys.argv[1], sys.argv[2])
fd=os.open(Path(sys.argv[2]).parent, os.O_RDONLY | os.O_DIRECTORY)
try: os.fsync(fd)
finally: os.close(fd)
PY
    TEMP=
  fi
  PINNED=$dest
}
# Stage the shim and the bundled Chisel helper only, setting DEST to the shim's copy. Selecting
# the helper is an explicit operator change to shim_network.tunnel_binary; omitting it keeps the
# existing transport. Staging never switches or restarts a running shim.
stage_runtime() {
  pin "$SHIM" trusted-shim trusted-shim-runtime
  DEST=$PINNED
  echo "[node-services] staged trusted-shim-runtime at $DEST" >&2
  if [ -n "$TUNNEL" ]; then
    pin "$TUNNEL" trusted-router-tunnel trusted-router-tunnel
    echo "[node-services] staged Chisel helper at $PINNED (select via shim_network.tunnel_binary)" >&2
  fi
}
if [ "${5:-}" = --stage-only ]; then
  [[ "$PREFIX" = /* && "$PREFIX" != *$'\n'* && "$PREFIX" != *$'\r'* ]] || die "unsupported service path"
  [ -f "$SHIM" ] || die "no shim runtime $SHIM"
  trap '[ -z "${TEMP:-}" ] || rm -f "$TEMP"' EXIT
  stage_runtime
  exit 0
fi
for path in "$PREFIX" "$ETC" "$WORKDIR"; do
  [[ "$path" = /* && "$path" != *$'\n'* && "$path" != *$'\r'* && "$path" != *'%'* && "$path" != *'$'* ]] || die "unsupported service path"
done
# Hold node-start's lock until handover. Only the control agent must be stopped
# for first conversion; existing Ray actors, containers and shim stay alive.
mkdir -p "$WORKDIR"
if ! [ "/proc/$$/fd/9" -ef "$WORKDIR/node-agent.lock" ]; then
  exec 9>"$WORKDIR/node-agent.lock"
fi
flock -n 9 || die "control agent is running; use fleet-ctl node upgrade, or stop only the control agent before first supervisor conversion"
# A fresh install creates its venv later. The full pass repeats validation with
# that interpreter before changing configuration or installing services.
if [ "${5:-}" = --check ] && ! "$PYTHON" -c 'import yaml' 2>/dev/null; then exit 0; fi
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
COMMITTED=0
CHANGED=()
ENABLED=()
STARTED=()
RELOADED=0
STARTED_NODE=0
# Until the node unit is active, a failure hands the agent back to the cron watchdog: the
# configuration is restored and the units this run enabled are disabled again, with the agent
# this run started under them. A unit systemd already owned keeps retrying. A Ray watcher or
# shim daemon this run started is stopped before its unit and configuration are restored, so
# nothing keeps running on settings the rollback removed. No agent ran under it yet (this run
# holds the agent lock), and Ray's unit stops only its watcher. A service that was already
# running keeps running, so no runtime state is reverted.
restore() {
  local status=$?
  if [ "$status" != 0 ] && [ "$COMMITTED" = 0 ]; then
    if [ "$STARTED_NODE" = 1 ] && [[ " ${ENABLED[*]} " = *" zkm-fleet-node.service "* ]]; then
      sudo systemctl stop zkm-fleet-node.service || true
    fi
    [ "${#STARTED[@]}" = 0 ] || sudo systemctl stop "${STARTED[@]}" || true
    [ "${#ENABLED[@]}" = 0 ] || sudo systemctl disable "${ENABLED[@]}" || true
    for name in "${CHANGED[@]}"; do
      if [ -f "$STAGE/$name.before" ]; then
        sudo install -m 644 "$STAGE/$name.before" "/etc/systemd/system/$name"
      else
        sudo rm -f "/etc/systemd/system/$name"
      fi
    done
    [ "$RELOADED" = 0 ] || sudo systemctl daemon-reload || true
    cp -p "$STAGE/apps.before" "$ETC/apps.yaml.restore"
    mv "$ETC/apps.yaml.restore" "$ETC/apps.yaml"
    cp -p "$STAGE/watchdog.before" "$ETC/watchdog.env.restore"
    mv "$ETC/watchdog.env.restore" "$ETC/watchdog.env"
    echo '[node-services] handover failed; stopped the services this run started, restored apps.yaml and watchdog.env and disabled the units this run enabled, so the cron watchdog keeps supervising the agent. Repair the failure and rerun.' >&2
  elif [ "$status" != 0 ]; then
    echo '[node-services] configuration committed; services retained. Repair the failure and rerun to finish handover.' >&2
  fi
  rm -rf "$STAGE"
  [ -z "${TEMP:-}" ] || rm -f "$TEMP"
  exit "$status"
}
trap restore EXIT
USER_NAME=$(id -un)
stage_runtime
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
    Path(stage,f'zkm-fleet-{name}.service.legacy').write_text(text)
    initial=text.replace('StartLimitIntervalSec=0\n','')
    if name=='ray':
        initial=(initial.replace('Description=Fleet local Ray watchdog','Description=Fleet local Ray')
            .replace('Type=simple','Type=oneshot').replace('/ray-watch.sh','/fleet-watchdog.sh')
            .replace('Restart=on-failure\nRestartSec=2\n','RemainAfterExit=yes\nEnvironment=FLEET_NODE_SUPERVISED=1\n'))
    Path(stage,f'zkm-fleet-{name}.service.initial').write_text(initial)
    if name=='ray':
        text=text.replace('KillMode=control-group','KillMode=process')
    Path(stage,f'zkm-fleet-{name}.service').write_text('# Managed by zkm-fleet install-node-services\n'+text)
PY
for unit in "$STAGE/"*.service; do
  target="/etc/systemd/system/$(basename "$unit")"
  name=$(basename "$unit")
  # Refresh our exact previous output; preserve operator edits and drop-ins.
  if sudo test -e "$target"; then
    sudo cat "$target" > "$STAGE/$name.before"
    if ! cmp -s "$STAGE/$name.before" "$unit.legacy" && ! cmp -s "$STAGE/$name.before" "$unit.initial" && ! cmp -s "$STAGE/$name.before" "$PREFIX/libexec/node-services/$name"; then
      continue
    fi
  fi
  CHANGED+=("$name")
  sudo install -m 644 "$unit" "$target"
done
RELOADED=1
sudo systemctl daemon-reload
for unit in zkm-fleet-ray.service zkm-fleet-shim.service; do
  sudo systemctl is-active --quiet "$unit" || STARTED+=("$unit")
done
sudo systemctl start zkm-fleet-ray.service zkm-fleet-shim.service
(cd "$PREFIX/bridge" && "$PYTHON" -m fleet_bridge.shim check --apps "$ETC/apps.yaml" --wait-seconds 30)
for unit in zkm-fleet-ray.service zkm-fleet-shim.service zkm-fleet-node.service; do
  sudo systemctl is-enabled --quiet "$unit" || ENABLED+=("$unit")
done
sudo systemctl enable zkm-fleet-ray.service zkm-fleet-shim.service zkm-fleet-node.service
flock -u 9
STARTED_NODE=1
sudo systemctl start zkm-fleet-node.service
sudo systemctl is-active --quiet zkm-fleet-node.service || die "node unit did not remain active"
# Commit only now that systemd runs the agent: from here the cron watchdog leaves it alone, and
# an interrupted handover is resumed in place.
(cd "$PREFIX/bridge" && "$PYTHON" - "$ETC/watchdog.env" <<'PY'
import sys
from pathlib import Path
from fleet_bridge.node import _replace
p=Path(sys.argv[1]);text=p.read_text()
if not text.rstrip().endswith('FLEET_NODE_SUPERVISED=1'):
    _replace(p,text.rstrip()+'\nFLEET_NODE_SUPERVISED=1\n')
PY
)
mkdir -p "$PREFIX/libexec/node-services"
for name in "${CHANGED[@]}"; do cp "$STAGE/$name" "$PREFIX/libexec/node-services/$name"; done
COMMITTED=1
