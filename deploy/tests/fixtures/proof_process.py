#!/usr/bin/env python3
"""Fake child for container supervisor tests; contains no proof implementation."""
import os
from pathlib import Path
import signal
import sys
import time

name = Path(sys.argv[0]).name
state = Path(os.environ['TEST_CHILD_STATE'])


def stop(*_):
    (state / (name + '.stopped')).write_text('stopped')
    sys.exit(0)


signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)
(state / (name + '.pid')).write_text(str(os.getpid()))
if os.environ.get('TEST_FAIL_PROCESS') == name:
    deadline = time.monotonic() + 5
    while len(list(state.glob('*.pid'))) != 2 and time.monotonic() < deadline:
        time.sleep(.02)
    sys.exit(int(os.environ['TEST_EXIT_CODE']))
while True:
    time.sleep(.1)
