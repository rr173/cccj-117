"""Deterministic test synchronization points.

When RVAULT_SYNC_DIR is set, every sync point writes <name>.reached into that
directory.  If the point's name is also listed in RVAULT_SYNC_WAIT (comma
separated), the process then blocks until <name>.proceed appears.

Test drivers use this to reproduce exact interleavings (e.g. a withdrawal
registered while a bundle sits between its read snapshot and its directory
registration) and to SIGKILL a process at a precise stage boundary - without
any sleep-based guessing.
"""
from __future__ import annotations

import os
import time

_TIMEOUT = 120.0


def sync_point(name: str) -> None:
    d = os.environ.get("RVAULT_SYNC_DIR")
    if not d:
        return
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, name + ".reached"), "w") as f:
        f.write(str(os.getpid()))
    wait_for = {w.strip() for w in os.environ.get("RVAULT_SYNC_WAIT", "").split(",") if w.strip()}
    if name not in wait_for:
        return
    proceed = os.path.join(d, name + ".proceed")
    deadline = time.time() + _TIMEOUT
    while time.time() < deadline:
        if os.path.exists(proceed):
            return
        time.sleep(0.05)
    raise RuntimeError("sync point timed out: " + name)
