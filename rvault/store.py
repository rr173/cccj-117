"""Managed storage layout, per-study locks and crash-recovery sweep.

Layout of a store directory (the only place the system ever writes):
  vault.sqlite3   logical state
  bundles/        readable directory of issued bundle files  bundles/<id>.json
  tmp/            scratch space (spooled intake, half-written bundles);
                  contents here are NEVER considered readable output
  receipts/       per-batch receipts (identifiers only, no payload)
  locks/          flock files, one per study

Locks are flock(2) based, so a killed process releases them automatically and
a new process can always make progress ("occupancy is recoverable after exit").
"""
from __future__ import annotations

import contextlib
import fcntl
import hashlib
import os

from . import db as dbmod


class Store:
    def __init__(self, root: str):
        self.root = os.path.abspath(root)
        self.db_path = os.path.join(self.root, "vault.sqlite3")
        self.bundles_dir = os.path.join(self.root, "bundles")
        self.tmp_dir = os.path.join(self.root, "tmp")
        self.receipts_dir = os.path.join(self.root, "receipts")
        self.locks_dir = os.path.join(self.root, "locks")

    def ensure(self) -> None:
        for d in (self.root, self.bundles_dir, self.tmp_dir, self.receipts_dir, self.locks_dir):
            os.makedirs(d, exist_ok=True)
        conn = self.connect()
        try:
            dbmod.init(conn)
        finally:
            conn.close()

    def connect(self):
        return dbmod.connect(self.db_path)

    def bundle_path(self, bundle_id: str) -> str:
        return os.path.join(self.bundles_dir, bundle_id + ".json")


@contextlib.contextmanager
def study_lock(store: Store, study: str):
    """Serialize bundle registration and withdrawal staging per study.

    The lock is an flock on a per-study file; the OS releases it when the
    holder dies, so a crashed process never wedges a study forever.
    """
    os.makedirs(store.locks_dir, exist_ok=True)
    key = hashlib.sha256(study.encode("utf-8")).hexdigest()[:32]
    path = os.path.join(store.locks_dir, key + ".lock")
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def reconcile(store: Store) -> dict:
    """Crash-recovery sweep; idempotent and safe to run anytime.

    - bundles left in state 'registering' by a dead process are aborted and
      their half-placed files removed (a bundle is only ever visible once it
      reaches state 'active')
    - stale scratch files in tmp/ are removed (spool/receipt files whose
      owner pid is gone, bundle tmp files with no live 'registering' row,
      decoded mail attachments whose owner pid is gone; mail temp files are
      never evidence of receipt - the durable record lives in ml_* tables)
    """
    result = {"aborted_bundles": [], "tmp_removed": 0}
    if not os.path.isdir(store.root):
        return result
    store.ensure()
    conn = store.connect()
    try:
        rows = conn.execute("SELECT bundle_id, study FROM bundles WHERE state='registering'").fetchall()
        for row in rows:
            # The live writer (if any) holds the study lock across its whole
            # critical section, so once we hold it the row is stable.
            with study_lock(store, row["study"]):
                cur = conn.execute("SELECT state FROM bundles WHERE bundle_id=?",
                                   (row["bundle_id"],)).fetchone()
                if cur and cur["state"] == "registering":
                    for p in (os.path.join(store.tmp_dir, row["bundle_id"] + ".tmp"),
                              store.bundle_path(row["bundle_id"])):
                        try:
                            os.remove(p)
                        except FileNotFoundError:
                            pass
                    conn.execute("BEGIN IMMEDIATE")
                    conn.execute("UPDATE bundles SET state='aborted' WHERE bundle_id=?",
                                 (row["bundle_id"],))
                    conn.execute("COMMIT")
                    result["aborted_bundles"].append(row["bundle_id"])
        registering = {r["bundle_id"] for r in conn.execute(
            "SELECT bundle_id FROM bundles WHERE state='registering'")}
        for name in os.listdir(store.tmp_dir):
            remove = False
            if name.endswith(".tmp"):
                remove = name[:-4] not in registering
            elif name.startswith("intake.") and name.endswith(".spool"):
                remove = _stale_pid_file(name)
            elif name.startswith("receipt.") and name.endswith(".jsonl"):
                remove = _stale_pid_file(name)
            elif name.startswith("mail.") and name.endswith(".part"):
                remove = _stale_pid_file(name)
            elif name.startswith("nested.") and name.endswith(".input"):
                remove = _stale_pid_file(name)
            if remove:
                try:
                    os.remove(os.path.join(store.tmp_dir, name))
                    result["tmp_removed"] += 1
                except FileNotFoundError:
                    pass
    finally:
        conn.close()
    return result


def _stale_pid_file(name: str) -> bool:
    try:
        pid = int(name.split(".")[1])
    except (IndexError, ValueError):
        return False
    return not _pid_alive(pid)
