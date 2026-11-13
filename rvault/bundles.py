"""Dataset bundles for analysts.

A bundle is a JSON document with a unique, never-reused id containing the
adopted edition per observation plus group_code aggregates; details and
aggregates are computed from one read snapshot.  Files are written only into
the store's managed area: staged in tmp/, then atomically renamed into the
readable bundles/ directory and registered as 'active'.  Half-written files
never appear in the readable directory, and a crashed attempt is cleaned up
by reconcile().

Interleaving with withdrawal: the read snapshot is taken WITHOUT the study
lock; the registration critical section re-checks the withdrawal barrier
under the lock.  Therefore:
  * withdrawal registered before our critical section -> we abort and leave
    no file behind;
  * our registration committed first -> the withdrawal's staged cleanup
    finds the bundle via the registry and deletes it.
"""
from __future__ import annotations

import os

from .query import aggregate, projection_rows
from .store import reconcile, study_lock
from .syncpoints import sync_point
from .util import canonical_bytes, now_iso, sha256_hex


class BundleAborted(Exception):
    def __init__(self, barred_count: int):
        super().__init__("participants under withdrawal")
        self.barred_count = barred_count


class BundleNotFound(Exception):
    pass


class BundleGone(Exception):
    def __init__(self, state: str, note: str = ""):
        super().__init__(state)
        self.state = state
        self.note = note


class BundleTampered(Exception):
    pass


def _alloc_bundle_id(conn) -> str:
    row = conn.execute("SELECT value FROM counters WHERE name='bundle_seq'").fetchone()
    n = 1 if row is None else row["value"] + 1
    if row is None:
        conn.execute("INSERT INTO counters VALUES ('bundle_seq', 1)")
    else:
        conn.execute("UPDATE counters SET value=? WHERE name='bundle_seq'", (n,))
    return "B%06d" % n


def build_content(rows, study: str, groups: dict, bundle_id: str, created_at: str) -> dict:
    participants = sorted({r["person"] for r in rows})
    observations = [{
        "observation": r["observation"], "person": r["person"], "edition": r["edition"],
        "display_name": r["display_name"], "measure": r["measure"], "memo": r["memo"],
    } for r in rows]
    return {
        "bundle_id": bundle_id,
        "study": study,
        "created_at": created_at,
        "participants": participants,
        "observations": observations,
        "aggregates": aggregate(rows, groups),
    }


def create_bundle(store, study: str, groups: dict):
    """Returns (bundle_id, content). Raises BundleAborted if the withdrawal
    barrier appeared between the read snapshot and registration."""
    reconcile(store)
    conn = store.connect()
    try:
        # One read snapshot for both details and aggregates.
        conn.execute("BEGIN")
        rows = projection_rows(conn, study)
        conn.execute("COMMIT")
        participants = sorted({r["person"] for r in rows})

        sync_point("bundle.after_read")

        with study_lock(store, study):
            sync_point("bundle.in_critical")
            barred = [p for p in participants if conn.execute(
                "SELECT 1 FROM barrier WHERE study=? AND person=?",
                (study, p)).fetchone()]
            if barred:
                # Withdrawal landed first: leave nothing behind.
                raise BundleAborted(len(barred))
            conn.execute("BEGIN IMMEDIATE")
            bundle_id = _alloc_bundle_id(conn)
            created_at = now_iso()
            content = build_content(rows, study, groups, bundle_id, created_at)
            data = canonical_bytes(content)
            digest = sha256_hex(data)
            rel = "bundles/" + bundle_id + ".json"
            conn.execute(
                "INSERT INTO bundles VALUES (?,?,?,?,?,'registering')",
                (bundle_id, study, created_at, rel, digest))
            conn.executemany(
                "INSERT INTO bundle_participants VALUES (?,?)",
                [(bundle_id, p) for p in participants])
            conn.execute("COMMIT")

            tmp = os.path.join(store.tmp_dir, bundle_id + ".tmp")
            with open(tmp, "wb") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, store.bundle_path(bundle_id))

            conn.execute("BEGIN IMMEDIATE")
            conn.execute("UPDATE bundles SET state='active' WHERE bundle_id=?", (bundle_id,))
            conn.execute("COMMIT")

        sync_point("bundle.after_register")
        return bundle_id, content
    finally:
        conn.close()


def list_bundles(store, include_all: bool = False) -> list:
    conn = store.connect()
    try:
        q = "SELECT bundle_id, study, state, created_at FROM bundles"
        if not include_all:
            q += " WHERE state='active'"
        q += " ORDER BY bundle_id"
        return [dict(r) for r in conn.execute(q)]
    finally:
        conn.close()


def read_bundle(store, bundle_id: str) -> bytes:
    conn = store.connect()
    try:
        row = conn.execute("SELECT * FROM bundles WHERE bundle_id=?",
                           (bundle_id,)).fetchone()
    finally:
        conn.close()
    if row is None:
        raise BundleNotFound(bundle_id)
    if row["state"] != "active":
        raise BundleGone(row["state"])
    try:
        with open(store.bundle_path(bundle_id), "rb") as f:
            data = f.read()
    except FileNotFoundError:
        raise BundleGone("file_missing", "registry says active but file is gone")
    if sha256_hex(data) != row["digest"]:
        raise BundleTampered(bundle_id)
    return data
