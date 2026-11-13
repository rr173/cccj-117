"""Withdrawal registry and staged, crash-safe erasure.

Flow (each stage is its own committed transaction):
  1. register   - permanent registry rows + barrier entries.  From this
                  commit on, all read paths and new bundle requests hide the
                  persons, and intake suppresses their payload.
  2. editions   - delete the persons' rows from the edition history (the
                  current projection is a query over that history, so it
                  disappears there too).  Observation-identity tombstones and
                  batch digests are deliberately kept.
  3. bundles    - find every active bundle of the study that lists any of
                  the persons as a participant and delete the whole file
                  (never just the participant list).  Deletion targets ONLY
                  the canonical managed path bundles/<bundle_id>.json
                  derived from the bundle id; a path stored in the registry
                  is never used for unlink, so strange paths and anything
                  outside the managed directory can never be deleted on
                  request - a mismatch is flagged as an anomaly instead.
                  Each deletion commits separately so a killed process
                  leaves an auditable, resumable trail.  Missing files are
                  recorded as 'missing'.

Only when no affected data or file remains readable is the withdrawal marked
'completed'.  resume() re-enters the same state machine; re-registering the
same (withdrawal_id, study, person set) is an idempotent retry, while the
same id with a different set is a conflict.  Overlapping withdrawals both
stay in force.
"""
from __future__ import annotations

import os

from . import changefeed
from .store import reconcile, study_lock
from .syncpoints import sync_point
from .util import now_iso, sha256_hex


class WithdrawalConflict(Exception):
    pass


def _persons_of(conn, wid: str) -> list:
    return [r["person"] for r in conn.execute(
        "SELECT person FROM withdrawal_persons WHERE withdrawal_id=? ORDER BY person",
        (wid,))]


def _status_conn(conn, wid: str) -> dict | None:
    w = conn.execute("SELECT * FROM withdrawals WHERE withdrawal_id=?", (wid,)).fetchone()
    if w is None:
        return None
    bundles = {"pending": [], "cleared": [], "missing": [], "refused": []}
    for r in conn.execute(
            "SELECT bundle_id, state FROM withdrawal_bundles WHERE withdrawal_id=? "
            "ORDER BY bundle_id", (wid,)):
        bundles.setdefault(r["state"], []).append(r["bundle_id"])
    return {
        "withdrawal_id": wid,
        "study": w["study"],
        "state": w["state"],
        "persons": _persons_of(conn, wid),
        "editions_removed": w["editions_removed"],
        "bundles": bundles,
        "anomalies": w["anomalies"],
        "created_at": w["created_at"],
        "updated_at": w["updated_at"],
    }


def status(store, wid: str | None = None):
    conn = store.connect()
    try:
        if wid is not None:
            s = _status_conn(conn, wid)
            return None if s is None else [s]
        return [_status_conn(conn, r["withdrawal_id"]) for r in conn.execute(
            "SELECT withdrawal_id FROM withdrawals ORDER BY withdrawal_id")]
    finally:
        conn.close()


def register(store, wid: str, study: str, persons: list) -> dict:
    """Register a withdrawal (or idempotently retry it) and drive its stages.
    Raises WithdrawalConflict if the id exists with a different study or set."""
    persons = sorted(set(persons))
    if not persons:
        raise ValueError("person set must not be empty")
    if any(not isinstance(p, str) or not p for p in persons):
        raise ValueError("persons must be non-empty strings")
    reconcile(store)
    ph = sha256_hex("\n".join(persons).encode("utf-8"))
    conn = store.connect()
    try:
        with study_lock(store, study):
            conn.execute("BEGIN IMMEDIATE")
            w = conn.execute("SELECT * FROM withdrawals WHERE withdrawal_id=?",
                             (wid,)).fetchone()
            if w is None:
                ts = now_iso()
                conn.execute(
                    "INSERT INTO withdrawals VALUES (?,?,?,?,0,0,?,?)",
                    (wid, study, ph, "registered", ts, ts))
                conn.executemany("INSERT INTO withdrawal_persons VALUES (?,?)",
                                 [(wid, p) for p in persons])
                # Overlapping withdrawals: first claim wins the barrier row,
                # both registries stay in force.
                conn.executemany("INSERT OR IGNORE INTO barrier VALUES (?,?,?)",
                                 [(study, p, wid) for p in persons])
                # Remove notifications are written by the SAME transaction as
                # the barrier: from this commit they are consumable even
                # though erasure stages have not run yet.  An object already
                # announced as removed by an earlier withdrawal is skipped by
                # INSERT OR IGNORE inside the collector, so it can never be
                # brought back later.
                changefeed.on_withdrawal_registered(conn, wid, study, persons)
            elif w["study"] != study or w["person_hash"] != ph:
                conn.execute("ROLLBACK")
                raise WithdrawalConflict(wid)
            conn.execute("COMMIT")
    finally:
        conn.close()
    sync_point("withdraw.after_register")
    run_stages(store, wid)
    return status(store, wid)[0]


def _clear_one_bundle(store, conn, wid: str, bundle_id: str) -> None:
    row = conn.execute("SELECT * FROM bundles WHERE bundle_id=?", (bundle_id,)).fetchone()
    note = None
    anomaly = False
    if row is None or row["state"] != "active":
        outcome = "missing" if row is None else "cleared"
    else:
        # Only ever unlink the canonical managed path for this bundle id.
        # The registry's stored path is cross-checked but never followed.
        canonical = os.path.realpath(store.bundle_path(bundle_id))
        stored = os.path.realpath(os.path.join(store.root, row["path"]))
        if stored != canonical:
            anomaly = True
            note = "registry path mismatch; canonical managed file removed instead"
        try:
            os.remove(canonical)
            outcome = "cleared"
        except FileNotFoundError:
            outcome = "missing"
        except OSError:
            outcome, note = "refused", "unlink failed"
    conn.execute("BEGIN IMMEDIATE")
    conn.execute(
        "UPDATE withdrawal_bundles SET state=?, note=? "
        "WHERE withdrawal_id=? AND bundle_id=?", (outcome, note, wid, bundle_id))
    if outcome in ("cleared", "missing"):
        conn.execute("UPDATE bundles SET state='deleted' WHERE bundle_id=? AND state='active'",
                     (bundle_id,))
    if outcome == "refused" or anomaly:
        conn.execute("UPDATE withdrawals SET anomalies=anomalies+1 WHERE withdrawal_id=?",
                     (wid,))
    conn.execute("COMMIT")


def run_stages(store, wid: str) -> None:
    conn = store.connect()
    try:
        w = conn.execute("SELECT * FROM withdrawals WHERE withdrawal_id=?",
                         (wid,)).fetchone()
        if w is None:
            raise KeyError(wid)
        study = w["study"]
        persons = _persons_of(conn, wid)
        placeholders = ",".join("?" * len(persons))
        with study_lock(store, study):
            if w["state"] == "registered":
                conn.execute("BEGIN IMMEDIATE")
                cur = conn.execute(
                    "DELETE FROM editions WHERE study=? AND person IN (%s)" % placeholders,
                    [study] + persons)
                conn.execute(
                    "UPDATE withdrawals SET state='editions_cleared', editions_removed=?, "
                    "updated_at=? WHERE withdrawal_id=?",
                    (cur.rowcount, now_iso(), wid))
                conn.execute("COMMIT")
                w = conn.execute("SELECT * FROM withdrawals WHERE withdrawal_id=?",
                                 (wid,)).fetchone()
            if w["state"] == "editions_cleared":
                sync_point("withdraw.after_editions")
                # (Re)scan the registry for affected bundles.  Bundles
                # registered after the barrier cannot contain these persons
                # (registration re-checks the barrier under the same lock).
                conn.execute("BEGIN IMMEDIATE")
                rows = conn.execute(
                    "SELECT DISTINCT b.bundle_id FROM bundles b "
                    "JOIN bundle_participants bp ON bp.bundle_id = b.bundle_id "
                    "WHERE b.study=? AND b.state='active' AND bp.person IN (%s) "
                    "ORDER BY b.bundle_id" % placeholders,
                    [study] + persons).fetchall()
                conn.executemany(
                    "INSERT OR IGNORE INTO withdrawal_bundles VALUES (?,?,'pending',NULL)",
                    [(wid, r["bundle_id"]) for r in rows])
                conn.execute("COMMIT")
                pending = [r["bundle_id"] for r in conn.execute(
                    "SELECT bundle_id FROM withdrawal_bundles "
                    "WHERE withdrawal_id=? AND state='pending' ORDER BY bundle_id",
                    (wid,))]
                done = 0
                for bundle_id in pending:
                    _clear_one_bundle(store, conn, wid, bundle_id)
                    done += 1
                    sync_point("withdraw.after_bundle_delete.%d" % done)
                remaining = conn.execute(
                    "SELECT COUNT(*) AS c FROM withdrawal_bundles "
                    "WHERE withdrawal_id=? AND state='pending'", (wid,)).fetchone()["c"]
                if remaining == 0:
                    sync_point("withdraw.before_complete")
                    conn.execute("BEGIN IMMEDIATE")
                    conn.execute(
                        "UPDATE withdrawals SET state='completed', updated_at=? "
                        "WHERE withdrawal_id=?", (now_iso(), wid))
                    conn.execute("COMMIT")
    finally:
        conn.close()


def resume(store, wid: str | None = None, all_pending: bool = False) -> list:
    """Continue incomplete withdrawals.  A completed withdrawal is a no-op
    and can never re-release old data (the barrier and tombstones persist)."""
    reconcile(store)
    conn = store.connect()
    try:
        if all_pending:
            ids = [r["withdrawal_id"] for r in conn.execute(
                "SELECT withdrawal_id FROM withdrawals WHERE state != 'completed' "
                "ORDER BY withdrawal_id")]
        else:
            row = conn.execute("SELECT withdrawal_id FROM withdrawals WHERE withdrawal_id=?",
                               (wid,)).fetchone()
            if row is None:
                raise KeyError(wid)
            ids = [wid]
    finally:
        conn.close()
    for i in ids:
        run_stages(store, i)
    conn = store.connect()
    try:
        return [_status_conn(conn, i) for i in ids]
    finally:
        conn.close()
