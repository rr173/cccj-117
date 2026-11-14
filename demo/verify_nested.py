#!/usr/bin/env python3
"""Independent verifier for the nested study-table demo.

Reads the OUTPUT relational database directly (its own SQL, no rvault code)
and compares every table against the hand-written expectations in
expected_nested.py: row identity, parent identity, source coordinates,
projected values and per-column presence markers.  Also checks per-root
processing records and run status.

Usage: python3 verify_nested.py --database DB --checkpoint NAME
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import expected_nested as E  # noqa: E402

FAILURES = []


def check(name, actual, expected):
    if actual != expected:
        FAILURES.append(name)
        print("FAIL %s\n  expected: %r\n  actual:   %r"
              % (name, expected, actual))
    else:
        print("ok   %s" % name)


def connect(path):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def physical_rows(conn, table):
    """Every physical row, shaped exactly as the expectations describe."""
    cols = [r[1] for r in conn.execute('PRAGMA table_info("%s")' % table)]
    value_cols = [c for c in cols
                  if c not in ("row_id", "parent_row_id", "root_row_id",
                               "source_digest", "root_index", "index_path",
                               "array_path", "ordinal")
                  and not c.endswith("__present")]
    out = []
    for r in conn.execute('SELECT * FROM "%s" ORDER BY root_index, ordinal, '
                          'row_id' % table):
        vals = {}
        for c in value_cols:
            vals[c] = {"value": r[c], "presence": r[c + "__present"]}
        out.append({
            "row_id": r["row_id"],
            "parent_row_id": r["parent_row_id"],
            "root_row_id": r["root_row_id"],
            "root_index": r["root_index"],
            "index_path": json.loads(r["index_path"]),
            "array_path": json.loads(r["array_path"]),
            "ordinal": r["ordinal"],
            "values": vals,
        })
    return out


def check_tables(conn, expected, label):
    for table, rows in expected.items():
        check("%s:%s rows" % (label, table), physical_rows(conn, table), rows)


def check_root_records(conn, expected, label, digest):
    rows = [dict(r) for r in conn.execute(
        "SELECT root_index, status, category FROM ns_roots "
        "WHERE source_digest=? ORDER BY root_index", (digest,))]
    check("%s:root records" % label, rows, expected)


def check_counts(conn, expected, label, digest):
    actual = {}
    for table in expected:
        actual[table] = conn.execute(
            'SELECT COUNT(*) FROM "%s" WHERE source_digest=?'
            % table, (digest,)).fetchone()[0]
    check("%s:row counts" % label, actual, expected)


def run_record(conn, digest):
    r = conn.execute("SELECT * FROM ns_runs WHERE source_digest=?",
                     (digest,)).fetchone()
    return dict(r) if r else None


def check_run_status(conn, digest, status, label):
    r = run_record(conn, digest)
    check("%s:run status" % label, r["status"] if r else None, status)


# ---------------------------------------------------------------------------
# checkpoints
# ---------------------------------------------------------------------------

def cp_example(db):
    conn = connect(db)
    try:
        check_tables(conn, E.E_EXAMPLE, "example")
        check_counts(conn, E.E_EXAMPLE_COUNTS, "example", E.D_EXAMPLE)
        check_root_records(conn, [{"root_index": 0, "status": "committed",
                                   "category": None}], "example", E.D_EXAMPLE)
        check_run_status(conn, E.D_EXAMPLE, "complete", "example")
        check("example:one root", conn.execute(
            "SELECT COUNT(*) FROM ns_roots WHERE source_digest=?",
            (E.D_EXAMPLE,)).fetchone()[0], 1)
        # no row may reference a parent from another root
        for table in ("visits", "readings", "consents", "tags", "subs"):
            bad = conn.execute(
                'SELECT COUNT(*) FROM "%s" c JOIN ns_roots r '
                "ON c.root_row_id = r.root_row_id "
                "WHERE c.source_digest=? AND r.status != 'committed'"
                % table, (E.D_EXAMPLE,)).fetchone()[0]
            check("example:%s parents committed" % table, bad, 0)
    finally:
        conn.close()


def cp_late(db):
    conn = connect(db)
    try:
        check_tables(conn, E.E_LATE, "late")
        check_run_status(conn, E.D_LATE, "complete", "late")
    finally:
        conn.close()


def cp_variants(db):
    conn = connect(db)
    try:
        check_tables(conn, E.E_VARIANTS, "variants")
        check_counts(conn, E.E_VARIANTS_COUNTS, "variants", E.D_VARIANTS)
        check_root_records(conn, [
            {"root_index": 0, "status": "committed", "category": None},
            {"root_index": 1, "status": "committed", "category": None},
        ], "variants", E.D_VARIANTS)
    finally:
        conn.close()


def cp_bad_middle(db):
    conn = connect(db)
    try:
        check_counts(conn, E.E_BAD_MIDDLE_COUNTS, "bad_middle", E.D_BAD_MIDDLE)
        check_root_records(conn, E.E_BAD_MIDDLE_ROOTS, "bad_middle",
                           E.D_BAD_MIDDLE)
        persons = [r[0] for r in conn.execute(
            "SELECT person FROM participants WHERE source_digest=? "
            "ORDER BY ordinal", (E.D_BAD_MIDDLE,))]
        check("bad_middle:surviving persons", persons, E.E_BAD_MIDDLE_PERSONS)
        # the rejected root left nothing: no row anywhere with root_index 1
        for table in ("participants", "visits", "readings", "consents",
                      "tags", "subs"):
            n = conn.execute(
                'SELECT COUNT(*) FROM "%s" WHERE source_digest=? '
                "AND root_index=1" % table, (E.D_BAD_MIDDLE,)).fetchone()[0]
            check("bad_middle:%s root 1 rows gone" % table, n, 0)
        detail = conn.execute(
            "SELECT detail_json, path FROM ns_roots WHERE source_digest=? "
            "AND root_index=1", (E.D_BAD_MIDDLE,)).fetchone()
        check("bad_middle:path is identifier-only",
              detail["path"], "/person")
        payload = json.loads(detail["detail_json"])
        blob = json.dumps(payload)
        check("bad_middle:no payload value in diagnostic",
              ("R1b" in blob or "R1" in blob), False)
    finally:
        conn.close()


def cp_bad_type(db):
    conn = connect(db)
    try:
        check_counts(conn, E.E_BAD_TYPE_COUNTS, "bad_type", E.D_BAD_TYPE)
        check_root_records(conn, E.E_BAD_TYPE_ROOTS, "bad_type", E.D_BAD_TYPE)
        persons = [r[0] for r in conn.execute(
            "SELECT person FROM participants WHERE source_digest=? "
            "ORDER BY ordinal", (E.D_BAD_TYPE,))]
        check("bad_type:surviving persons", persons, E.E_BAD_TYPE_PERSONS)
        blob = json.dumps(dict(conn.execute(
            "SELECT detail_json FROM ns_roots WHERE source_digest=? "
            "AND root_index=1", (E.D_BAD_TYPE,)).fetchone()))
        check("bad_type:no payload in diagnostic", "7" in blob, False)
    finally:
        conn.close()


def cp_truncated(db):
    conn = connect(db)
    try:
        check_counts(conn, E.E_TRUNCATED_COUNTS, "truncated", E.D_TRUNCATED)
        check_root_records(conn, E.E_TRUNCATED_ROOTS, "truncated",
                           E.D_TRUNCATED)
        persons = [r[0] for r in conn.execute(
            "SELECT person FROM participants WHERE source_digest=? "
            "ORDER BY ordinal", (E.D_TRUNCATED,))]
        check("truncated:surviving persons", persons, E.E_TRUNCATED_PERSONS)
        run = run_record(conn, E.D_TRUNCATED)
        check("truncated:status", run["status"], "aborted_syntax")
        check("truncated:category", run["error_category"], "json_syntax")
        check("truncated:position at end of file", run["error_position"],
              E.E_TRUNCATED_POSITION)
        check("truncated:position basis", run["error_position_basis"],
              "end_of_file")
        # the truncated root's half tree must not exist: there is no root 1
        n = conn.execute(
            "SELECT COUNT(*) FROM ns_roots WHERE source_digest=? "
            "AND root_index=1", (E.D_TRUNCATED,)).fetchone()[0]
        check("truncated:no record for the truncated root", n, 0)
    finally:
        conn.close()


def cp_resume(db):
    """After a forced exit and a resume, nothing is duplicated."""
    conn = connect(db)
    try:
        check_tables(conn, E.E_RESUME, "resume")
        check_counts(conn, E.E_RESUME_COUNTS, "resume", E.D_RESUME)
        statuses = [dict(r) for r in conn.execute(
            "SELECT root_index, status, category FROM ns_roots "
            "WHERE source_digest=? ORDER BY root_index", (E.D_RESUME,))]
        check("resume:root records", statuses, E.E_RESUME_ROOTS)
        run = run_record(conn, E.D_RESUME)
        check("resume:status", run["status"], "complete")
        check("resume:committed count", run["roots_committed"],
              len(E.E_RESUME_ROOTS))
    finally:
        conn.close()


def cp_plan_mismatch(db):
    """The output database must still be bound to the original plan."""
    conn = connect(db)
    try:
        check("db:plan digest", conn.execute(
            "SELECT value FROM meta WHERE key='plan_digest'").fetchone()[0],
            E.PLAN_DIGEST)
    finally:
        conn.close()


def cp_big(db):
    conn = connect(db)
    try:
        expected = E.BIG_EXPECT
        counts = {}
        for table in expected["counts"]:
            counts[table] = conn.execute(
                'SELECT COUNT(*) FROM "%s"' % table).fetchone()[0]
        check("big:row counts", counts, expected["counts"])
        first = conn.execute(
            'SELECT code, value, "value__present" FROM readings '
            "ORDER BY ordinal LIMIT 1").fetchone()
        check("big:first reading", tuple(first), ("C00000", "0", 1))
        last = conn.execute(
            'SELECT code, value, "value__present" FROM readings '
            "ORDER BY ordinal DESC LIMIT 1").fetchone()
        check("big:last reading", tuple(last), expected["last_reading"])
        # every reading belongs to some visit, and the last large visit of
        # root 1 owns exactly BIG_READINGS of them
        orphan = conn.execute(
            "SELECT COUNT(*) FROM readings r LEFT JOIN visits v "
            "ON r.parent_row_id = v.row_id WHERE v.row_id IS NULL"
        ).fetchone()[0]
        check("big:no orphan readings", orphan, 0)
        big_visit = conn.execute(
            "SELECT v.row_id, COUNT(r.row_id) c FROM visits v "
            "JOIN participants p ON v.parent_row_id = p.row_id "
            "JOIN readings r ON r.parent_row_id = v.row_id "
            "WHERE p.person='B1' GROUP BY v.row_id "
            "ORDER BY v.ordinal DESC LIMIT 1").fetchone()
        check("big:last large visit owns %d readings" % E.BIG_READINGS,
              big_visit["c"], E.BIG_READINGS)
        check("big:one root record per source root", conn.execute(
            "SELECT COUNT(*) FROM ns_roots").fetchone()[0], E.BIG_ROOTS)
    finally:
        conn.close()


CHECKPOINTS = {
    "example": cp_example,
    "late": cp_late,
    "variants": cp_variants,
    "bad_middle": cp_bad_middle,
    "bad_type": cp_bad_type,
    "truncated": cp_truncated,
    "resume": cp_resume,
    "plan_mismatch": cp_plan_mismatch,
    "big": cp_big,
}


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--database", required=True)
    ap.add_argument("--checkpoint", required=True, choices=sorted(CHECKPOINTS))
    args = ap.parse_args(argv)
    CHECKPOINTS[args.checkpoint](args.database)
    if FAILURES:
        print("\n%d FAILURES" % len(FAILURES), file=sys.stderr)
        return 1
    print("checkpoint %s verified" % args.checkpoint)
    return 0


if __name__ == "__main__":
    sys.exit(main())
