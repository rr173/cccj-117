#!/usr/bin/env python3
"""Independent verifier for the versioned input-contract demo.

Reads the store's SQLite database and receipt files DIRECTLY (its own SQL, no
rvault code) and compares them field by field against the hand-written
expectations in expected_contracts.py.  Exits non-zero on any mismatch.

Usage: python3 verify_contracts.py --store STORE --checkpoint NAME
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import expected_contracts as E  # noqa: E402

FAILURES = []


def check(name, actual, exp):
    if actual != exp:
        FAILURES.append(name)
        print("FAIL %s\n  expected: %r\n  actual:   %r" % (name, exp, actual))
    else:
        print("ok   %s" % name)


def connect(store):
    conn = sqlite3.connect(os.path.join(store, "vault.sqlite3"))
    conn.row_factory = sqlite3.Row
    return conn


def fetch_contracts(conn):
    return [(r["provider"], r["material_type"], r["version"],
             r["predecessor"], r["compatibility"])
            for r in conn.execute(
                "SELECT provider, material_type, version, predecessor, "
                "compatibility FROM ct_contracts "
                "ORDER BY provider, material_type, version")]


def fetch_fields(conn, provider, material_type, version):
    rows = []
    for r in conn.execute(
            "SELECT name, type, required, nullable, default_json, target, position "
            "FROM ct_fields WHERE provider=? AND material_type=? AND version=? "
            "ORDER BY position", (provider, material_type, version)):
        raw = r["default_json"]
        rows.append((r["name"], r["type"], r["required"], r["nullable"],
                     0 if raw is None else 1,
                     None if raw is None else json.loads(raw), r["target"]))
    return rows


def fetch_editions(conn):
    return [tuple(r) for r in conn.execute(
        "SELECT study, observation, edition, person, display_name, measure, memo, "
        "batch_tag FROM editions ORDER BY study, observation, edition")]


def fetch_batches(conn):
    return sorted((r["batch_tag"], r["protocol"], r["provider"],
                   r["material_type"], r["contract_version"])
                  for r in conn.execute(
                      "SELECT batch_tag, protocol, provider, material_type, "
                      "contract_version FROM ct_batches"))


def receipt_path(store, tag):
    safe = "".join(c if (c.isalnum() or c in "._-") else "_" for c in tag) or "_"
    return os.path.join(store, "receipts",
                        "%s-%s.jsonl" % (safe, hashlib.sha256(tag.encode()).hexdigest()[:8]))


def fetch_feed(conn):
    refresh = {(r["study"], r["observation"]) for r in conn.execute(
        "SELECT study, observation FROM cf_events WHERE op='refresh'")}
    removed = {(r["study"], r["observation"]) for r in conn.execute(
        "SELECT study, observation FROM cf_events WHERE op='remove'")}
    return refresh, removed


def count(conn, table):
    return conn.execute("SELECT COUNT(*) FROM " + table).fetchone()[0]


def run(store, cp):
    conn = connect(store)
    try:
        if cp == "cp_initial":
            check("initial contract versions", fetch_contracts(conn),
                  [("labA", "obs", 1, None, "initial"),
                   ("labB", "measurement", 1, None, "initial")])
            check("labA v1 field rows", fetch_fields(conn, "labA", "obs", 1),
                  E.CONTRACT_FIELDS[("labA", "obs", 1)])
            check("labB v1 field rows",
                  fetch_fields(conn, "labB", "measurement", 1),
                  E.CONTRACT_FIELDS[("labB", "measurement", 1)])
            check("no batches recorded yet", count(conn, "ct_batches"), 0)

        elif cp == "cp_register":
            # The demo later publishes a private labRej clone (v1+v2) to run
            # the incompatible-change battery; it is not part of the tower
            # and may or may not exist yet at this checkpoint.
            check("contract versions", [c for c in fetch_contracts(conn)
                                        if c[0] != "labRej"],
                  [("labA", "obs", 1, None, "initial"),
                   ("labA", "obs", 2, 1, "compatible"),
                   ("labA", "obs", 3, 2, "branch"),
                   ("labB", "measurement", 1, None, "initial"),
                   ("labB", "measurement", 2, 1, "compatible")])
            for key, fields in sorted(E.CONTRACT_FIELDS.items()):
                check("fields %s" % (key,), fetch_fields(conn, *key), fields)
            check("no batches recorded yet", count(conn, "ct_batches"), 0)

        elif cp == "cp_import":
            check("accepted stored rows", fetch_editions(conn),
                  sorted(E.LABA_GOOD_ROWS + E.LABB_GOOD_ROWS))
            check("batch protocols", fetch_batches(conn), [
                ("a-good", "contract", "labA", "obs", 2),
                ("b-good", "contract", "labB", "measurement", 2)])
            # Fractional measure is preserved exactly (never rewritten).
            frac = conn.execute(
                "SELECT measure FROM editions WHERE study='laba' AND observation='L3'"
            ).fetchone()[0]
            check("fractional measure stored unchanged", frac, 10.5)
            ints = [r[0] for r in conn.execute(
                "SELECT measure FROM editions WHERE observation IN ('L1','L2') "
                "ORDER BY observation, edition")]
            check("integer measures stay integers", ints, [10, 11, 20])
            refresh, removed = fetch_feed(conn)
            check("refresh notifications", sorted(refresh),
                  sorted([o for o in E.EXPECTED_FEED_REFRESH
                          if o[0] not in ("legacy",) and o != ("laba", "B1")]))
            check("no remove notifications yet", removed, set())

        elif cp == "cp_rejected":
            # The mixed batch left no row, no batch, no receipt, no event.
            # Accepted earlier: a-good (4 rows) and b-good (2 rows).
            check("rejected batch stored nothing", fetch_editions(conn),
                  sorted(E.LABA_GOOD_ROWS + E.LABB_GOOD_ROWS))
            check("rejected batch has no batch row",
                  [b for b in fetch_batches(conn) if b[0] == "mixed-bad"], [])
            check("rejected batch has no receipt file",
                  os.path.exists(receipt_path(store, "mixed-bad")), False)
            refresh, _ = fetch_feed(conn)
            check("rejected batch emitted no notifications", sorted(refresh),
                  sorted([o for o in E.EXPECTED_FEED_REFRESH
                          if o[0] != "legacy" and o != ("laba", "B1")]))
            check("tmp dir empty after rejection",
                  os.listdir(os.path.join(store, "tmp")), [])
            # Rejection leaves the contract tower exactly as registered.
            check("contract tower unchanged by rejections",
                  [c for c in fetch_contracts(conn) if c[0] != "labRej"],
                  [("labA", "obs", 1, None, "initial"),
                   ("labA", "obs", 2, 1, "compatible"),
                   ("labA", "obs", 3, 2, "branch"),
                   ("labB", "measurement", 1, None, "initial"),
                   ("labB", "measurement", 2, 1, "compatible")])

        elif cp == "cp_legacy":
            check("legacy row stored unchanged", fetch_editions(conn),
                  sorted(E.LABA_GOOD_ROWS + E.LABB_GOOD_ROWS + E.LEGACY_ROWS
                         + [E.V3_BRANCH_ROW]))
            check("legacy batch protocol recorded",
                  [b for b in fetch_batches(conn) if b[0] == "legacy-good"],
                  [("legacy-good", "legacy", None, None, None)])
            refresh, removed = fetch_feed(conn)
            check("legacy import emitted refresh too",
                  sorted(refresh), sorted(E.EXPECTED_FEED_REFRESH))
            check("legacy import emitted no remove", removed, set())

        elif cp == "cp_withdrawn":
            # pa-1's two labA editions are erased; the contract metadata and
            # the change feed stay intact, and no notification payload exists.
            # Both editions of L1 belong to pa-1 (both erased); L2/L3 stay.
            check("withdrawn person's editions erased", fetch_editions(conn),
                  sorted([E.LABA_GOOD_ROWS[1], E.LABA_GOOD_ROWS[2]]
                         + E.LABB_GOOD_ROWS + E.LEGACY_ROWS + [E.V3_BRANCH_ROW]))
            check("withdrawals completed",
                  [(r["withdrawal_id"], r["state"], r["editions_removed"])
                   for r in conn.execute(
                       "SELECT withdrawal_id, state, editions_removed FROM withdrawals "
                       "ORDER BY withdrawal_id")], [("W1", "completed", 2)])
            check("barrier registered", [tuple(r) for r in conn.execute(
                "SELECT study, person FROM barrier ORDER BY study, person")],
                [("laba", "pa-1")])
            check("contract tower untouched by withdrawal", fetch_contracts(conn),
                  [("labA", "obs", 1, None, "initial"),
                   ("labA", "obs", 2, 1, "compatible"),
                   ("labA", "obs", 3, 2, "branch"),
                   ("labB", "measurement", 1, None, "initial"),
                   ("labB", "measurement", 2, 1, "compatible"),
                   ("labB", "measurement", 3, 2, "compatible"),
                   ("labRej", "obs", 1, None, "initial"),
                   ("labRej", "obs", 2, 1, "compatible")])
            refresh, removed = fetch_feed(conn)
            check("L1 removed by withdrawal", ("laba", "L1") in removed, True)
            # L1 may keep its HISTORICAL refresh events; a remove dominates
            # and no NEW refresh is ever emitted for it.  Assert both.
            last_refresh = conn.execute(
                "SELECT MAX(seq) FROM cf_events WHERE op='refresh' AND "
                "study='laba' AND observation='L1'").fetchone()[0]
            first_remove = conn.execute(
                "SELECT MIN(seq) FROM cf_events WHERE op='remove' AND "
                "study='laba' AND observation='L1'").fetchone()[0]
            check("no refresh after the removal", last_refresh < first_remove, True)
            check("removed object recorded in cf_removed_objects", 1,
                  conn.execute("SELECT COUNT(*) FROM cf_removed_objects "
                               "WHERE study='laba' AND observation='L1'").fetchone()[0])

    finally:
        conn.close()
    return not FAILURES


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--store", required=True)
    ap.add_argument("--checkpoint", required=True)
    args = ap.parse_args()
    ok = run(args.store, args.checkpoint)
    if ok:
        print("checkpoint %s: ALL CHECKS PASSED" % args.checkpoint)
        return 0
    print("checkpoint %s: %d CHECK(S) FAILED" % (args.checkpoint, len(FAILURES)))
    return 1


if __name__ == "__main__":
    sys.exit(main())
