#!/usr/bin/env python3
"""Independent verifier for the rvault demo.

Reads the store's SQLite database and bundle/receipt files DIRECTLY (its own
SQL, no rvault code) and compares them field by field against the
hand-written expectations in expected.py.  Exits non-zero on any mismatch.

Usage: python3 verify.py --store STORE --checkpoint cpN
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import expected as E  # noqa: E402

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


def fetch_editions(conn):
    return [tuple(r) for r in conn.execute(
        "SELECT study, observation, edition, person, display_name, measure, memo, "
        "batch_tag FROM editions ORDER BY study, observation, edition")]


def fetch_owners(conn):
    return [tuple(r) for r in conn.execute(
        "SELECT study, observation, person FROM observation_owner "
        "ORDER BY study, observation")]


def fetch_projection(conn, study):
    return [tuple(r) for r in conn.execute(
        "SELECT e.observation, e.person, e.edition, e.display_name, e.measure, e.memo "
        "FROM editions e "
        "WHERE e.study = ? AND e.edition = ("
        "  SELECT MAX(edition) FROM editions e2 "
        "  WHERE e2.study = e.study AND e2.observation = e.observation) "
        "AND NOT EXISTS (SELECT 1 FROM barrier b "
        "                WHERE b.study = e.study AND b.person = e.person) "
        "ORDER BY e.observation", (study,))]


def fetch_barrier(conn):
    return [tuple(r) for r in conn.execute(
        "SELECT study, person FROM barrier ORDER BY study, person")]


def fetch_withdrawals(conn):
    return {r["withdrawal_id"]: (r["study"], r["state"], r["editions_removed"])
            for r in conn.execute(
                "SELECT withdrawal_id, study, state, editions_removed FROM withdrawals")}


def fetch_withdrawal_bundles(conn):
    out = {}
    for r in conn.execute(
            "SELECT withdrawal_id, bundle_id, state FROM withdrawal_bundles "
            "ORDER BY withdrawal_id, bundle_id"):
        out.setdefault(r["withdrawal_id"], {})[r["bundle_id"]] = r["state"]
    return out


def fetch_bundles_table(conn):
    return [tuple(r) for r in conn.execute(
        "SELECT bundle_id, study, state FROM bundles ORDER BY bundle_id")]


def fetch_batches(conn):
    return {r["batch_tag"]: (r["accepted_count"], r["suppressed_count"])
            for r in conn.execute(
                "SELECT batch_tag, accepted_count, suppressed_count FROM batches")}


def receipt_path(store, tag):
    safe = "".join(c if (c.isalnum() or c in "._-") else "_" for c in tag) or "_"
    name = "%s-%s.jsonl" % (safe, hashlib.sha256(tag.encode("utf-8")).hexdigest()[:8])
    return os.path.join(store, "receipts", name)


def fetch_receipt(store, tag):
    items = []
    with open(receipt_path(store, tag), encoding="utf-8") as f:
        for line in f:
            d = json.loads(line)
            items.append((d["line"], d["study"], d["observation"],
                          d["person"], d["edition"], d["disposition"]))
    return items


def check_bundle_file(store, bundle_id, exp):
    path = os.path.join(store, "bundles", bundle_id + ".json")
    if not os.path.exists(path):
        check("bundle_file %s exists" % bundle_id, False, True)
        return
    with open(path, encoding="utf-8") as f:
        actual = json.load(f)
    ts = actual.pop("created_at", None)
    check("bundle_file %s created_at format" % bundle_id,
          bool(isinstance(ts, str) and
               re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$", ts)), True)
    check("bundle_file %s content" % bundle_id, actual, exp)


def residue_check(store, conn, spec):
    forbidden = spec["forbidden"]
    hits = []
    tables = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")]
    for t in tables:
        for row in conn.execute('SELECT * FROM "%s"' % t):
            for v in row:
                if isinstance(v, str):
                    for s in forbidden:
                        if s in v:
                            hits.append((t, s))
    check("residue: database tables free of withdrawn payload", hits, [])

    file_hits = []
    for sub in ("bundles", "receipts"):
        root = os.path.join(store, sub)
        for dirpath, _dirs, files in os.walk(root):
            for fn in files:
                with open(os.path.join(dirpath, fn), encoding="utf-8",
                          errors="replace") as f:
                    data = f.read()
                for s in forbidden:
                    if s in data:
                        file_hits.append((os.path.join(sub, fn), s))
    check("residue: bundle/receipt files free of withdrawn payload", file_hits, [])

    names = {r[0] for r in conn.execute("SELECT display_name FROM editions")}
    for s in spec.get("required_names", []):
        check("residue: untouched participant %r still present" % s, s in names, True)

    persons = spec.get("withdrawn_persons", [])
    if persons:
        q = "SELECT COUNT(*) FROM editions WHERE person IN (%s)" % ",".join(
            "?" * len(persons))
        cnt = conn.execute(q, persons).fetchone()[0]
        check("residue: zero edition rows for withdrawn persons", cnt, 0)


def run_checkpoint(store, cp) -> bool:
    spec = E.CHECKPOINTS[cp]
    conn = connect(store)
    try:
        if "editions" in spec:
            check("editions", fetch_editions(conn), [tuple(t) for t in spec["editions"]])
        if "owners" in spec:
            check("owners", fetch_owners(conn), [tuple(t) for t in spec["owners"]])
        for key in sorted(k for k in spec if k.startswith("projection_")):
            study = key[len("projection_"):]
            check(key, fetch_projection(conn, study),
                  [tuple(t) for t in spec[key]])
        if "barrier" in spec:
            check("barrier", fetch_barrier(conn), [tuple(t) for t in spec["barrier"]])
        if "withdrawals" in spec:
            check("withdrawals", fetch_withdrawals(conn), spec["withdrawals"])
        if "withdrawal_anomalies" in spec:
            actual = {r["withdrawal_id"]: r["anomalies"] for r in conn.execute(
                "SELECT withdrawal_id, anomalies FROM withdrawals")}
            for wid, n in spec["withdrawal_anomalies"].items():
                check("withdrawal anomalies %s" % wid, actual.get(wid), n)
        if "withdrawal_bundles" in spec:
            actual = fetch_withdrawal_bundles(conn)
            for wid, exp_map in spec["withdrawal_bundles"].items():
                check("withdrawal_bundles %s" % wid, actual.get(wid, {}), exp_map)
        if "bundles_table" in spec:
            check("bundles_table", fetch_bundles_table(conn),
                  [tuple(t) for t in spec["bundles_table"]])
        for bid, content in sorted(spec.get("bundle_files", {}).items()):
            check_bundle_file(store, bid, content)
        for bid in spec.get("files_absent", []):
            check("file absent %s" % bid,
                  os.path.exists(os.path.join(store, "bundles", bid + ".json")), False)
        for tag, items in sorted(spec.get("receipts", {}).items()):
            check("receipt %s" % tag, fetch_receipt(store, tag),
                  [tuple(t) for t in items])
        if "batches" in spec:
            check("batches", fetch_batches(conn), spec["batches"])
        for tag in spec.get("batches_absent", []):
            check("batch absent %s" % tag, tag in fetch_batches(conn), False)
        if spec.get("tmp_empty"):
            check("tmp dir empty",
                  os.listdir(os.path.join(store, "tmp")), [])
        if "residue" in spec:
            residue_check(store, conn, spec["residue"])
    finally:
        conn.close()
    return not FAILURES


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--store", required=True)
    ap.add_argument("--checkpoint", required=True, choices=sorted(E.CHECKPOINTS))
    args = ap.parse_args()
    ok = run_checkpoint(args.store, args.checkpoint)
    if ok:
        print("checkpoint %s: ALL CHECKS PASSED" % args.checkpoint)
        return 0
    print("checkpoint %s: %d CHECK(S) FAILED" % (args.checkpoint, len(FAILURES)))
    return 1


if __name__ == "__main__":
    sys.exit(main())
