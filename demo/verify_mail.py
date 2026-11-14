#!/usr/bin/env python3
"""Independent verifier for the mail-channel demo.

Reads the store's SQLite database DIRECTLY (its own SQL, no rvault code)
and compares editions, batches, change-feed events and the mail processing
record field by field against the hand-written expectations in
expected_mail.py.  Exits non-zero on any mismatch.

Usage: python3 verify_mail.py --store STORE --checkpoint NAME
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import expected_mail as E  # noqa: E402

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


def editions(conn):
    return [tuple(r) for r in conn.execute(
        "SELECT study, observation, edition, person, display_name, measure, "
        "memo, batch_tag FROM editions ORDER BY study, observation, edition")]


def batches(conn):
    return sorted((r["batch_tag"], r["digest"]) for r in conn.execute(
        "SELECT batch_tag, digest FROM batches"))


def events(conn):
    return [tuple(r) for r in conn.execute(
        "SELECT seq, frame_id, op, study, observation FROM cf_events "
        "ORDER BY seq")]


def ml_messages(conn):
    return {r["message_digest"]: dict(r) for r in conn.execute(
        "SELECT * FROM ml_messages")}


def ml_batches(conn, digest):
    return [(int(r["seq"]), r["batch_tag"], r["content_id"], r["part"],
             r["sha256"], r["status"]) for r in conn.execute(
                "SELECT * FROM ml_batches WHERE message_digest=? ORDER BY seq",
                (digest,))]


def one_message(conn):
    msgs = ml_messages(conn)
    if len(msgs) != 1:
        check("exactly one ml_messages row", len(msgs), 1)
        return None, None
    digest = next(iter(msgs))
    return digest, msgs[digest]


def check_common_main(store):
    conn = connect(store)
    try:
        check("editions", editions(conn), E.M1_EDITIONS)
        check("batches", batches(conn),
              sorted((t, s) for t, _, s in E.M1_BATCHES))
        check("cf_events", events(conn), E.M1_EVENTS)
        digest, msg = one_message(conn)
        if msg is not None:
            check("message state", msg["state"], "done")
            check("manifest part", msg["manifest_part"], E.M1_MANIFEST_PART)
            check("part count", msg["part_count"], E.M1_PART_COUNT)
            check("batch count", msg["batch_count"], 2)
            check("counts", (msg["accepted_count"], msg["duplicate_count"],
                             msg["rejected_count"]), (2, 0, 0))
            check("ml_batches", ml_batches(conn, digest),
                  [(1, "mail-b1", "data-1@rvault", "3", E.B1_SHA, "accepted"),
                   (2, "mail-b2", "data-2@rvault", "4", E.B2_SHA, "accepted")])
    finally:
        conn.close()


def cp_main(store):
    check_common_main(store)


def cp_partial(store):
    conn = connect(store)
    try:
        check("editions", editions(conn), E.PARTIAL_EDITIONS)
        check("batches", batches(conn),
              sorted((t, s) for t, _, s in E.PARTIAL_BATCHES
                     if t in ("pb1", "pb3")))
        check("cf_events", events(conn), E.PARTIAL_EVENTS)
        digest, msg = one_message(conn)
        if msg is not None:
            check("message state", msg["state"], "done")
            check("counts", (msg["accepted_count"], msg["duplicate_count"],
                             msg["rejected_count"]), (2, 0, 1))
            rows = ml_batches(conn, digest)
            check("ml_batches statuses", [r[5] for r in rows],
                  E.PARTIAL_STATUSES)
            check("ml_batches tags", [r[1] for r in rows],
                  ["pb1", "pb2", "pb3"])
            detail = conn.execute(
                "SELECT detail_json FROM ml_batches WHERE message_digest=? "
                "AND seq=2", (digest,)).fetchone()["detail_json"]
            detail = json.loads(detail)
            check("rejected batch status", detail["status"], "rejected")
            errs = detail["errors"]
            check("rejected batch error", errs,
                  [{"line": 2,
                    "reason": "field edition must be a positive integer"}])
            check("no payload in detail",
                  E.P_SECRET_NAME in json.dumps(detail)
                  or E.P_SECRET_MEMO in json.dumps(detail), False)
    finally:
        conn.close()


def cp_crashed(store):
    """State right after the SIGKILL: batch c1 committed, record pending."""
    conn = connect(store)
    try:
        check("editions after crash", editions(conn), E.CRASH_EDITIONS[:1])
        check("batches after crash", batches(conn),
              [("c1", E.CRASH_BATCHES[0][2])])
        check("cf_events after crash", events(conn), E.CRASH_EVENTS[:1])
        digest, msg = one_message(conn)
        if msg is not None:
            check("message state after crash", msg["state"], "importing")
            check("ml_batches after crash",
                  [r[5] for r in ml_batches(conn, digest)],
                  ["pending", "pending", "pending"])
    finally:
        conn.close()


def cp_resumed(store):
    conn = connect(store)
    try:
        check("editions", editions(conn), E.CRASH_EDITIONS)
        check("batches", batches(conn),
              sorted((t, s) for t, _, s in E.CRASH_BATCHES))
        check("cf_events", events(conn), E.CRASH_EVENTS)
        digest, msg = one_message(conn)
        if msg is not None:
            check("message state", msg["state"], "done")
            check("counts", (msg["accepted_count"], msg["duplicate_count"],
                             msg["rejected_count"]), (2, 1, 0))
            check("ml_batches statuses",
                  [r[5] for r in ml_batches(conn, digest)],
                  E.RESUMED_STATUSES)
    finally:
        conn.close()


def cp_errors(store):
    conn = connect(store)
    try:
        check("no editions", editions(conn), [])
        check("no batches", batches(conn), [])
        check("no events", events(conn), [])
        msgs = ml_messages(conn)
        check("message count", len(msgs), len(E.ERROR_CASES))
        by_digest = {}
        for digest, msg in msgs.items():
            errs = json.loads(msg["errors_json"]) if msg["errors_json"] else []
            by_digest[digest] = (msg, errs)
            check("message state", msg["state"], "rejected")
        # the demo records, per case name, the digest it received; match by
        # the errors file the demo wrote
        cases_path = os.path.join(store, "..", "error_cases.json")
        with open(cases_path, encoding="utf-8") as f:
            cases = json.load(f)
        for name, digest in sorted(cases.items()):
            msg, errs = by_digest[digest]
            check("%s categories" % name, [e["category"] for e in errs],
                  E.ERROR_CASES[name])
            if name in E.ERROR_PARTS:
                check("%s first part" % name, errs[0].get("part"),
                      E.ERROR_PARTS[name])
            check("%s no payload leak" % name,
                  E.ERR_SECRET in json.dumps(errs), False)
    finally:
        conn.close()


def cp_safe(store):
    conn = connect(store)
    try:
        check("editions", editions(conn),
              [("ms", "safe1", 1, "ps-1", "Safe", 1, "", "safe-b1")])
        check("batches", batches(conn),
              [("safe-b1", E.SAFE_BATCHES[0][2])])
        digest, msg = one_message(conn)
        if msg is not None:
            check("message state", msg["state"], "done")
            check("ml_batches statuses",
                  [r[5] for r in ml_batches(conn, digest)], ["accepted"])
    finally:
        conn.close()


def cp_big(store):
    conn = connect(store)
    try:
        n = conn.execute("SELECT COUNT(*) AS c FROM editions").fetchone()["c"]
        check("big editions count", n, E.BIG_N)
        row = conn.execute("SELECT digest FROM batches WHERE batch_tag='bigb'"
                           ).fetchone()
        msgs = ml_messages(conn)
        digest = next(iter(msgs)) if len(msgs) == 1 else None
        mlsha = None
        if digest is not None:
            mlsha = conn.execute(
                "SELECT sha256 FROM ml_batches WHERE message_digest=?",
                (digest,)).fetchone()["sha256"]
        check("big batch digest == manifest digest",
              None if row is None else row["digest"], mlsha)
        check("big message state",
              None if digest is None else msgs[digest]["state"], "done")
    finally:
        conn.close()


CHECKPOINTS = {
    "main": cp_main,
    "partial": cp_partial,
    "crashed": cp_crashed,
    "resumed": cp_resumed,
    "errors": cp_errors,
    "safe": cp_safe,
    "big": cp_big,
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--store", required=True)
    ap.add_argument("--checkpoint", required=True, choices=sorted(CHECKPOINTS))
    args = ap.parse_args()
    CHECKPOINTS[args.checkpoint](args.store)
    if FAILURES:
        print("VERIFY FAILED: %s" % ", ".join(FAILURES))
        return 1
    print("checkpoint %s: all ok" % args.checkpoint)
    return 0


if __name__ == "__main__":
    sys.exit(main())
