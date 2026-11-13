#!/usr/bin/env python3
"""Independent verifier for the change-feed demo.

Reads the store's SQLite database with its OWN SQL (no rvault code) and
compares frames, events, consumer positions, pending deliveries and the
retention floor against the hand-written literals in expected_feed.py.

Usage:
  verify_feed.py --store STORE --db-checkpoint NAME
  verify_feed.py --store STORE --pull-json FILE --expected DELIVERY_KEY
"""
from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import expected_feed as E  # noqa: E402

FAILURES = []


def check(name, actual, exp):
    if actual != exp:
        FAILURES.append(name)
        print("FAIL %s\n  expected: %r\n  actual:   %r" % (name, exp, actual))
    else:
        print("ok   %s" % name)


# ---------------------------------------------------------------- direct SQL
def connect(store):
    import sqlite3
    conn = sqlite3.connect(os.path.join(store, "vault.sqlite3"))
    conn.row_factory = sqlite3.Row
    return conn


def fetch_frames(conn):
    return [(r["frame_id"], r["start_seq"], r["end_seq"], r["event_count"])
            for r in conn.execute(
                "SELECT frame_id, start_seq, end_seq, event_count FROM cf_frames "
                "ORDER BY frame_id")]


def fetch_events(conn):
    return [(r["seq"], r["frame_id"], r["ordinal"], r["op"], r["observation"])
            for r in conn.execute(
                "SELECT seq, frame_id, ordinal, op, observation FROM cf_events "
                "ORDER BY seq")]


def fetch_consumers(conn):
    return {r["name"]: {"generation": r["generation"],
                        "start_seq": r["start_seq"],
                        "acked_seq": r["acked_seq"],
                        "enabled": r["disabled_at"] is None}
            for r in conn.execute(
                "SELECT name, generation, start_seq, acked_seq, disabled_at "
                "FROM cf_consumers ORDER BY name")}


def fetch_meta(conn):
    d = {r["name"]: r["value"] for r in conn.execute("SELECT name, value FROM cf_meta")}
    return d


def fetch_pending(conn, expect_tokens=None):
    """Pending deliveries.  A token is an opaque credential: when the spec
    supplies a token it is compared, otherwise only the range is checked."""
    exp = expect_tokens or {}
    out = {}
    for r in conn.execute("SELECT consumer, delivery_id, from_seq, through_seq "
                          "FROM cf_deliveries WHERE state='pending'"):
        entry = {"from_seq": r["from_seq"], "through_seq": r["through_seq"]}
        if r["consumer"] in exp and exp[r["consumer"]].get("token") is not None:
            entry["token"] = r["delivery_id"]
        out[r["consumer"]] = entry
    return out


def fetch_removed(conn):
    return [(r["study"], r["observation"]) for r in conn.execute(
        "SELECT study, observation FROM cf_removed_objects ORDER BY study, observation")]


# ---------------------------------------------------------------- expectations
def expect_state(conn, spec, label=""):
    if "frames" in spec:
        check(label + "frames", fetch_frames(conn), [tuple(x) for x in spec["frames"]])
    if "events" in spec:
        check(label + "events", fetch_events(conn), [tuple(x) for x in spec["events"]])
    if "consumers" in spec:
        actual = fetch_consumers(conn)
        for name, exp in spec["consumers"].items():
            check(label + "consumer " + name, actual.get(name), exp)
        if "no_extra_consumers" in spec:
            check(label + "consumer set", sorted(actual),
                  sorted(spec["consumers"]))
    if "retention_floor" in spec or "tail_seq" in spec:
        meta = fetch_meta(conn)
        if "retention_floor" in spec:
            check(label + "retention floor", meta.get("retention_floor", 0),
                  spec["retention_floor"])
        if "tail_seq" in spec:
            check(label + "tail seq", meta.get("head_seq", 0), spec["tail_seq"])
    if "pending" in spec:
        check(label + "pending deliveries",
              fetch_pending(conn, spec["pending"]), spec["pending"])
    if "removed" in spec:
        check(label + "removed object set", fetch_removed(conn),
              [tuple(x) for x in spec["removed"]])
    if "enabled" in spec:
        check(label + "enabled consumers",
              sorted(n for n, c in fetch_consumers(conn).items() if c["enabled"]),
              sorted(spec["enabled"]))


# ---------------------------------------------------------------- pull check
def parse_pull(text):
    """Parse the line protocol strictly; raises on structural truncation."""
    lines = [ln.rstrip("\n") for ln in text.splitlines()]
    if not lines:
        raise AssertionError("empty pull output")
    start = lines[0].split()
    assert start[0] == "START", "first line is not START: %r" % lines[0]
    kv = dict(x.split("=", 1) for x in start[1:])
    end = lines[-1].split()
    assert end[0] == "END", "last line is not END (truncated output?)"
    endkv = dict(x.split("=", 1) for x in end[1:])
    frames, events = [], []
    cur = None
    for ln in lines[1:-1]:
        p = ln.split()
        if p[0] == "FRAME":
            d = dict(x.split("=", 1) for x in p[1:])
            assert int(d["index"]) == len(frames) + 1, "frame index gap"
            cur = {"fid": int(d["id"]), "start": int(d["start"]),
                   "end": int(d["end"]), "count": int(d["events"]),
                   "ts": d["ts"], "seen": 0, "first": None, "last": None}
        elif p[0] == "EVENT":
            d = dict(x.split("=", 1) for x in p[1:])
            assert cur is not None, "EVENT outside a frame"
            assert int(d["frame"]) == cur["fid"], "event frame mismatch"
            assert int(d["seq"]) == (cur["last"] or cur["start"] - 1) + 1, \
                "seq gap inside frame"
            cur["seen"] += 1
            cur["first"] = cur["first"] or int(d["seq"])
            cur["last"] = int(d["seq"])
            assert d["op"] in ("refresh", "remove"), "bad op"
            events.append((int(d["seq"]), cur["fid"], int(d["ordinal"]),
                           d["op"], d["observation"]))
        elif p[0] == "ENDFRAME":
            d = dict(x.split("=", 1) for x in p[1:])
            assert cur is not None and int(d["id"]) == cur["fid"], "ENDFRAME mismatch"
            assert int(d["events"]) == cur["seen"], "frame count mismatch"
            assert int(d["first"]) == cur["first"] and int(d["last"]) == cur["last"], \
                "frame first/last mismatch"
            frames.append({"fid": cur["fid"], "start": cur["start"],
                           "end": cur["end"], "count": cur["count"]})
            cur = None
        elif p[0] in ("START", "END"):
            raise AssertionError("nested %s marker" % p[0])
        else:
            raise AssertionError("unknown protocol line: %r" % ln)
    assert cur is None, "delivery truncated inside a frame"
    assert int(endkv["events"]) == len(events), "END event count mismatch"
    assert int(endkv["frames"]) == len(frames), "END frame count mismatch"
    return kv, frames, events, endkv


def check_pull(text, key):
    """Compare one pull against expected_feed.MAIN_DELIVERIES[key]."""
    exp = E.MAIN_DELIVERIES[key]
    kv, frames, events, endkv = parse_pull(text)
    if exp is None:
        check("pull %s: empty" % key, kv.get("empty"), "1")
        return
    from_seq, through, count, exp_frames = exp
    check("pull %s from" % key, kv.get("from"), str(from_seq))
    check("pull %s through" % key, kv.get("through"), str(through))
    check("pull %s frame count" % key, int(kv["frames"]), len(exp_frames))
    check("pull %s events" % key, int(kv["events"]), count)
    check("pull %s end events" % key, int(endkv["events"]), count)
    got = [(f["fid"], f["start"], f["end"], f["count"]) for f in frames]
    check("pull %s frames" % key, got, [tuple(x) for x in exp_frames])
    exp_events = []
    for fid, start, _end, _n in exp_frames:
        for i, (op, obs) in enumerate(E.MAIN_FRAMES[fid], 1):
            exp_events.append((start + i - 1, fid, i, op, obs))
    check("pull %s event list" % key, events, exp_events)


def check_pull_edge(text, key):
    """Compare one edge-store pull against EDGE_DELIVERIES[key]
    (from, through, events, [(fid,start,end,count)])."""
    exp = E.EDGE_DELIVERIES[key]
    kv, frames, events, endkv = parse_pull(text)
    if exp is None:
        check("edge pull %s: empty" % key, kv.get("empty"), "1")
        return
    from_seq, through, count, exp_frames = exp
    check("edge pull %s from" % key, kv.get("from"), str(from_seq))
    check("edge pull %s through" % key, kv.get("through"), str(through))
    got = [(f["fid"], f["start"], f["end"], f["count"]) for f in frames]
    check("edge pull %s frames" % key, got, [tuple(x) for x in exp_frames])
    exp_events = []
    for fid, start, _end, _n in exp_frames:
        for i, (op, obs) in enumerate(E.EDGE_FRAMES[fid], 1):
            exp_events.append((start + i - 1, fid, i, op, obs))
    check("edge pull %s event list" % key, events, exp_events)
    check("edge pull %s end" % key, int(endkv["events"]), count)


# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--store", required=True)
    ap.add_argument("--db-checkpoint")
    ap.add_argument("--pull-json")
    ap.add_argument("--expected")
    ap.add_argument("--legacy", action="store_true")
    ap.add_argument("--edge", action="store_true")
    args = ap.parse_args()
    conn = connect(args.store)
    try:
        if args.db_checkpoint:
            if args.edge:
                spec = E.EDGE_CHECKPOINTS[args.db_checkpoint]
            else:
                spec = E.DB_CHECKPOINTS[args.db_checkpoint]
            if "retained_frames" in spec:
                check("retained frames", fetch_frames(conn),
                      [tuple(x) for x in spec["retained_frames"]])
            if "trimmed_frames" in spec:
                pass  # asserted by the driver from the trim output
            if "refused_consumer_absent" in spec:
                check("refused consumer absent",
                      spec["refused_consumer_absent"] in fetch_consumers(conn), False)
            expect_state(conn, spec)
        if args.pull_json:
            with open(args.pull_json, encoding="utf-8") as f:
                text = f.read()
            if args.edge:
                check_pull_edge(text, args.expected)
            else:
                check_pull(text, args.expected)
        if args.legacy:
            check("legacy events", fetch_events(conn),
                  [tuple(x) for x in E.LEGACY_IMPORT_EVENTS])
            check("legacy frames", fetch_frames(conn), [(1, 1, 1, 1)])
            check("legacy tail", fetch_meta(conn).get("head_seq", 0), 1)
            # only the post-install write is announced; oba has no events and
            # obb's pre-existing editions e1/e2 produced none either
            check("legacy event observations",
                  sorted({e[4] for e in fetch_events(conn)}), ["obb"])
            check("legacy event count covers only the new edition",
                  len(fetch_events(conn)), 1)
            expect_state(conn, E.LEGACY_CHECKPOINT)
    finally:
        conn.close()
    return 0 if not FAILURES else 1


if __name__ == "__main__":
    sys.exit(main())
