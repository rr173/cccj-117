"""Command line interface.

  rvault --store S init
  rvault --store S import --batch-tag T [--input FILE]
  rvault --store S query --study X
  rvault --store S history --study X --observation O
  rvault --store S aggregate --study X [--groups groups.json]
  rvault --store S bundle create --study X [--groups groups.json]
  rvault --store S bundle list [--all]
  rvault --store S bundle read --bundle-id B
  rvault --store S withdraw register --withdrawal-id W --study X --person P [--person Q ...]
  rvault --store S withdraw resume (--withdrawal-id W | --all)
  rvault --store S withdraw status [--withdrawal-id W]
  rvault --store S reconcile
  rvault --store S feed register --consumer NAME [--from-seq N] [--reactivate]
  rvault --store S feed pull --consumer NAME [--limit N] [--preview]
  rvault --store S feed ack --consumer NAME --token TOKEN
  rvault --store S feed disable --consumer NAME
  rvault --store S feed status [--consumer NAME]
  rvault --store S feed trim [--through-seq N]

Exit codes: 0 ok; 2 rejected input / conflicting credential, withdrawal, ack or
trim; 3 bundle aborted because a withdrawal landed during creation; 4 unknown or
unreadable object; 5 integrity failure; 6 change-feed start position no longer
retained (gap).
"""
from __future__ import annotations

import argparse
import json
import sys

from . import bundles as bundles_mod
from . import changefeed as feed_mod
from . import intake as intake_mod
from . import query as query_mod
from . import withdraw as withdraw_mod
from .store import Store, reconcile


def _print(obj) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2))


def _load_groups(path):
    if not path:
        return {}
    with open(path, encoding="utf-8") as f:
        m = json.load(f)
    if not isinstance(m, dict) or not all(
            isinstance(k, str) and isinstance(v, str) for k, v in m.items()):
        raise SystemExit("groups file must be a JSON object mapping person -> group_code")
    return m


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="rvault",
                                description="local research data intake and withdrawal processing")
    p.add_argument("--store", required=True, help="managed store directory")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("init", help="create the store layout")

    sp = sub.add_parser("import", help="atomically import one JSONL batch")
    sp.add_argument("--batch-tag", required=True)
    sp.add_argument("--input", default="-", help="JSONL file (default: stdin)")

    sp = sub.add_parser("query", help="current projection (adopted edition per observation)")
    sp.add_argument("--study", required=True)

    sp = sub.add_parser("history", help="all stored editions of one observation")
    sp.add_argument("--study", required=True)
    sp.add_argument("--observation", required=True)

    sp = sub.add_parser("aggregate", help="group_code totals over the current projection")
    sp.add_argument("--study", required=True)
    sp.add_argument("--groups")

    b = sub.add_parser("bundle", help="analyst dataset bundles")
    bsub = b.add_subparsers(dest="bcmd", required=True)
    sp = bsub.add_parser("create")
    sp.add_argument("--study", required=True)
    sp.add_argument("--groups")
    sp = bsub.add_parser("list")
    sp.add_argument("--all", action="store_true")
    sp = bsub.add_parser("read")
    sp.add_argument("--bundle-id", required=True)

    w = sub.add_parser("withdraw", help="withdrawal registry and erasure")
    wsub = w.add_subparsers(dest="wcmd", required=True)
    sp = wsub.add_parser("register")
    sp.add_argument("--withdrawal-id", required=True)
    sp.add_argument("--study", required=True)
    sp.add_argument("--person", action="append", required=True)
    sp = wsub.add_parser("resume")
    g = sp.add_mutually_exclusive_group(required=True)
    g.add_argument("--withdrawal-id")
    g.add_argument("--all", action="store_true")
    sp = wsub.add_parser("status")
    sp.add_argument("--withdrawal-id")

    sub.add_parser("reconcile", help="sweep crash leftovers (also runs automatically)")

    f = sub.add_parser("feed", help="persistent change notifications for indexers")
    fsub = f.add_subparsers(dest="fcmd", required=True)
    sp = fsub.add_parser("register", help="create or re-enable a named consumer")
    sp.add_argument("--consumer", required=True)
    sp.add_argument("--from-seq", type=int,
                    help="explicit retained start (default: current tail)")
    sp.add_argument("--reactivate", action="store_true",
                    help="re-enable a disabled name as a new lifecycle")
    sp = fsub.add_parser("pull", help="deliver the pending range (line protocol)")
    sp.add_argument("--consumer", required=True)
    sp.add_argument("--limit", type=int, default=1000,
                    help="target event count; frames are never split (default 1000)")
    sp.add_argument("--preview", action="store_true",
                    help="show what the next pull would deliver; creates nothing")
    sp = fsub.add_parser("ack", help="acknowledge a delivery token")
    sp.add_argument("--consumer", required=True)
    sp.add_argument("--token", required=True)
    sp = fsub.add_parser("disable", help="stop receiving and release retention hold")
    sp.add_argument("--consumer", required=True)
    sp = fsub.add_parser("status", help="backlog, positions and retention floor")
    sp.add_argument("--consumer")
    sp = fsub.add_parser("trim", help="delete fully acknowledged frames")
    sp.add_argument("--through-seq", type=int,
                    help="explicit upper bound (required when no consumer is enabled)")
    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    store = Store(args.store)
    store.ensure()

    if args.cmd == "init":
        _print({"initialized": store.root})
        return 0
    if args.cmd == "reconcile":
        _print(reconcile(store))
        return 0

    if args.cmd == "import":
        if args.input == "-":
            code, obj = intake_mod.import_batch(store, args.batch_tag, sys.stdin.buffer)
        else:
            with open(args.input, "rb") as f:
                code, obj = intake_mod.import_batch(store, args.batch_tag, f)
        _print(obj)
        return code

    if args.cmd == "query":
        conn = store.connect()
        try:
            rows = query_mod.projection_rows(conn, args.study)
        finally:
            conn.close()
        _print({"study": args.study,
                "observations": [query_mod.row_dict(r) for r in rows]})
        return 0

    if args.cmd == "history":
        conn = store.connect()
        try:
            rows = query_mod.history_rows(conn, args.study, args.observation)
        finally:
            conn.close()
        _print({"study": args.study, "observation": args.observation,
                "editions": [{"edition": r["edition"], "person": r["person"],
                              "display_name": r["display_name"], "measure": r["measure"],
                              "memo": r["memo"], "batch_tag": r["batch_tag"]} for r in rows]})
        return 0

    if args.cmd == "aggregate":
        groups = _load_groups(args.groups)
        conn = store.connect()
        try:
            conn.execute("BEGIN")
            rows = query_mod.projection_rows(conn, args.study)
            result = query_mod.aggregate(rows, groups)
            conn.execute("COMMIT")
        finally:
            conn.close()
        _print({"study": args.study, "groups": result})
        return 0

    if args.cmd == "bundle":
        if args.bcmd == "create":
            groups = _load_groups(args.groups)
            try:
                bundle_id, content = bundles_mod.create_bundle(store, args.study, groups)
            except bundles_mod.BundleAborted as e:
                _print({"status": "aborted", "reason": "participants under withdrawal",
                        "study": args.study,
                        "withdrawn_participants": e.barred_count})
                return 3
            _print({"status": "created", "bundle_id": bundle_id, "study": args.study,
                    "participants": len(content["participants"]),
                    "path": "bundles/" + bundle_id + ".json"})
            return 0
        if args.bcmd == "list":
            _print({"bundles": bundles_mod.list_bundles(store, args.all)})
            return 0
        if args.bcmd == "read":
            try:
                data = bundles_mod.read_bundle(store, args.bundle_id)
            except bundles_mod.BundleNotFound:
                print(json.dumps({"error": "unknown bundle_id",
                                  "bundle_id": args.bundle_id}), file=sys.stderr)
                return 4
            except bundles_mod.BundleGone as e:
                print(json.dumps({"error": "bundle not readable",
                                  "bundle_id": args.bundle_id, "state": e.state,
                                  "note": e.note}), file=sys.stderr)
                return 4
            except bundles_mod.BundleTampered:
                print(json.dumps({"error": "bundle digest mismatch",
                                  "bundle_id": args.bundle_id}), file=sys.stderr)
                return 5
            sys.stdout.write(data.decode("utf-8"))
            return 0

    if args.cmd == "withdraw":
        if args.wcmd == "register":
            try:
                st = withdraw_mod.register(store, args.withdrawal_id,
                                           args.study, args.person)
            except withdraw_mod.WithdrawalConflict:
                print(json.dumps({"error": "withdrawal_id already registered with a "
                                           "different study or person set",
                                  "withdrawal_id": args.withdrawal_id}), file=sys.stderr)
                return 2
            except ValueError as e:
                print(json.dumps({"error": str(e)}), file=sys.stderr)
                return 2
            _print(st)
            return 0
        if args.wcmd == "resume":
            try:
                sts = withdraw_mod.resume(store, args.withdrawal_id, args.all)
            except KeyError:
                print(json.dumps({"error": "unknown withdrawal_id",
                                  "withdrawal_id": args.withdrawal_id}), file=sys.stderr)
                return 4
            _print({"withdrawals": sts})
            return 0
        if args.wcmd == "status":
            sts = withdraw_mod.status(store, args.withdrawal_id)
            if sts is None:
                print(json.dumps({"error": "unknown withdrawal_id",
                                  "withdrawal_id": args.withdrawal_id}), file=sys.stderr)
                return 4
            _print({"withdrawals": sts})
            return 0

    if args.cmd == "feed":
        return _feed_command(store, args)

    return 1


def _feed_error(e) -> int:
    payload = e.as_dict() if hasattr(e, "as_dict") else {"code": "error", "error": str(e)}
    print(json.dumps(payload, ensure_ascii=False), file=sys.stderr)
    return e.exit_code


def _feed_command(store, args) -> int:
    try:
        if args.fcmd == "register":
            _print(feed_mod.register(store, args.consumer, start_from=args.from_seq,
                                     reactivate=args.reactivate))
            return 0
        if args.fcmd == "disable":
            _print(feed_mod.disable(store, args.consumer))
            return 0
        if args.fcmd == "ack":
            _print(feed_mod.ack(store, args.consumer, args.token))
            return 0
        if args.fcmd == "status":
            _print(feed_mod.status(store, args.consumer))
            return 0
        if args.fcmd == "trim":
            _print(feed_mod.trim(store, args.through_seq))
            return 0
        if args.fcmd == "pull":
            gen = (feed_mod.preview(store, args.consumer, args.limit)
                   if args.preview
                   else feed_mod.iter_delivery(store, args.consumer, args.limit))
            try:
                for line in gen:
                    sys.stdout.write(line)
                    sys.stdout.flush()
            except BrokenPipeError:
                return 0
            return 0
    except feed_mod.StartOutOfRange as e:
        return _feed_error(e)
    except (feed_mod.ChangeFeedError, ValueError) as e:
        return _feed_error(e)
    return 1
