"""Command line interface.

  rvault --store S init
  rvault --store S import --batch-tag T [--input FILE]
                  [--provider P --material-type M --contract-version N]
  rvault --store S contract register --provider P --material-type M
                  --version N --fields FILE [--predecessor N]
                  [--compatibility compatible|branch]
  rvault --store S contract compare --provider P --material-type M
                  --against N --fields FILE
  rvault --store S contract show --provider P --material-type M --version N
  rvault --store S contract list [--provider P] [--material-type M]
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
  rvault --store S lineage declare --job J --rule R [--input FILE]
  rvault --store S lineage leaf [--input FILE]
  rvault --store S lineage upstream --node JSON [--max-depth N]
  rvault --store S lineage downstream --node JSON [--max-depth N]
  rvault --store S lineage jobs
  rvault --store S mail receive --input FILE.eml [--chunk-size N]
  rvault --store S mail resume --input FILE.eml [--chunk-size N]
  rvault --store S mail status [--message DIGEST]
  rvault --store S nested plan show --plan FILE
  rvault --store S nested prepare --plan FILE --input FILE --database DB
  rvault --store S nested resume  --plan FILE --input FILE --database DB
  rvault --store S nested status  --database DB [--source DIGEST]
  rvault --store S nested roots   --database DB [--source DIGEST] [--root N]
  rvault --store S nested read    --database DB --table T [--source DIGEST]
                  [--root N] [--limit N]
  rvault --store S nested children --database DB --table T --parent ROW_ID
                  [--limit N]

Exit codes: 0 ok; 2 rejected input / conflicting credential, withdrawal, ack,
trim, lineage or contract registration; 3 bundle aborted because a withdrawal
landed during creation; 4 unknown or unreadable object (including an unknown
contract version); 5 integrity failure; 6 change-feed start position no longer
retained (gap).
"""
from __future__ import annotations

import argparse
import json
import os
import sys

from . import bundles as bundles_mod
from . import changefeed as feed_mod
from . import contracts as contracts_mod
from . import intake as intake_mod
from . import lineage as lineage_mod
from . import mail as mail_mod
from . import study_nested as nested_mod

try:
    from . import study_nested_engine as nested_engine
except ModuleNotFoundError as _nested_import_error:
    nested_engine = None
    _NESTED_IMPORT_ERROR = _nested_import_error
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
    sp.add_argument("--provider", help="input contract: provider id (with "
                    "--material-type and --contract-version)")
    sp.add_argument("--material-type", help="input contract: material type")
    sp.add_argument("--contract-version", type=int,
                    help="input contract: exact published version to check "
                         "against (never silently the latest)")

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

    c = sub.add_parser("contract", help="versioned upstream input contracts")
    csub = c.add_subparsers(dest="ccmd", required=True)
    sp = csub.add_parser("register", help="publish one immutable contract version")
    sp.add_argument("--provider", required=True)
    sp.add_argument("--material-type", required=True)
    sp.add_argument("--version", type=int, required=True)
    sp.add_argument("--fields", required=True,
                    help='JSON file: {"fields": [{name,type,required,nullable,'
                         'default?,target?}, ...]}')
    sp.add_argument("--predecessor", type=int,
                    help="version this one was compared with (required after v1)")
    sp.add_argument("--compatibility", choices=["compatible", "branch"],
                    help="explicit compatibility declaration: 'compatible' "
                         "(compatible upgrade) or 'branch' (new branch); "
                         "the first version omits it")
    sp = csub.add_parser("compare", help="field-by-field diff against one version")
    sp.add_argument("--provider", required=True)
    sp.add_argument("--material-type", required=True)
    sp.add_argument("--against", type=int, required=True,
                    help="published version to compare the candidate with")
    sp.add_argument("--fields", required=True)
    sp = csub.add_parser("show", help="one published version with its change list")
    sp.add_argument("--provider", required=True)
    sp.add_argument("--material-type", required=True)
    sp.add_argument("--version", type=int, required=True)
    sp = csub.add_parser("list", help="published contract versions")
    sp.add_argument("--provider")
    sp.add_argument("--material-type")

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

    ln = sub.add_parser("lineage", help="revision-level data lineage")
    lsub = ln.add_subparsers(dest="lcmd", required=True)
    sp = lsub.add_parser("declare", help="record one processing job (JSONL relation)")
    sp.add_argument("--job", required=True, help="job identifier")
    sp.add_argument("--rule", required=True, help="processing rule identifier")
    sp.add_argument("--input", default="-",
                    help='JSONL: {"output": node, "inputs": [node, ...]} (default: stdin)')
    sp = lsub.add_parser("leaf", help="declare revisions as original collection leaves")
    sp.add_argument("--input", default="-",
                    help="JSONL of node objects (default: stdin)")
    sp = lsub.add_parser("upstream", help="revisions this one depends on")
    sp.add_argument("--node", required=True,
                    help='JSON {"study":..,"observation":..,"edition":..}')
    sp.add_argument("--max-depth", type=int, default=None,
                    help="maximum hops (default: unlimited)")
    sp = lsub.add_parser("downstream", help="outputs that depend on this revision")
    sp.add_argument("--node", required=True,
                    help='JSON {"study":..,"observation":..,"edition":..}')
    sp.add_argument("--max-depth", type=int, default=None,
                    help="maximum hops (default: unlimited)")
    lsub.add_parser("jobs", help="list recorded processing jobs")

    m = sub.add_parser("mail", help="offline .eml intake channel")
    msub = m.add_subparsers(dest="mcmd", required=True)
    sp = msub.add_parser("receive", help="preflight one .eml message, then "
                         "import its manifest's batches (idempotent)")
    sp.add_argument("--input", required=True, help="raw .eml file")
    sp.add_argument("--chunk-size", type=int, default=mail_mod.DEFAULT_CHUNK,
                    help="read chunk size in bytes (default: 65536)")
    sp = msub.add_parser("resume", help="continue an interrupted message")
    sp.add_argument("--input", required=True, help="raw .eml file")
    sp.add_argument("--chunk-size", type=int, default=mail_mod.DEFAULT_CHUNK,
                    help="read chunk size in bytes (default: 65536)")
    sp = msub.add_parser("status", help="mail processing records")
    sp.add_argument("--message", help="message digest (default: all messages)")

    n = sub.add_parser("nested", help="nested study-table preparation "
                       "(independent relational output database)")
    nsub = n.add_subparsers(dest="ncmd", required=True)
    np_ = nsub.add_parser("plan", help="compile and show a preparation plan")
    psub = np_.add_subparsers(dest="pcmd", required=True)
    sp = psub.add_parser("show", help="validate a plan and print its tables")
    sp.add_argument("--plan", required=True, help="declarative plan JSON file")
    sp = nsub.add_parser("prepare", help="prepare one JSON source file")
    sp.add_argument("--plan", required=True)
    sp.add_argument("--input", required=True,
                    help="JSON file whose top level is an array of roots")
    sp.add_argument("--database", required=True,
                    help="output relational database (created/bound to plan)")
    sp.add_argument("--chunk-size", type=int, default=65536,
                    help="parser read chunk in bytes (default 65536)")
    sp = nsub.add_parser("resume", help="continue a previously started source")
    sp.add_argument("--plan", required=True)
    sp.add_argument("--input", required=True)
    sp.add_argument("--database", required=True)
    sp.add_argument("--chunk-size", type=int, default=65536)
    sp = nsub.add_parser("status", help="run and per-root records")
    sp.add_argument("--database", required=True)
    sp.add_argument("--source", help="source digest (default: all sources)")
    sp = nsub.add_parser("roots", help="per-root processing outcome")
    sp.add_argument("--database", required=True)
    sp.add_argument("--source")
    sp.add_argument("--root", type=int)
    sp = nsub.add_parser("read", help="stream projected rows of one table")
    sp.add_argument("--database", required=True)
    sp.add_argument("--table", required=True)
    sp.add_argument("--source")
    sp.add_argument("--root", type=int)
    sp.add_argument("--limit", type=int)
    sp = nsub.add_parser("children",
                         help="direct child rows of one parent row")
    sp.add_argument("--database", required=True)
    sp.add_argument("--table", required=True)
    sp.add_argument("--parent", required=True)
    sp.add_argument("--limit", type=int)
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
        selectors = (args.provider, args.material_type, args.contract_version)
        given = [s is not None for s in selectors]
        selection = None
        if any(given):
            if not all(given):
                print(json.dumps({"error": "contract-checked import requires all "
                                           "of --provider, --material-type and "
                                           "--contract-version",
                                  "status": "rejected"}), file=sys.stderr)
                return 2
            selection = selectors
        if args.input == "-":
            code, obj = intake_mod.import_batch(store, args.batch_tag,
                                                sys.stdin.buffer, selection)
        else:
            with open(args.input, "rb") as f:
                code, obj = intake_mod.import_batch(store, args.batch_tag, f,
                                                    selection)
        _print(obj)
        return code

    if args.cmd == "contract":
        return _contract_command(store, args)

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

    if args.cmd == "lineage":
        return _lineage_command(store, args)

    if args.cmd == "nested":
        return _nested_command(store, args)

    if args.cmd == "mail":
        if args.mcmd in ("receive", "resume"):
            code, obj = mail_mod.receive(
                store, args.input, chunk_size=args.chunk_size,
                require_existing=(args.mcmd == "resume"))
            _print(obj)
            return code
        if args.mcmd == "status":
            obj = mail_mod.status(store, args.message)
            if obj is None:
                print(json.dumps({"error": "unknown message",
                                  "message_digest": args.message}),
                      file=sys.stderr)
                return 4
            _print(obj)
            return 0

    return 1


def _nested_command(store, args) -> int:
    if nested_engine is None:
        print(json.dumps({"code": "missing_dependency",
                          "error": "the nested subsystem requires the "
                                   "ijson library (pip install ijson)",
                          "detail": str(_NESTED_IMPORT_ERROR)}),
              file=sys.stderr)
        return 4
    try:
        if args.ncmd == "plan":
            plan = nested_mod.load_plan(args.plan)
            _print({"status": "valid", "plan_digest": plan.digest,
                    "root": plan.root_name,
                    "tables": [{"name": t.name, "parent": t.parent,
                                "path": t.path,
                                "columns": [c.as_dict() for c in t.columns]}
                               for t in plan.tables]})
            return 0
        if args.ncmd in ("prepare", "resume"):
            plan = nested_mod.load_plan(args.plan)
            try:
                summary = nested_engine.prepare(
                    store, plan, args.input, args.database,
                    require_existing=(args.ncmd == "resume"),
                    chunk_size=args.chunk_size)
            except FileNotFoundError:
                print(json.dumps({"code": "unknown_input",
                                  "error": "input file not found",
                                  "input": args.input}), file=sys.stderr)
                return 4
            _print(summary)
            # 0 = every processed root handled (committed or rejected with a
            # recorded per-root outcome); 2 = the document itself could not be
            # finished (syntax damage / wrong top-level shape).
            return 2 if summary.get("status") == "aborted" else 0
        if args.ncmd == "status":
            if not os.path.exists(args.database):
                print(json.dumps({"code": "unknown_database",
                                  "error": "no such database",
                                  "database": args.database}),
                      file=sys.stderr)
                return 4
            conn = nested_mod.open_output(args.database)
            try:
                if not nested_engine.plan_tables(conn):
                    print(json.dumps({"code": "unknown_database",
                                      "error": "not a preparation database"}),
                          file=sys.stderr)
                    return 4
                runs = list(nested_engine.run_records(conn, args.source))
                totals = {}
                for t in nested_engine.table_names(conn):
                    n = conn.execute(
                        'SELECT COUNT(*) c FROM %s' % nested_mod.ident(t)
                        + (" WHERE source_digest=?" if args.source else ""),
                        ((args.source,) if args.source else ())).fetchone()["c"]
                    totals[t] = n
                _print({"runs": runs, "row_totals": totals})
                return 0
            finally:
                conn.close()
        if args.ncmd == "roots":
            if not os.path.exists(args.database):
                print(json.dumps({"code": "unknown_database"}),
                      file=sys.stderr)
                return 4
            conn = nested_mod.open_output(args.database)
            try:
                if not nested_engine.plan_tables(conn):
                    return 4
                _print({"roots": list(nested_engine.root_records(
                    conn, args.source, args.root))})
                return 0
            finally:
                conn.close()
        if args.ncmd == "read":
            if not os.path.exists(args.database):
                print(json.dumps({"code": "unknown_database"}),
                      file=sys.stderr)
                return 4
            conn = nested_mod.open_output(args.database)
            try:
                if not nested_engine.plan_tables(conn):
                    return 4
                try:
                    rows = nested_engine.iter_rows(
                        conn, args.table, args.source, args.root,
                        limit=args.limit)
                except nested_engine.UnknownTable:
                    print(json.dumps({"code": "unknown_table",
                                      "table": args.table}), file=sys.stderr)
                    return 4
                for row in rows:
                    sys.stdout.write(json.dumps(row, ensure_ascii=False) + "\n")
                return 0
            finally:
                conn.close()
        if args.ncmd == "children":
            if not os.path.exists(args.database):
                print(json.dumps({"code": "unknown_database"}),
                      file=sys.stderr)
                return 4
            conn = nested_mod.open_output(args.database)
            try:
                if not nested_engine.plan_tables(conn):
                    return 4
                try:
                    rows = nested_engine.children(
                        conn, args.table, args.parent, limit=args.limit)
                except nested_engine.UnknownTable:
                    print(json.dumps({"code": "unknown_table",
                                      "table": args.table}), file=sys.stderr)
                    return 4
                for row in rows:
                    sys.stdout.write(json.dumps(row, ensure_ascii=False) + "\n")
                return 0
            finally:
                conn.close()
    except nested_mod.PlanError as e:
        print(json.dumps(e.as_dict(), ensure_ascii=False), file=sys.stderr)
        return e.exit_code
    return 1


def _read_fields_file(path):
    try:
        with open(path, encoding="utf-8") as f:
            document = json.load(f)
    except FileNotFoundError:
        raise contracts_mod.InvalidContract("fields file not found: " + path)
    except json.JSONDecodeError as e:
        raise contracts_mod.InvalidContract("fields file is not valid JSON: %s" % e)
    return contracts_mod.parse_spec(document)


def _contract_command(store, args) -> int:
    try:
        if args.ccmd == "register":
            specs = _read_fields_file(args.fields)
            code, obj = contracts_mod.register(
                store, args.provider, args.material_type, args.version, specs,
                args.predecessor, args.compatibility)
            _print(obj)
            return code
        if args.ccmd == "compare":
            specs = _read_fields_file(args.fields)
            _print(contracts_mod.compare(store, args.provider, args.material_type,
                                         args.against, specs))
            return 0
        if args.ccmd == "show":
            _print(contracts_mod.show(store, args.provider, args.material_type,
                                      args.version))
            return 0
        if args.ccmd == "list":
            _print({"contracts": contracts_mod.list_versions(
                store, args.provider, args.material_type)})
            return 0
    except contracts_mod.ContractError as e:
        _print(e.as_dict())
        return e.exit_code
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


def _lineage_command(store, args) -> int:
    try:
        if args.lcmd == "declare":
            if args.input == "-":
                code, obj = lineage_mod.declare(store, args.job, args.rule,
                                                sys.stdin.buffer)
            else:
                with open(args.input, "rb") as f:
                    code, obj = lineage_mod.declare(store, args.job, args.rule, f)
            _print(obj)
            return code
        if args.lcmd == "leaf":
            if args.input == "-":
                code, obj = lineage_mod.declare_leaves(store, sys.stdin.buffer)
            else:
                with open(args.input, "rb") as f:
                    code, obj = lineage_mod.declare_leaves(store, f)
            _print(obj)
            return code
        if args.lcmd == "jobs":
            _print({"jobs": lineage_mod.list_jobs(store)})
            return 0
        if args.lcmd in ("upstream", "downstream"):
            node = lineage_mod.parse_node_arg(args.node)
            gen = lineage_mod.traverse(store, node, args.max_depth, args.lcmd)
            try:
                for line in gen:
                    sys.stdout.write(line + "\n")
                    sys.stdout.flush()
            except BrokenPipeError:
                return 0
            return 0
    except lineage_mod.LineageError as e:
        print(json.dumps(e.as_dict(), ensure_ascii=False), file=sys.stderr)
        return e.exit_code
    return 1
