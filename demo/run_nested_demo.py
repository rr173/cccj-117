#!/usr/bin/env python3
"""End-to-end demo for nested study-table preparation.

All artifacts (plans, fixtures, stores, output databases, logs and the
SIGKILL synchronization directory) live in a fresh temporary directory
printed at the start; override with RVAULT_NESTED_WORK.

Every expectation is hand-written in demo/expected_nested.py and checked by
demo/verify_nested.py with its own SQL - the production code is never used
to compute an expected value.

Phases:
  1  plan compilation and its rejection classes (no input byte is read)
  2  the task's example: four tables and every parent/child link
  3  parent scalars written after the child arrays give the same projection
  4  empty array / missing / null / false / 0 / duplicate values /
     dotted, slashed and spaced keys
  5  a duplicate-key root and a type-error root between good roots:
     the bad root's whole subtree disappears, other roots stay readable
  6  a syntax-truncated tail: no half tree, committed roots kept, the
     abort position is reported
  7  SIGKILL at "descendant rows written, root not committed", then resume:
     no duplication, same identities and parent links
  8  a different plan and different source bytes cannot continue the progress
  9  a few roots but one root with 120k descendants, run under an explicit
     process memory limit; row counts, first/last children and peak RSS
 10  the older rvault commands still run (import/query/withdraw/feed/mail)

Usage: python3 demo/run_nested_demo.py
"""
from __future__ import annotations

import json
import os
import resource
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time

DEMO = os.path.dirname(os.path.abspath(__file__))
PROJ = os.path.dirname(DEMO)
WORK = os.environ.get("RVAULT_NESTED_WORK") or tempfile.mkdtemp(
    prefix="rvault-nested-")
SAMPLES = os.path.join(WORK, "samples")
LOGS = os.path.join(WORK, "logs")
SYNC = os.path.join(WORK, "sync")

sys.path.insert(0, DEMO)
import expected_nested as E  # noqa: E402

BASE_ENV = dict(os.environ)
BASE_ENV["PYTHONPATH"] = PROJ

_step = 0
_failures = []


def log(name, text):
    with open(os.path.join(LOGS, name), "a", encoding="utf-8") as f:
        f.write(text)


def write_sample(name, text, directory=SAMPLES):
    os.makedirs(directory, exist_ok=True)
    path = os.path.join(directory, name)
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write(text)
    return path


def rv(store, *args, ok=(0,), tag=None, env=None):
    global _step
    _step += 1
    cmd = [sys.executable, "-m", "rvault", "--store", store] + list(args)
    r = subprocess.run(cmd, capture_output=True, text=True,
                       env=env or BASE_ENV)
    name = "%03d_%s" % (_step, tag or (args[0] if args else "cmd"))
    log(name + ".out", r.stdout)
    log(name + ".err", r.stderr)
    if r.returncode not in ok:
        raise SystemExit("command failed rc=%d: %s\nstdout:\n%s\nstderr:\n%s"
                         % (r.returncode, " ".join(cmd), r.stdout, r.stderr))
    return r


def rvj(store, *args, **kw):
    return json.loads(rv(store, *args, **kw).stdout)


def check(cond, msg, detail=None):
    if cond:
        print("ok  ", msg)
    else:
        _failures.append(msg)
        print("FAIL", msg)
        if detail is not None:
            print("     detail:", detail)


def verify(db, cp):
    r = subprocess.run([sys.executable, os.path.join(DEMO, "verify_nested.py"),
                        "--database", db, "--checkpoint", cp],
                       capture_output=True, text=True)
    log("verify_%s.log" % cp, r.stdout + r.stderr)
    if r.returncode != 0:
        print(r.stdout)
        print(r.stderr, file=sys.stderr)
        raise SystemExit("verify %s FAILED" % cp)
    print("== checkpoint %s ok" % cp)


def db_rows(path, sql, args=()):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in conn.execute(sql, args)]
    finally:
        conn.close()


# --------------------------------------------------------------------------
# phase 1: plan compilation
# --------------------------------------------------------------------------
def phase_plan():
    store = os.path.join(WORK, "store_plan")
    plan_path = write_sample("plan.json", json.dumps(E.PLAN, indent=2))

    out = rvj(store, "nested", "plan", "show", "--plan", plan_path)
    check(out["status"] == "valid", "valid plan compiles")
    check(out["plan_digest"] == E.PLAN_DIGEST,
          "plan digest matches the independently computed canonical digest",
          out["plan_digest"])
    check([t["name"] for t in out["tables"]] ==
          ["participants", "visits", "readings", "subs", "consents", "tags"],
          "tables compiled in depth-first declaration order")
    check(out["tables"][2]["parent"] == "visits",
          "readings declares visits as its parent")

    bad_dir = os.path.join(SAMPLES, "bad_plans")
    for i, (category, plan) in enumerate(E.PLAN_REJECTIONS):
        path = write_sample("bad_%02d_%s.json" % (i, category),
                            json.dumps(plan), directory=bad_dir)
        r = rv(store, "nested", "plan", "show", "--plan", path, ok=(2,),
               tag="plan_%s" % category)
        got = json.loads(r.stderr)
        check(got.get("plan_error") == category,
              "plan rejection category %r" % category, got)

    # a plan error must not have read the input: use a nonexistent file
    r = rv(store, "nested", "prepare", "--plan",
           os.path.join(bad_dir, "bad_00_unknown_parent.json"),
           "--input", os.path.join(SAMPLES, "does-not-exist.json"),
           "--database", os.path.join(WORK, "never.sqlite3"), ok=(2,),
           tag="plan_before_input")
    check(json.loads(r.stderr).get("plan_error") == "unknown_parent",
          "invalid plan reported before any input is touched")
    check(not os.path.exists(os.path.join(WORK, "never.sqlite3")),
          "invalid plan creates no output database")


# --------------------------------------------------------------------------
# phase 2-4: example, late fields, variants
# --------------------------------------------------------------------------
def prepare_fixture(store, plan_path, text, name, dbname):
    src = write_sample(name, text)
    db = os.path.join(WORK, dbname)
    out = rvj(store, "nested", "prepare", "--plan", plan_path,
              "--input", src, "--database", db)
    return src, db, out


def phase_example():
    store = os.path.join(WORK, "store_example")
    plan_path = os.path.join(SAMPLES, "plan.json")
    src, db, out = prepare_fixture(store, plan_path, E.F_EXAMPLE,
                                   "example.json", "example.sqlite3")
    check(out["source_digest"] == E.D_EXAMPLE,
          "source digest is sha256 of the exact file bytes",
          out["source_digest"])
    check(out["status"] == "complete", "example run complete")
    check(out["rows"] == E.E_EXAMPLE_COUNTS,
          "counts: 1 participant, 2 visits, 2 readings, 3 consents",
          out["rows"])

    # interface: read rows table by table, in reproducible input order
    got = []
    for table in ("participants", "visits", "readings", "consents"):
        lines = rv(store, "nested", "read", "--database", db,
                   "--table", table).stdout.strip().split("\n")
        got.append([json.loads(x) for x in lines])
    check([r["row_id"] for r in got[1]] ==
          [r["row_id"] for r in E.E_EXAMPLE["visits"]],
          "read orders visits by input index and reproduces identities")

    # interface: children of one parent row
    parent = got[1][0]["row_id"]
    lines = rv(store, "nested", "children", "--database", db,
               "--table", "readings", "--parent", parent).stdout.strip()
    kids = [json.loads(x) for x in lines.split("\n") if x]
    check([k["row_id"] for k in kids] ==
          [r["row_id"] for r in E.E_EXAMPLE["readings"]],
          "direct children of V1 are exactly its two readings")
    lines = rv(store, "nested", "children", "--database", db,
               "--table", "readings", "--parent", got[1][1]["row_id"]
               ).stdout.strip()
    check(lines == "", "V2 (empty readings array) has no child rows but "
          "its own row exists")

    # interface: per-root processing result
    roots = rvj(store, "nested", "roots", "--database", db)["roots"]
    check(roots[0]["status"] == "committed"
          and roots[0]["counts"] == E.E_EXAMPLE_ROOT_COUNTS,
          "root record carries the committed status and per-table counts",
          roots[0]["counts"])
    status = rvj(store, "nested", "status", "--database", db)
    check(status["runs"][0]["status"] == "complete",
          "run status complete")
    check(status["row_totals"] == E.E_EXAMPLE_COUNTS,
          "status row totals match")

    # the same input under the minimal four-table plan documented in the
    # README yields the same four tables and counts
    minimal_path = write_sample("plan_minimal.json",
                                json.dumps(E.MINIMAL_PLAN, indent=2))
    minimal_db = os.path.join(WORK, "example_minimal.sqlite3")
    out_min = rvj(store, "nested", "prepare", "--plan", minimal_path,
                  "--input", src, "--database", minimal_db,
                  tag="example_minimal")
    check(out_min["rows"] == E.MINIMAL_COUNTS,
          "the minimal documented plan reproduces the same counts",
          out_min["rows"])

    # no reading was deduplicated even though code repeats
    check(len([r for r in got[2]]) == 2,
          "duplicate business codes produce two rows (no dedup)")
    # readings belong to V1, not V2
    check(all(read["parent_row_id"] == parent for read in got[2]),
          "readings are attached to V1 only")
    verify(db, "example")


def phase_late():
    store = os.path.join(WORK, "store_late")
    plan_path = os.path.join(SAMPLES, "plan.json")
    src, db, out = prepare_fixture(store, plan_path, E.F_LATE,
                                   "late.json", "late.sqlite3")
    check(out["status"] == "complete", "late-field run complete")
    verify(db, "late")

    # compare projected values with the early-field run: identical
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    try:
        early = sqlite3.connect(os.path.join(WORK, "example.sqlite3"))
        early.row_factory = sqlite3.Row
        try:
            for table in ("participants", "visits", "readings", "consents"):
                cols = ["person", "study", "display.label", "ratio",
                        "visits_after", "memo"]
                if table != "participants":
                    if table == "visits":
                        cols = ["visit_label", "visit_note"]
                    elif table == "readings":
                        cols = ["code", "value"]
                    else:
                        cols = ["kind", "granted"]
                a = [[r[c] for c in cols] + [r[c + "__present"] for c in cols]
                     for r in conn.execute(
                         'SELECT * FROM "%s" ORDER BY root_index, ordinal'
                         % table)]
                b = [[r[c] for c in cols] + [r[c + "__present"] for c in cols]
                     for r in early.execute(
                         'SELECT * FROM "%s" ORDER BY root_index, ordinal'
                         % table)]
                check(a == b, "%s projection identical when parent fields "
                      "follow the child arrays" % table, (a, b))
        finally:
            early.close()
    finally:
        conn.close()


def phase_variants():
    store = os.path.join(WORK, "store_variants")
    plan_path = os.path.join(SAMPLES, "plan.json")
    src, db, out = prepare_fixture(store, plan_path, E.F_VARIANTS,
                                   "variants.json", "variants.sqlite3")
    check(out["status"] == "complete", "variant run complete")
    verify(db, "variants")
    # explicit claims about the subtle distinctions
    p0, p1 = E.E_VARIANTS["participants"]
    check(p0["values"]["ratio"]["value"] == "0"
          and p0["values"]["ratio"]["presence"] == 1,
          "0 is a present value, not missing")
    check(p0["values"]["memo"]["value"] == ""
          and p0["values"]["memo"]["presence"] == 1,
          "empty string is a present value, not missing")
    check(p1["values"]["memo"]["presence"] == 2,
          "explicit null is distinguishable from missing")
    check(p1["values"]["study"]["presence"] == 0,
          "missing key is presence 0")
    consent_rows = E.E_VARIANTS["consents"]
    check(consent_rows[0]["values"] == consent_rows[1]["values"],
          "two identical consents remain two rows")
    reading_rows = E.E_VARIANTS["readings"]
    check(reading_rows[0]["values"] == reading_rows[1]["values"],
          "two identical readings with the same code remain two rows")
    tag_b = [r for r in E.E_VARIANTS["tags"]
             if r["values"]["tag"]["value"] == "a/b.c"]
    check(tag_b and [r["array_path"] for r in E.E_VARIANTS["tags"]] ==
          [[["tags", 0]], [["tags", 1]]],
          "a key with '/' is one literal segment; the path stays ['meta','tags']")
    # sub-array under a visit: empty arrays produce no rows but no error
    check(E.E_VARIANTS["subs"] == [], "an empty subs array yields no rows")


# --------------------------------------------------------------------------
# phase 5-6: mid-file rejections and truncated tail
# --------------------------------------------------------------------------
def phase_bad_roots():
    store = os.path.join(WORK, "store_bad")
    plan_path = os.path.join(SAMPLES, "plan.json")

    src, db, out = prepare_fixture(store, plan_path, E.F_BAD_MIDDLE,
                                   "bad_middle.json", "bad_middle.sqlite3")
    check(out["status"] == "complete_with_rejections",
          "duplicate-key root rejected, run continues")
    check(out["roots_rejected_total"] == 1, "exactly one rejected root")
    verify(db, "bad_middle")
    # the bad root's whole subtree is gone: reading the roots shows it
    roots = rvj(store, "nested", "roots", "--database", db)["roots"]
    check(roots[1]["status"] == "rejected"
          and roots[1]["category"] == "duplicate_key",
          "rejected root records duplicate_key")
    print("     rejected-root diagnostic:", json.dumps(roots[1]["detail"]))
    check("R1" not in json.dumps(roots[1]),
          "diagnostic does not echo the duplicate business value")

    src, db, out = prepare_fixture(store, plan_path, E.F_BAD_TYPE,
                                   "bad_type.json", "bad_type.sqlite3")
    check(out["status"] == "complete_with_rejections",
          "type-mismatch root rejected, run continues")
    verify(db, "bad_type")

    src = write_sample("truncated.json", E.F_TRUNCATED)
    db = os.path.join(WORK, "truncated.sqlite3")
    r = rv(store, "nested", "prepare", "--plan", plan_path,
           "--input", src, "--database", db, ok=(2,), tag="truncated")
    got = json.loads(r.stdout)
    check(got["status"] == "aborted", "truncated tail aborts the run")
    check(got["error"]["code"] == "json_syntax", "truncated: syntax category")
    check(got["error"]["position"] == len(E.F_TRUNCATED),
          "truncated: position is the end of the file",
          got["error"]["position"])
    check(got["error"]["root_index"] == 1,
          "truncated: reports the root it could not finish")
    verify(db, "truncated")
    # resuming a truncated file: the committed root is kept, the truncated
    # one still fails the same way, and nothing is duplicated
    r = rv(store, "nested", "resume", "--plan", plan_path, "--input", src,
           "--database", db, ok=(2,), tag="truncated_resume")
    got2 = json.loads(r.stdout)
    check(got2["roots_skipped_already_committed"] == 1,
          "resume skips the already committed root of a truncated file")
    check(got2["rows"] == E.E_TRUNCATED_COUNTS,
          "resume of a truncated file duplicates nothing", got2["rows"])
    check(got2["status"] == "aborted", "truncated file still aborts on resume")


# --------------------------------------------------------------------------
# phase 7: forced exit and resume
# --------------------------------------------------------------------------
def phase_resume():
    store = os.path.join(WORK, "store_resume")
    plan_path = os.path.join(SAMPLES, "plan.json")
    src = write_sample("resume.json", E.F_RESUME)
    db = os.path.join(WORK, "resume.sqlite3")
    os.makedirs(SYNC, exist_ok=True)

    # Kill at the commit boundary of the SECOND root: root 0 is already
    # durably committed, root 1's descendant rows are written but its
    # transaction is not committed.
    env = dict(BASE_ENV)
    env["RVAULT_SYNC_DIR"] = SYNC
    env["RVAULT_SYNC_WAIT"] = "nested.root_before_commit.1"
    proc = subprocess.Popen(
        [sys.executable, "-m", "rvault", "--store", store, "nested",
         "prepare", "--plan", plan_path, "--input", src, "--database", db],
        env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    def wait_for(name, timeout=60.0):
        marker = os.path.join(SYNC, name + ".reached")
        deadline = time.time() + timeout
        while time.time() < deadline:
            if os.path.exists(marker):
                return True
            time.sleep(0.02)
        return False

    reached = wait_for("nested.root_before_commit.1")
    check(reached, "process reached the second root's commit boundary")
    # root 0's rows exist in the uncommitted transaction, but nothing is
    # durable.  Kill at exactly this point.
    proc.send_signal(signal.SIGKILL)
    proc.communicate()
    log("resume_killed.txt", "killed rc=%s\n" % proc.returncode)

    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    try:
        roots = [dict(r) for r in conn.execute(
            "SELECT root_index, status FROM ns_roots ORDER BY root_index")]
        parts = conn.execute("SELECT COUNT(*) FROM participants").fetchone()[0]
    finally:
        conn.close()
    check(roots == [{"root_index": 0, "status": "committed"}],
          "after SIGKILL at root 1's commit boundary, root 0 is committed "
          "and root 1 has no record", roots)
    check(parts == 1, "root 0's row survived while root 1's half tree did "
          "not", parts)
    # every ancestor row of root 1 is gone too
    leftover = db_rows(db, "SELECT COUNT(*) c FROM visits WHERE root_index=1")[0]["c"]
    check(leftover == 0, "root 1's descendant rows were rolled back",
          leftover)

    # resume; the committed roots are skipped (none), and the run completes
    out = rvj(store, "nested", "resume", "--plan", plan_path, "--input", src,
              "--database", db, tag="resume")
    check(out["status"] == "complete", "resume completes the file")
    check(out["roots_committed_this_run"] == 2
          and out["roots_skipped_already_committed"] == 1,
          "resume skips the committed root 0 and processes 1 and 2", out)
    check(out["roots_committed_total"] == 3,
          "the database ends with exactly three committed roots", out)
    verify(db, "resume")

    # a second identical delivery is fully skipped, adding no rows
    out2 = rvj(store, "nested", "prepare", "--plan", plan_path, "--input",
               src, "--database", db, tag="resume_again")
    check(out2["roots_skipped_already_committed"] == 3,
          "re-delivery of identical bytes skips all committed roots")
    check(out2["roots_committed_this_run"] == 0,
          "re-delivery adds no new root rows")
    verify(db, "resume")

    # resume with a different plan cannot continue the same progress
    alt_plan = dict(E.PLAN)
    alt_plan["tables"] = list(E.PLAN["tables"]) + [
        {"name": "extra", "parent": "participants", "path": ["extra"],
         "columns": [{"name": "x", "path": ["x"], "type": "string"}]}]
    alt_path = write_sample("plan_alt.json", json.dumps(alt_plan))
    r = rv(store, "nested", "resume", "--plan", alt_path, "--input", src,
           "--database", db, ok=(2,), tag="alt_plan")
    got = json.loads(r.stderr)
    check(got.get("plan_error") == "plan_mismatch",
          "a different plan cannot continue this database", got)

    # the same source under a different plan in a fresh database is refused
    # at the schema-binding step too
    db2 = os.path.join(WORK, "resume_alt.sqlite3")
    r = rv(store, "nested", "prepare", "--plan", alt_path, "--input", src,
           "--database", db2, tag="alt_plan_fresh")
    check(r.returncode in (0, 2), "fresh database with the alt plan works")

    # changed source bytes (same length, different person) must not piggyback
    changed = E.F_RESUME.replace('"G1"', '"G9"')
    check(len(changed) == len(E.F_RESUME), "changed fixture keeps its length")
    src2 = write_sample("resume_changed.json", changed)
    out3 = rvj(store, "nested", "prepare", "--plan", plan_path, "--input",
               src2, "--database", db, tag="changed_bytes")
    check(out3["source_digest"] != E.D_RESUME,
          "changed bytes yield a different source digest")
    check(out3["source_digest"] != out["source_digest"],
          "changed bytes do not reuse the original digest")
    verify(db, "plan_mismatch")


# --------------------------------------------------------------------------
# phase 8: plan binding of the target database
# --------------------------------------------------------------------------
def phase_binding():
    store = os.path.join(WORK, "store_binding")
    plan_path = os.path.join(SAMPLES, "plan.json")
    src = os.path.join(SAMPLES, "example.json")
    db = os.path.join(WORK, "binding.sqlite3")
    out = rvj(store, "nested", "prepare", "--plan", plan_path, "--input",
              src, "--database", db)
    check(out["status"] == "complete", "binding fixture prepared")

    alt = dict(E.PLAN)
    alt["root"] = dict(E.PLAN["root"])
    alt["root"]["columns"] = list(E.PLAN["root"]["columns"]) + [
        {"name": "alias", "path": ["alias"], "type": "string"}]
    alt_path = write_sample("plan_bind_alt.json", json.dumps(alt))
    r = rv(store, "nested", "prepare", "--plan", alt_path, "--input", src,
           "--database", db, ok=(2,), tag="binding_alt")
    got = json.loads(r.stderr)
    check(got.get("plan_error") == "plan_mismatch",
          "the target database refuses a different plan", got)
    check(db_rows(db, "SELECT COUNT(*) c FROM participants")[0]["c"] == 1,
          "the refused plan did not alter the target database")


# --------------------------------------------------------------------------
# phase 9: many descendants under a memory limit
# --------------------------------------------------------------------------
def build_big_fixture(path):
    """3 roots; root 1 has BIG_VISITS visits each with BIG_READINGS readings.

    Written in small pieces so the demo never holds the huge JSON in RAM.
    Reading index i (global, across visits) has code C%05d and value i.
    """
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write("[")
        # root 0: one visit, one reading
        f.write('{"person":"B0","study":"S","visits":[{"label":"V0",'
                '"readings":[{"code":"C00000","value":0}]}],'
                '"consents":[{"kind":"k"}]},')
        # root 1: BIG_VISITS visits with BIG_READINGS readings each
        f.write('{"person":"B1","study":"S","visits":[')
        idx = 0
        for visit in range(E.BIG_VISITS):
            if visit:
                f.write(",")
            f.write('{"label":"V%d","readings":[' % visit)
            for r in range(E.BIG_READINGS):
                if r:
                    f.write(",")
                f.write('{"code":"C%05d","value":%d}' % (idx % 100000, idx))
                idx += 1
            f.write(']}')
        f.write('],"consents":[{"kind":"k"}]},')
        # root 2: one visit, one reading
        f.write('{"person":"B2","study":"S","visits":[{"label":"V0",'
                '"readings":[{"code":"C%05d","value":%d}]}],'
                '"consents":[{"kind":"k"}]}'
                % (idx % 100000, idx))
        f.write("]")


def phase_big():
    store = os.path.join(WORK, "store_big")
    plan_path = os.path.join(SAMPLES, "plan.json")
    src = os.path.join(SAMPLES, "big.json")
    db = os.path.join(WORK, "big.sqlite3")
    build_big_fixture(src)
    size = os.path.getsize(src)

    limit_kb = 200 * 1024
    probe = r'''
import json, os, resource, subprocess, sys
cmd = sys.argv[1:]
limit_bytes = %d
def limit():
    resource.setrlimit(resource.RLIMIT_AS, (limit_bytes, limit_bytes))
p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                     text=True, preexec_fn=limit)
out, err = p.communicate()
r = resource.getrusage(resource.RUSAGE_CHILDREN)
print(json.dumps({"rc": p.returncode, "stdout": out, "stderr": err,
                  "peak_rss_kb": r.ru_maxrss,
                  "vm_limit_kb": limit_bytes // 1024}))
''' % (limit_kb * 1024)

    env = dict(BASE_ENV)
    proc = subprocess.run(
        [sys.executable, "-c", probe, sys.executable, "-m", "rvault",
         "--store", store, "nested", "prepare", "--plan", plan_path,
         "--input", src, "--database", db],
        env=env, capture_output=True, text=True)
    if proc.returncode != 0:
        raise SystemExit("memory-limit probe failed:\n" + proc.stderr)
    result = json.loads(proc.stdout)
    log("big_probe.json", json.dumps(result, indent=2))
    check(result["rc"] == 0, "big preparation succeeds under the memory "
          "limit (rc=%s)" % result["rc"], result["stderr"][-500:])
    summary = json.loads(result["stdout"])
    check(summary["rows"]["readings"] == E.BIG_EXPECT["counts"]["readings"],
          "big: total reading rows", summary["rows"]["readings"])
    check(summary["rows"] == E.BIG_EXPECT["counts"],
          "big: every table's row count matches the hand-computed totals",
          summary["rows"])
    check(summary["status"] == "complete", "big: complete")
    print("     input %.1f MB, %d roots, one root with %d readings"
          % (size / 1e6, E.BIG_ROOTS,
             E.BIG_VISITS * E.BIG_READINGS))
    print("     process address-space limit %d kB, measured peak RSS %d kB"
          % (result["vm_limit_kb"], result["peak_rss_kb"]))
    verify(db, "big")

    # first/last child items through the public read interface
    lines = rv(store, "nested", "read", "--database", db, "--table",
               "readings", "--limit", "1").stdout.strip()
    first = json.loads(lines)
    check(first["values"]["code"]["value"] == "C00000"
          and first["values"]["value"]["value"] == "0",
          "big: first reading is code C00000 / value 0", first["values"])
    # the last reading: read the whole table in a bounded-memory child
    probe2 = r'''
import json, sqlite3, sys
conn = sqlite3.connect(sys.argv[1])
row = conn.execute(
    "SELECT code, value, value__present, parent_row_id, index_path "
    "FROM readings ORDER BY ordinal DESC LIMIT 1").fetchone()
print(json.dumps(row))
'''
    r = subprocess.run([sys.executable, "-c", probe2, db],
                       capture_output=True, text=True, env=BASE_ENV)
    last = json.loads(r.stdout)
    check(last[0] == E.BIG_EXPECT["last_reading"][0]
          and last[1] == E.BIG_EXPECT["last_reading"][1]
          and last[2] == 1,
          "big: last reading matches the hand-computed value", last)


# --------------------------------------------------------------------------
# phase 10: the older commands still run
# --------------------------------------------------------------------------
def phase_legacy():
    store = os.path.join(WORK, "store_legacy")
    rv(store, "init", tag="legacy_init")
    payload = (
        '{"study":"alpha","person":"p1","observation":"o1","edition":1,'
        '"display_name":"A","measure":10,"memo":""}\n').encode("utf-8")
    r = subprocess.run(
        [sys.executable, "-m", "rvault", "--store", store, "import",
         "--batch-tag", "b1"], input=payload, capture_output=True,
        env=BASE_ENV)
    log("legacy_import.out", r.stdout.decode())
    check(r.returncode == 0, "legacy import still works")
    out = rvj(store, "query", "--study", "alpha")
    check(len(out["observations"]) == 1
          and out["observations"][0]["measure"] == 10,
          "legacy query still works")
    # mail status on an empty store must still answer
    r = rv(store, "mail", "status", tag="legacy_mail_status")
    check(json.loads(r.stdout) is not None, "legacy mail status still works")
    # nested output never touched vault.sqlite3 tables
    vault = os.path.join(store, "vault.sqlite3")
    conn = sqlite3.connect(vault)
    try:
        names = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")]
    finally:
        conn.close()
    check("participants" not in names,
          "the nested subsystem created no payload table in vault.sqlite3")


# --------------------------------------------------------------------------
def main():
    os.makedirs(LOGS, exist_ok=True)
    os.makedirs(SYNC, exist_ok=True)
    print("work directory:", WORK)
    print("samples:       ", SAMPLES)

    phase_plan()
    phase_example()
    phase_late()
    phase_variants()
    phase_bad_roots()
    phase_resume()
    phase_binding()
    phase_big()
    phase_legacy()

    print()
    if _failures:
        print("%d FAILURES" % len(_failures))
        for f in _failures:
            print(" -", f)
        return 1
    print("NESTED DEMO COMPLETE")
    return 0


if __name__ == "__main__":
    sys.exit(main())
