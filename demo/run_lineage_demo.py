#!/usr/bin/env python3
"""End-to-end demo for revision-level data lineage.

Hand-computed small graph (all checks below are literal expectations, not
derived from the implementation):

    study alpha                         study beta
    -----------                         ----------
    raw1 e1 (leaf)  raw2 e1 (leaf)      raw3 e1 (UNKNOWN source)
      |  \            |                   |
      |   \           |                   |
      |    \          v                   |
      |     \------> out e1 [J1]          |
      |                |                  |
      |                |                  |
      |                v                  |
      +-------------> mid e1 [J1] <-------+
      |                |
      |                |
      +------+         |
      |      |         |
      v      v         v
    final e1 [J2]  ==  depends on {mid e1, out e1, raw1 e1}

  * two studies (alpha/beta), cross-study edges both ways
  * one multi-output job: J1 produces mid e1 AND out e1
  * diamond shared ancestor: raw1 is upstream of final via mid AND via out
    (depth 2 both) - and additionally via a SHORTER direct edge (depth 1);
    it must appear once, at distance 1, with the direct witness path
  * raw3 e1 has no declared provenance: it is "unknown", NOT an original leaf
  * two revisions of mid: e1 (declared) and e2 (arrives later, inherits
    nothing until J3 declares it)

The demo walks: bidirectional queries with distances and witness paths,
depth-limited truncation markers, edition upgrade, every rejection class
(with a database REOPEN after each to prove no residue), a racing pair of
opposite declarations, a query snapshot racing a concurrent registration,
withdrawal interplay, and a deep/heavily-shared graph for scale + memory.

Test data and run artifacts live in a fresh temporary directory (printed at
the start; override with RVAULT_LINEAGE_WORK).  Usage:

    python3 demo/run_lineage_demo.py
"""
from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import time

DEMO = os.path.dirname(os.path.abspath(__file__))
PROJ = os.path.dirname(DEMO)
WORK = os.environ.get("RVAULT_LINEAGE_WORK") or tempfile.mkdtemp(prefix="rvault-lineage-")
STORE = os.path.join(WORK, "store")
FIX = os.path.join(WORK, "fixtures")
LOGS = os.path.join(WORK, "logs")
SYNC = os.path.join(WORK, "sync")

BASE_ENV = dict(os.environ)
BASE_ENV["PYTHONPATH"] = PROJ

_step = 0
_failures = []


def log(name, text):
    with open(os.path.join(LOGS, name), "a", encoding="utf-8") as f:
        f.write(text)


def rv(*args, ok=(0,), tag=None, env=None):
    global _step
    _step += 1
    cmd = [sys.executable, "-m", "rvault", "--store", STORE] + list(args)
    r = subprocess.run(cmd, capture_output=True, text=True, env=env or BASE_ENV)
    name = "%03d_%s" % (_step, tag or (args[0] if args else "cmd"))
    log(name + ".out", r.stdout)
    log(name + ".err", r.stderr)
    if r.returncode not in ok:
        raise SystemExit("command failed rc=%d: %s\nstdout:\n%s\nstderr:\n%s"
                         % (r.returncode, " ".join(cmd), r.stdout, r.stderr))
    return r


def rvj(*args, **kw):
    return json.loads(rv(*args, **kw).stdout)


def check(cond, msg):
    if not cond:
        _failures.append(msg)
        print("FAIL", msg)
    else:
        print("ok  ", msg)


def write_jsonl(name, rows):
    path = os.path.join(FIX, name)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    return path


def imp(tag, rows, ok=(0,)):
    return rv("import", "--batch-tag", tag, "--input", write_jsonl(tag + ".jsonl", rows), ok=ok)


def obs(study, person, observation, edition, name, measure, memo):
    return {"study": study, "person": person, "observation": observation,
            "edition": edition, "display_name": name, "measure": measure,
            "memo": memo}


def declare(job, rule, rows, ok=(0,)):
    return rv("lineage", "declare", "--job", job, "--rule", rule,
              "--input", write_jsonl(job.replace("/", "_") + ".jsonl", rows), ok=ok)


def leaf(rows, ok=(0,)):
    return rv("lineage", "leaf", "--input", write_jsonl("leaf_%d.jsonl" % _step, rows), ok=ok)


def query(direction, node, max_depth=None, ok=(0,)):
    args = ["lineage", direction, "--node", json.dumps(node, ensure_ascii=False)]
    if max_depth is not None:
        args += ["--max-depth", str(max_depth)]
    r = rv(*args, ok=ok)
    lines = [json.loads(ln) for ln in r.stdout.splitlines() if ln.strip()]
    if not lines or lines[0]["type"] != "start" or lines[-1]["type"] != "end":
        raise SystemExit("malformed lineage stream: %r" % (lines,))
    return lines[0], lines[1:-1], lines[-1], r.stdout


def N(study, observation, edition):
    return {"study": study, "observation": observation, "edition": edition}


def node_tuple(rec):
    """(node, distance, provenance, truncated, path) for comparison."""
    return (json.dumps(rec["node"], sort_keys=True), rec["distance"],
            rec["provenance"], rec["truncated"],
            [json.dumps(p, sort_keys=True) for p in rec["path"]])


def expect_nodes(actual, expected, label):
    got = [node_tuple(r) for r in actual]
    exp = [(json.dumps(n, sort_keys=True), d, prov, tr,
            [json.dumps(p, sort_keys=True) for p in path])
           for (n, d, prov, tr, path) in expected]
    check(got == exp, "%s: nodes/distances/provenance/truncation/paths match "
          "the hand-computed expectation" % label)
    if got != exp:
        print("  expected: %s" % (exp,))
        print("  actual:   %s" % (got,))


# --------------------------------------------------------------------------
# independent database checks (own SQL, no rvault code), always on a FRESH
# connection: proves rejections leave no residue and committed rows match
# the hand-computed graph exactly.
# --------------------------------------------------------------------------
def db_counts():
    conn = sqlite3.connect(os.path.join(STORE, "vault.sqlite3"))
    try:
        jobs = conn.execute("SELECT COUNT(*) FROM ln_jobs").fetchone()[0]
        nodes = conn.execute("SELECT COUNT(*) FROM ln_nodes").fetchone()[0]
        edges = conn.execute("SELECT COUNT(*) FROM ln_edges").fetchone()[0]
        return jobs, nodes, edges
    finally:
        conn.close()


def check_counts(exp, label):
    got = db_counts()
    check(got == exp, "%s: (jobs, nodes, edges) == %s (reopened DB)" % (label, exp))


def db_edges():
    conn = sqlite3.connect(os.path.join(STORE, "vault.sqlite3"))
    try:
        return {(r[0], r[1], r[2], r[3], r[4], r[5], r[6]) for r in conn.execute(
            "SELECT out_study, out_observation, out_edition, "
            "       in_study, in_observation, in_edition, job_id FROM ln_edges")}
    finally:
        conn.close()


def db_nodes():
    conn = sqlite3.connect(os.path.join(STORE, "vault.sqlite3"))
    try:
        return {(r[0], r[1], r[2]): (r[3], r[4]) for r in conn.execute(
            "SELECT study, observation, edition, kind, job_id FROM ln_nodes")}
    finally:
        conn.close()


def feed_tail():
    return rvj("feed", "status")["tail_seq"]


# Hand-computed alpha projection after all imports (phase 4): lineage
# operations must never change observation values, so phase 10 compares the
# live projection against this literal.
EXPECTED_ALPHA_PROJECTION = [
    {"observation": "final", "person": "p-dan", "edition": 1,
     "display_name": "Dan", "measure": 40, "memo": "m4"},
    {"observation": "mid", "person": "p-cid", "edition": 2,
     "display_name": "Cid2", "measure": 31, "memo": "m3b"},
    {"observation": "n1", "person": "p-hal", "edition": 1,
     "display_name": "Hal", "measure": 1, "memo": "x"},
    {"observation": "n2", "person": "p-hal", "edition": 1,
     "display_name": "Hal", "measure": 2, "memo": "x"},
    {"observation": "n3", "person": "p-hal", "edition": 1,
     "display_name": "Hal", "measure": 3, "memo": "x"},
    {"observation": "obs 换\nline", "person": "p-hal", "edition": 1,
     "display_name": "Weird", "measure": 9, "memo": "x"},
    {"observation": "ok1", "person": "p-hal", "edition": 1,
     "display_name": "Hal", "measure": 5, "memo": "x"},
    {"observation": "opp1", "person": "p-ivy", "edition": 1,
     "display_name": "Ivy", "measure": 6, "memo": "x"},
    {"observation": "opp2", "person": "p-jay", "edition": 1,
     "display_name": "Jay", "measure": 7, "memo": "x"},
    {"observation": "raw1", "person": "p-ann", "edition": 1,
     "display_name": "Ann-α①", "measure": 10, "memo": "memo\n换行 one"},
    {"observation": "raw2", "person": "p-bob", "edition": 1,
     "display_name": "Bob", "measure": 20, "memo": "m2"},
    {"observation": "x1", "person": "p-hal", "edition": 1,
     "display_name": "Hal", "measure": 4, "memo": "x"},
]


# --------------------------------------------------------------------------
# sync gates
# --------------------------------------------------------------------------
def spawn(args, wait_points):
    os.makedirs(SYNC, exist_ok=True)
    env = dict(BASE_ENV)
    env["RVAULT_SYNC_DIR"] = SYNC
    env["RVAULT_SYNC_WAIT"] = ",".join(wait_points)
    return subprocess.Popen([sys.executable, "-m", "rvault", "--store", STORE] + args,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            text=True, env=env)


def wait_reached(name, timeout=60.0):
    path = os.path.join(SYNC, name + ".reached")
    deadline = time.time() + timeout
    while time.time() < deadline:
        if os.path.exists(path):
            return
        time.sleep(0.02)
    raise SystemExit("sync point never reached: " + name)


def release(name):
    with open(os.path.join(SYNC, name + ".proceed"), "w") as f:
        f.write("go")


# ==========================================================================
def phase_import():
    print("-- phase 1: import the base observations (two studies)")
    rv("init")
    imp("b1", [
        obs("alpha", "p-ann", "raw1", 1, "Ann-α①", 10, "memo\n换行 one"),
        obs("alpha", "p-bob", "raw2", 1, "Bob", 20, "m2"),
        obs("alpha", "p-cid", "mid", 1, "Cid", 30, "m3"),
        obs("alpha", "p-dan", "final", 1, "Dan", 40, "m4"),
        obs("beta", "p-eli", "raw3", 1, "Eli-SECRET-①", 50, "m5"),
        obs("beta", "p-fay", "out", 1, "Fay", 60, "m6"),
    ])
    imp("b2", [
        obs("alpha", "p-hal", "n1", 1, "Hal", 1, "x"),
        obs("alpha", "p-hal", "n2", 1, "Hal", 2, "x"),
        obs("alpha", "p-hal", "n3", 1, "Hal", 3, "x"),
        obs("alpha", "p-hal", "x1", 1, "Hal", 4, "x"),
        obs("alpha", "p-hal", "ok1", 1, "Hal", 5, "x"),
        obs("alpha", "p-ivy", "opp1", 1, "Ivy", 6, "x"),
        obs("alpha", "p-jay", "opp2", 1, "Jay", 7, "x"),
        obs("beta", "p-gil", "w1", 1, "Gil", 8, "x"),
        # identity strings with space / newline / non-ASCII
        obs("alpha", "p-hal", "obs 换\nline", 1, "Weird", 9, "x"),
    ])
    check_counts((0, 0, 0), "after import: no lineage yet")


def phase_declare():
    print("-- phase 2: leaf declarations + multi-output job J1 + job J2")
    tail0 = feed_tail()
    r = leaf([N("alpha", "raw1", 1), N("alpha", "raw2", 1)])
    check(json.loads(r.stdout)["declared"] == 2, "two original leaves declared")
    r = declare("J1", "R1", [
        {"output": N("alpha", "mid", 1),
         "inputs": [N("alpha", "raw1", 1), N("beta", "raw3", 1)]},
        {"output": N("beta", "out", 1),
         "inputs": [N("alpha", "raw1", 1), N("alpha", "raw2", 1)]},
    ])
    j = json.loads(r.stdout)
    check(j["status"] == "recorded" and j["outputs"] == 2 and j["edges"] == 4,
          "J1: one job, two outputs, four edges (cross-study)")
    r = declare("J2", "R2", [
        {"output": N("alpha", "final", 1),
         "inputs": [N("alpha", "mid", 1), N("beta", "out", 1), N("alpha", "raw1", 1)]},
    ])
    check(json.loads(r.stdout)["edges"] == 3, "J2: final depends on mid, out, raw1")
    check_counts((2, 5, 7), "after J1+J2")
    check(feed_tail() == tail0, "lineage registration emits no change-feed events")


def phase_queries():
    print("-- phase 3: hand-computed bidirectional queries")
    final, mid, out = N("alpha", "final", 1), N("alpha", "mid", 1), N("beta", "out", 1)
    raw1, raw2, raw3 = N("alpha", "raw1", 1), N("alpha", "raw2", 1), N("beta", "raw3", 1)

    start, nodes, end, _ = query("upstream", final)
    check(start["provenance"] == "derived" and start["job_id"] == "J2"
          and start["rule_id"] == "R2" and start["state"] == "active",
          "upstream final: start status derived/J2/R2/active")
    expect_nodes(nodes, [
        (mid, 1, "derived", False, [final, mid]),
        (raw1, 1, "leaf", False, [final, raw1]),          # shorter alternative path wins
        (out, 1, "derived", False, [final, out]),
        (raw2, 2, "leaf", False, [final, out, raw2]),
        (raw3, 2, "unknown", False, [final, mid, raw3]),  # unknown source, not "original"
    ], "upstream final (unlimited)")
    check(end["nodes"] == 5 and end["complete"] is True
          and end["depth_limited"] is False,
          "upstream final: 5 nodes, complete, not depth-limited "
          "(diamond ancestor raw1 appears ONCE at distance 1)")

    start, nodes, end, _ = query("upstream", final, max_depth=1)
    expect_nodes(nodes, [
        (mid, 1, "derived", True, [final, mid]),    # truncated: has inputs beyond depth
        (raw1, 1, "leaf", False, [final, raw1]),    # leaf: nothing to expand
        (out, 1, "derived", True, [final, out]),
    ], "upstream final (max-depth 1)")
    check(end["depth_limited"] is True, "max-depth 1: depth_limited set")

    start, nodes, end, _ = query("upstream", final, max_depth=0)
    check(nodes == [] and start["provenance"] == "derived"
          and end["nodes"] == 0 and end["depth_limited"] is True,
          "max-depth 0: only start status; derived start is NOT reported as "
          "'no dependencies'")

    start, nodes, end, _ = query("upstream", raw3)
    check(start["provenance"] == "unknown" and nodes == []
          and end["depth_limited"] is False,
          "raw3: unknown source (absence of parents is NOT 'original data')")

    start, nodes, end, _ = query("upstream", mid)
    expect_nodes(nodes, [
        (raw1, 1, "leaf", False, [mid, raw1]),
        (raw3, 1, "unknown", False, [mid, raw3]),
    ], "upstream mid")

    start, nodes, end, _ = query("downstream", raw1)
    check(start["provenance"] == "leaf" and start["has_dependents"] is True,
          "downstream raw1: start is a leaf with dependents")
    expect_nodes(nodes, [
        (final, 1, "derived", False, [raw1, final]),
        (mid, 1, "derived", False, [raw1, mid]),
        (out, 1, "derived", False, [raw1, out]),
    ], "downstream raw1 (unlimited)")

    start, nodes, end, _ = query("downstream", raw1, max_depth=1)
    expect_nodes(nodes, [
        (final, 1, "derived", False, [raw1, final]),  # final has no dependents
        (mid, 1, "derived", True, [raw1, mid]),       # mid does (final)
        (out, 1, "derived", True, [raw1, out]),       # out does (final)
    ], "downstream raw1 (max-depth 1): truncation per node")

    start, nodes, end, _ = query("downstream", raw1, max_depth=0)
    check(nodes == [] and start["has_dependents"] is True
          and end["depth_limited"] is True,
          "downstream max-depth 0: has_dependents reported, not 'no dependents'")

    start, nodes, end, _ = query("downstream", raw3)
    expect_nodes(nodes, [
        (mid, 1, "derived", False, [raw3, mid]),
        (final, 2, "derived", False, [raw3, mid, final]),
    ], "downstream raw3: pipeline maintainer sees what to recompute")

    start, nodes, end, _ = query("downstream", raw2)
    expect_nodes(nodes, [
        (out, 1, "derived", False, [raw2, out]),
        (final, 2, "derived", False, [raw2, out, final]),
    ], "downstream raw2")

    start, nodes, end, _ = query("downstream", final)
    check(nodes == [] and start["has_dependents"] is False
          and end["depth_limited"] is False,
          "downstream final: nothing depends on the final result")


def phase_edition_upgrade():
    print("-- phase 4: a newer edition inherits nothing from the old one")
    imp("b3", [obs("alpha", "p-cid", "mid", 2, "Cid2", 31, "m3b")])
    start, nodes, end, _ = query("upstream", N("alpha", "mid", 2))
    check(start["provenance"] == "unknown" and nodes == [],
          "mid e2: brand-new edition has UNKNOWN provenance (no inheritance "
          "from mid e1's edges)")
    declare("J3", "R1", [{"output": N("alpha", "mid", 2),
                          "inputs": [N("alpha", "raw2", 1)]}])
    start, nodes, end, _ = query("upstream", N("alpha", "mid", 2))
    expect_nodes(nodes, [(N("alpha", "raw2", 1), 1, "leaf", False,
                          [N("alpha", "mid", 2), N("alpha", "raw2", 1)])],
                 "mid e2 after J3: its own edge only")
    start, nodes, end, _ = query("upstream", N("alpha", "mid", 1))
    check(end["nodes"] == 2, "mid e1 keeps its own edges (editions independent)")
    start, nodes, end, _ = query("downstream", N("alpha", "raw2", 1))
    expect_nodes(nodes, [
        (N("alpha", "mid", 2), 1, "derived", False, [N("alpha", "raw2", 1), N("alpha", "mid", 2)]),
        (N("beta", "out", 1), 1, "derived", False, [N("alpha", "raw2", 1), N("beta", "out", 1)]),
        (N("alpha", "final", 1), 2, "derived", False,
         [N("alpha", "raw2", 1), N("beta", "out", 1), N("alpha", "final", 1)]),
    ], "downstream raw2 now includes mid e2")
    check_counts((3, 6, 8), "after edition upgrade")


def phase_rejections():
    print("-- phase 5: rejection battery; the DB is REOPENED after each one")
    exp = (3, 6, 8)

    r = leaf([N("alpha", "mid", 1)], ok=(2,))
    check(json.loads(r.stderr)["code"] == "leaf_conflicts_with_derived",
          "leaf on a derived output rejected")
    check_counts(exp, "after leaf/derived conflict")

    r = declare("J4", "R4", [{"output": N("alpha", "raw1", 1),
                              "inputs": [N("alpha", "raw2", 1)]}], ok=(2,))
    check(json.loads(r.stderr)["code"] == "output_already_claimed",
          "derived on a declared leaf rejected")
    check_counts(exp, "after derived/leaf conflict")

    r = declare("J1", "R1", [{"output": N("alpha", "mid", 1),
                              "inputs": [N("alpha", "raw1", 1)]}], ok=(2,))
    check(json.loads(r.stderr)["code"] == "job_conflict",
          "same job id, changed relation rejected")
    check_counts(exp, "after changed-content resend")

    r = declare("J1", "R9", [
        {"output": N("alpha", "mid", 1),
         "inputs": [N("alpha", "raw1", 1), N("beta", "raw3", 1)]},
        {"output": N("beta", "out", 1),
         "inputs": [N("alpha", "raw1", 1), N("alpha", "raw2", 1)]}], ok=(2,))
    check(json.loads(r.stderr)["code"] == "job_conflict",
          "same job id, same relation but different rule rejected")
    check_counts(exp, "after changed-rule resend")

    # identical resend: reordered + duplicated inputs are equivalent
    r = declare("J1", "R1", [
        {"output": N("beta", "out", 1),
         "inputs": [N("alpha", "raw2", 1), N("alpha", "raw1", 1), N("alpha", "raw1", 1)]},
        {"output": N("alpha", "mid", 1),
         "inputs": [N("beta", "raw3", 1), N("alpha", "raw1", 1)]}])
    check(json.loads(r.stdout)["status"] == "duplicate",
          "identical normalized relation resend is an idempotent duplicate")
    check_counts(exp, "after idempotent resend")

    r = declare("J5", "R5", [
        {"output": N("alpha", "n1", 1), "inputs": [N("alpha", "n2", 1)]},
        {"output": N("alpha", "n2", 1), "inputs": [N("alpha", "n3", 1)]},
        {"output": N("alpha", "n3", 1), "inputs": [N("alpha", "n1", 1)]}], ok=(2,))
    e = json.loads(r.stderr)
    check(e["code"] == "cycle_detected" and len(e["cycle_nodes"]) == 3,
          "in-batch 3-cycle rejected (merged relation judged as a whole)")
    check_counts(exp, "after in-batch cycle")

    r = declare("J6", "R6", [{"output": N("beta", "raw3", 1),
                              "inputs": [N("alpha", "final", 1)]}], ok=(2,))
    check(json.loads(r.stderr)["code"] == "cycle_detected",
          "cross-job cycle raw3->final->mid->raw3 rejected")
    check_counts(exp, "after cross-job cycle")

    r = declare("J7", "R7", [{"output": N("alpha", "x1", 1),
                              "inputs": [N("alpha", "x1", 1)]}], ok=(2,))
    check(json.loads(r.stderr)["code"] == "self_dependency",
          "self-dependency rejected")
    check_counts(exp, "after self-dependency")

    r = declare("J8", "R8", [
        {"output": N("alpha", "ok1", 1), "inputs": [N("alpha", "raw1", 1)]},
        {"output": N("alpha", "ghost", 1), "inputs": [N("alpha", "raw1", 1)]}], ok=(2,))
    check(json.loads(r.stderr)["code"] == "unknown_revision",
          "multi-output job with one nonexistent revision rejected")
    check_counts(exp, "after atomicity test (ok1 must NOT be recorded)")
    nodes = db_nodes()
    check(("alpha", "ok1", 1) not in nodes and all(e[5] != "J8" for e in db_edges()),
          "one illegal item: no job row, no node provenance, no partial edges")

    r = declare("J9", "R9", [{"output": N("alpha", "mid", 1),
                              "inputs": [N("alpha", "raw2", 1)]}], ok=(2,))
    e = json.loads(r.stderr)
    check(e["code"] == "output_already_claimed" and e["existing_job"] == "J1",
          "another job re-claiming an explained output rejected")
    check_counts(exp, "after re-claim attempt")

    r = declare("J10", "R10", [{"output": N("alpha", "n1", 1), "inputs": []}], ok=(2,))
    check(json.loads(r.stderr)["code"] == "invalid_declaration",
          "empty input list rejected")
    r = declare("J10", "R10", [
        {"output": N("alpha", "n1", 1), "inputs": [N("alpha", "raw1", 1)]},
        {"output": N("alpha", "n1", 1), "inputs": [N("alpha", "raw2", 1)]}], ok=(2,))
    check(json.loads(r.stderr)["code"] == "invalid_declaration",
          "same output twice in one job rejected")
    check_counts(exp, "after malformed declarations")


def phase_weird_identities():
    print("-- phase 5b: identity strings with space/newline/non-ASCII round-trip")
    weird = "obs 换\nline"
    leaf([N("alpha", weird, 1)])
    declare("J 换/①", "R 换", [{"output": N("alpha", "x1", 1),
                                "inputs": [N("alpha", weird, 1)]}])
    start, nodes, end, _ = query("upstream", N("alpha", "x1", 1))
    check(len(nodes) == 1 and nodes[0]["node"] == N("alpha", weird, 1)
          and nodes[0]["provenance"] == "leaf",
          "non-ASCII/newline observation name round-trips unambiguously")
    start, nodes, end, _ = query("downstream", N("alpha", weird, 1))
    check(len(nodes) == 1 and nodes[0]["node"] == N("alpha", "x1", 1)
          and nodes[0]["job_id"] == "J 换/①",
          "job id with space/non-ASCII round-trips")
    jobs = rvj("lineage", "jobs")["jobs"]
    check(any(j["job_id"] == "J 换/①" and j["rule_id"] == "R 换" for j in jobs),
          "jobs listing carries the exact identity strings")
    check_counts((4, 8, 9), "after weird-identity declarations")


def phase_race():
    print("-- phase 6: two opposite declarations race; no merged cycle")
    ja = [{"output": N("alpha", "opp1", 1), "inputs": [N("alpha", "opp2", 1)]}]
    jb = [{"output": N("alpha", "opp2", 1), "inputs": [N("alpha", "opp1", 1)]}]
    pa = spawn(["lineage", "declare", "--job", "JA", "--rule", "RA",
                "--input", write_jsonl("ja.jsonl", ja)],
               ["lineage.declare.before_commit"])
    wait_reached("lineage.declare.before_commit")   # A validated, holds the write lock
    pb = spawn(["lineage", "declare", "--job", "JB", "--rule", "RB",
                "--input", write_jsonl("jb.jsonl", jb)], [])
    time.sleep(1.0)                                  # B blocks on BEGIN IMMEDIATE
    check(pb.poll() is None, "opposite declaration blocks while A holds the "
          "write lock (cannot check against a stale snapshot)")
    release("lineage.declare.before_commit")
    out_a, err_a = pa.communicate()
    out_b, err_b = pb.communicate()
    check(pa.returncode == 0 and json.loads(out_a)["status"] == "recorded",
          "A (opp1<-opp2) commits")
    check(pb.returncode == 2 and json.loads(err_b)["code"] == "cycle_detected",
          "B (opp2<-opp1) is rejected: the two directions never merge into a cycle")
    check_counts((5, 9, 10), "after the race (only JA recorded)")
    jobs = {j["job_id"] for j in rvj("lineage", "jobs")["jobs"]}
    check("JA" in jobs and "JB" not in jobs, "JB left no job row behind")


def phase_query_snapshot():
    print("-- phase 7: one query, one consistent point in time")
    pq = spawn(["lineage", "downstream", "--node",
                json.dumps(N("alpha", "raw1", 1))], ["lineage.query.after_start"])
    wait_reached("lineage.query.after_start")        # query read the start node
    pd = spawn(["lineage", "declare", "--job", "JD", "--rule", "RD", "--input",
                write_jsonl("jd.jsonl", [{"output": N("alpha", "n1", 1),
                                          "inputs": [N("alpha", "raw1", 1)]}])], [])
    time.sleep(1.0)                                  # JD blocks at COMMIT
    check(pd.poll() is None, "concurrent declaration waits for the open read "
          "transaction")
    release("lineage.query.after_start")
    out_q, err_q = pq.communicate()
    out_d, err_d = pd.communicate()
    check(pq.returncode == 0 and pd.returncode == 0, "both processes finish")
    lines = [json.loads(ln) for ln in out_q.splitlines() if ln.strip()]
    names = [r["node"]["observation"] for r in lines if r["type"] == "node"]
    check("n1" not in names and len(names) == 3,
          "the in-flight answer does NOT mix in the concurrently registered "
          "relation (single snapshot)")
    check(pd.returncode == 0 and json.loads(out_d)["status"] == "recorded",
          "JD commits once the query's snapshot is released")
    start, nodes, end, _ = query("downstream", N("alpha", "raw1", 1))
    check(end["nodes"] == 4 and any(r["node"]["observation"] == "n1" for r in nodes),
          "a fresh query sees JD's relation")
    check_counts((6, 10, 11), "after JD")


def phase_withdrawal():
    print("-- phase 8: withdrawal marks nodes but never rewrites the graph")
    rv("withdraw", "register", "--withdrawal-id", "W1", "--study", "beta",
       "--person", "p-eli", "--person", "p-gil")
    conn = sqlite3.connect(os.path.join(STORE, "vault.sqlite3"))
    gone = conn.execute("SELECT COUNT(*) FROM editions WHERE observation='raw3'").fetchone()[0]
    conn.close()
    check(gone == 0, "raw3 payload rows erased by the withdrawal")

    start, nodes, end, raw = query("upstream", N("alpha", "final", 1))
    by_name = {r["node"]["observation"]: r for r in nodes}
    check(end["nodes"] == 5 and by_name["raw3"]["state"] == "withdrawn"
          and by_name["raw3"]["provenance"] == "unknown",
          "withdrawn node still explains the graph, marked withdrawn")
    check(all(by_name[n]["state"] == "active" for n in ("mid", "out", "raw1", "raw2")),
          "other nodes stay active")
    check("Eli-SECRET-①" not in raw and "p-eli" not in raw and "m5" not in raw,
          "traceback never reads withdrawn payload back (no name/memo/person)")

    start, nodes, end, _ = query("downstream", N("beta", "raw3", 1))
    check(start["state"] == "withdrawn" and end["nodes"] == 2,
          "downstream from a withdrawn node still follows identity relations")

    exp = (6, 10, 11)
    r = declare("JW1", "RW", [{"output": N("alpha", "ok1", 1),
                               "inputs": [N("beta", "raw3", 1)]}], ok=(2,))
    check(json.loads(r.stderr)["code"] == "revision_withdrawn",
          "withdrawn revision as INPUT rejected")
    r = declare("JW2", "RW", [{"output": N("beta", "w1", 1),
                               "inputs": [N("alpha", "raw1", 1)]}], ok=(2,))
    check(json.loads(r.stderr)["code"] == "revision_withdrawn",
          "withdrawn revision as OUTPUT rejected")
    r = leaf([N("beta", "w1", 1)], ok=(2,))
    check(json.loads(r.stderr)["code"] == "revision_withdrawn",
          "leaf declaration on a withdrawn revision rejected")
    r = declare("JW3", "RW", [{"output": N("alpha", "n2", 1),
                               "inputs": [N("beta", "w1", 1)]}], ok=(2,))
    check(json.loads(r.stderr)["code"] == "revision_withdrawn",
          "withdrawn input among others rejected")
    check_counts(exp, "after withdrawn-registration rejections")

    # lineage tables carry identifiers only: no payload strings anywhere
    conn = sqlite3.connect(os.path.join(STORE, "vault.sqlite3"))
    texts = set()
    for table in ("ln_jobs", "ln_nodes", "ln_edges"):
        for row in conn.execute("SELECT * FROM " + table):
            texts.update(str(v) for v in row if isinstance(v, str))
    conn.close()
    payload = {"Ann-α①", "Bob", "Cid", "Cid2", "Dan", "Eli-SECRET-①", "Fay",
               "Gil", "Hal", "Ivy", "Jay", "Weird", "memo\n换行 one", "m2",
               "m3", "m4", "m5", "m6", "p-ann", "p-bob", "p-cid", "p-dan",
               "p-eli", "p-fay", "p-gil", "p-hal", "p-ivy", "p-jay"}
    check(not (texts & payload), "lineage tables contain no names, measures, "
          "memos or person ids")


def phase_scale():
    print("-- phase 9: deep graph with massive shared ancestors (scale + memory)")
    layers, width = 60, 10
    rows = [obs("deep", "p-deep", "L%d_N%d" % (i, j), 1, "D", 1, "x")
            for i in range(layers) for j in range(width)]
    imp("deep", rows)
    leaf([N("deep", "L0_N%d" % j, 1) for j in range(width)])
    rel = [{"output": N("deep", "L%d_N%d" % (i, j), 1),
            "inputs": [N("deep", "L%d_N%d" % (i - 1, k), 1) for k in range(width)]}
           for i in range(1, layers) for j in range(width)]
    r = declare("JDEEP", "RD", rel)
    j = json.loads(r.stdout)
    check(j["outputs"] == 590 and j["edges"] == 5900,
          "one job with 590 outputs / 5900 edges recorded")
    check_counts((7, 610, 5911), "after deep graph")

    # hand-computed: upstream of L59_N0 = layers 0..58 = 590 nodes;
    # layer i sits at distance 59-i; every shared ancestor appears once.
    start, nodes, end, raw = query("upstream", N("deep", "L59_N0", 1))
    check(end["nodes"] == 590 and end["complete"] is True,
          "upstream visits 590 nodes (not 10^59 paths)")
    uniq = {(r["node"]["observation"]) for r in nodes}
    check(len(uniq) == 590, "every shared ancestor appears exactly once")
    dist = {r["node"]["observation"]: r["distance"] for r in nodes}
    check(dist["L0_N0"] == 59 and dist["L58_N0"] == 1 and dist["L10_N3"] == 49,
          "distances are true BFS depths")
    w = next(r for r in nodes if r["node"]["observation"] == "L10_N3")
    exp_path = [N("deep", "L%d_N0" % i, 1) for i in range(59, 10, -1)] + [N("deep", "L10_N3", 1)]
    check(w["path"] == exp_path,
          "witness path follows the deterministic smallest-parent rule")
    check(len(raw.splitlines()) == 592, "stream = 1 start + 590 nodes + 1 end")

    start, nodes, end, _ = query("downstream", N("deep", "L0_N0", 1))
    check(end["nodes"] == 590, "downstream visits the same 590 nodes")

    # peak memory of one query process, measured by the process itself
    probe = os.path.join(WORK, "mem_probe.py")
    with open(probe, "w") as f:
        f.write(
            "import json, resource, sys\n"
            "sys.path.insert(0, %r)\n"
            "from rvault.store import Store\n"
            "from rvault import lineage\n"
            "store = Store(%r)\n"
            "n = 0\n"
            "for line in lineage.traverse(store, ('deep', 'L59_N0', 1), None, 'upstream'):\n"
            "    if json.loads(line)['type'] == 'node':\n"
            "        n += 1\n"
            "print(json.dumps({'nodes': n,\n"
            "  'peak_rss_kb': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}))\n"
            % (PROJ, STORE))
    r = subprocess.run([sys.executable, probe], capture_output=True, text=True)
    mem = json.loads(r.stdout)
    check(mem["nodes"] == 590, "in-process traversal count matches")
    print("    scale: 600 nodes / 5900 edges in the deep graph; "
          "paths L59->L0 would be 10^59")
    print("    query output: 590 node records; peak RSS of the query "
          "process: %d KB" % mem["peak_rss_kb"])
    check(mem["peak_rss_kb"] < 64 * 1024,
          "bounded memory: peak RSS stays flat (visited set on disk, no path "
          "enumeration)")


def phase_final_verify():
    print("-- phase 10: independent SQL verification of the whole graph")
    # lineage operations never touch observation values: the alpha projection
    # captured right after the imports must be byte-identical now
    proj = rvj("query", "--study", "alpha")["observations"]
    check(proj == EXPECTED_ALPHA_PROJECTION,
          "observation values unchanged by lineage registration and queries")
    exp_edges = {
        ("alpha", "mid", 1, "alpha", "raw1", 1, "J1"),
        ("alpha", "mid", 1, "beta", "raw3", 1, "J1"),
        ("beta", "out", 1, "alpha", "raw1", 1, "J1"),
        ("beta", "out", 1, "alpha", "raw2", 1, "J1"),
        ("alpha", "final", 1, "alpha", "mid", 1, "J2"),
        ("alpha", "final", 1, "beta", "out", 1, "J2"),
        ("alpha", "final", 1, "alpha", "raw1", 1, "J2"),
        ("alpha", "mid", 2, "alpha", "raw2", 1, "J3"),
        ("alpha", "x1", 1, "alpha", "obs 换\nline", 1, "J 换/①"),
        ("alpha", "opp1", 1, "alpha", "opp2", 1, "JA"),
        ("alpha", "n1", 1, "alpha", "raw1", 1, "JD"),
    }
    edges = db_edges()
    small = {e for e in edges if e[0] != "deep"}
    check(small == exp_edges, "small-graph edge set matches the hand-computed "
          "11 edges exactly (reopened DB)")
    check(len([e for e in edges if e[0] == "deep"]) == 5900,
          "deep graph edge count")
    nodes = db_nodes()
    check(nodes[("alpha", "raw1", 1)] == ("leaf", None)
          and nodes[("alpha", "final", 1)] == ("derived", "J2")
          and ("beta", "raw3", 1) not in nodes,
          "node kinds: leaf/derived recorded, raw3 never claimed")
    check(rvj("bundle", "list")["bundles"] == [],
          "no bundles were created by lineage operations")


def main():
    for d in (FIX, LOGS, SYNC):
        os.makedirs(d, exist_ok=True)
    print("work dir: %s" % WORK)
    phase_import()
    phase_declare()
    phase_queries()
    phase_edition_upgrade()
    phase_rejections()
    phase_weird_identities()
    phase_race()
    phase_query_snapshot()
    phase_withdrawal()
    phase_scale()
    phase_final_verify()
    print("=" * 60)
    if _failures:
        print("FAILURES (%d):" % len(_failures))
        for f in _failures:
            print("  -", f)
        return 1
    print("all lineage checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
