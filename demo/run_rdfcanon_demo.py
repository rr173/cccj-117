#!/usr/bin/env python3
"""End-to-end demo for the offline RDF dataset canonicalization entry points.

Everything (inputs derived from the fixtures, canonical files, reports,
permutation logs) is written under one fresh temporary directory; the
repository fixtures are only read.  The script drives the real CLI
(``python3 -m rvault rdf ...``) as an administrator would, and checks results
with ``demo/verify_rdfcanon.py`` - an independent parser that never imports
the production module.

Phases
  1  same catalogue, renamed blank labels + shuffled + duplicated statements:
     equal canonical bytes, equal SHA-256, correspondence maps every quad;
  2  one 6-node directed cycle vs two disjoint 3-node cycles: same node count,
     same degree profile, same quad count -> must report *different*;
  3  blank graph name also used as an ordinary term, self-loop, non-ASCII,
     escaped text, language and datatype literals: read back and inspect;
  4  empty dataset: canonical bytes are empty, digest is SHA-256 of nothing;
  5  official W3C RDFC-1.0 tests (>=12 positive SHA-256 cases): byte-exact
     against upstream expected files, plus random blank-label and line-order
     permutations compared with the *same* official expected output;
  6  resource limit on a symmetric graph with a tiny max_work: reported as a
     failure (not "different"), previous output file bytes preserved; raising
     the budget completes the same input; a file with a broken last line is
     rejected and does not overwrite an existing output;
  7  existing intake / withdrawal / nested / BPE command paths still run.

Usage:  python3 demo/run_rdfcanon_demo.py
"""
from __future__ import annotations

import hashlib
import json
import os
import random
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
FIX = os.path.join(HERE, "rdfc10_tests")
sys.path.insert(0, HERE)
sys.path.insert(0, ROOT)

import verify_rdfcanon as V  # noqa: E402

PHASE = 0
LOG_LINES = []
FAILURES = []
ENV = dict(os.environ)
ENV["PYTHONPATH"] = ROOT + os.pathsep + ENV.get("PYTHONPATH", "")

OFFICIAL_TESTS = [
    "test020", "test022", "test023", "test024", "test025", "test030",
    "test033", "test043", "test057", "test058", "test060", "test061",
    "test062", "test070", "test071", "test072", "test073", "test076",
]
CROSS_GRAPH_TESTS = {"test057", "test058", "test070", "test071",
                     "test072", "test073"}


def log(msg):
    print(msg)
    LOG_LINES.append(msg)


def phase(title):
    global PHASE
    PHASE += 1
    log("\n=== phase %d: %s ===" % (PHASE, title))


def check(cond, msg):
    if cond:
        log("ok   " + msg)
    else:
        FAILURES.append(msg)
        log("FAIL " + msg)


def run(*args, expect=0):
    cmd = [sys.executable, "-m", "rvault"] + list(args)
    p = subprocess.run(cmd, cwd=ROOT, env=ENV, capture_output=True, text=True)
    if p.returncode != expect:
        raise AssertionError("command %r returned %d (expected %d)\n"
                             "stdout:\n%s\nstderr:\n%s"
                             % (cmd, p.returncode, expect, p.stdout, p.stderr))
    return p


def run_json(*args, expect=0):
    p = run(*args, expect=expect)
    return json.loads(p.stdout if p.stdout.strip() else p.stderr)


def sha(path):
    return V.sha256_file(path)


def copy(src, dst):
    shutil.copyfile(src, dst)
    return dst


# ---------------------------------------------------------------------------
# deterministic-but-random blank-label and line permutations of a fixture
# ---------------------------------------------------------------------------

def _labels_outside_terms(line):
    """Positions of '_:...' occurrences that are real blank node terms."""
    found = []
    i = 0
    while i < len(line):
        ch = line[i]
        if ch == "<":
            j = line.index(">", i)
            i = j + 1
            continue
        if ch == '"':
            i += 1
            while i < len(line):
                if line[i] == "\\":
                    i += 2
                    continue
                if line[i] == '"':
                    i += 1
                    break
                i += 1
            continue
        if line.startswith("_:", i):
            j = i + 2
            while j < len(line) and line[j] not in " \t\r\n":
                j += 1
            # a trailing '.' belongs to the statement, not the label
            end = j
            while end > i + 2 and line[end - 1] == ".":
                end -= 1
            found.append((i, end))
            i = j
            continue
        i += 1
    return found


def permuted_input(path, seed, add_duplicates=False):
    """Rename every blank label consistently, then shuffle statement lines."""
    rng = random.Random(seed)
    with open(path, "r", encoding="utf-8") as f:
        raw_lines = f.read().split("\n")
    if raw_lines and raw_lines[-1] == "":
        raw_lines.pop()
    data_lines = [l for l in raw_lines if l.strip() and
                  not l.lstrip().startswith("#")]
    labels = []
    for line in data_lines:
        for start, end in _labels_outside_terms(line):
            label = line[start + 2:end]
            if label not in labels:
                labels.append(label)
    alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    new_labels = {}
    used = set()
    for label in labels:
        while True:
            cand = "".join(rng.choice(alphabet)
                           for _ in range(rng.randint(2, 12)))
            if cand not in used:
                used.add(cand)
                break
        new_labels[label] = cand
    out = []
    for line in data_lines:
        pieces = []
        pos = 0
        for start, end in _labels_outside_terms(line):
            pieces.append(line[pos:start])
            pieces.append("_:" + new_labels[line[start + 2:end]])
            pos = end
        pieces.append(line[pos:])
        out.append("".join(pieces))
    if add_duplicates:
        out = out + [out[rng.randrange(len(out))]
                     for _ in range(max(1, len(out) // 4))]
    rng.shuffle(out)
    return "\n".join(out) + "\n", new_labels


# ---------------------------------------------------------------------------
# phases
# ---------------------------------------------------------------------------

def phase_same_catalogue(work):
    phase("same catalogue: renamed blank labels, shuffled and duplicated")
    case = os.path.join(FIX, "cases", "case1_rename")
    left_out = os.path.join(work, "case1_left.nq")
    right_out = os.path.join(work, "case1_right.nq")
    p = run_json("rdf", "normalize", "--input",
                 os.path.join(case, "left.nq"), "--output", left_out)
    check(p["quad_count"] == 6 and p["blank_nodes"] == 3,
          "left: 6 quads, 3 blank nodes")
    check(p["duplicate_occurrences"] == 0, "left has no duplicate lines")
    check(p["work"]["used"] == 0,
          "left needs no N-degree search (all first-degree hashes unique)")
    p2 = run_json("rdf", "normalize", "--input",
                  os.path.join(case, "right.nq"), "--output", right_out)
    check(p2["quad_count"] == 6,
          "right: duplicate statements collapsed to 6 quads "
          "(input had %d occurrences)" % p2["input_quad_occurrences"])
    check(p2["duplicate_occurrences"] == 3,
          "right: 3 repeated statements counted but not re-canonicalized")
    check(p["sha256"] == p2["sha256"],
          "both exports carry the same SHA-256: " + p["sha256"][:16] + "...")
    check(open(left_out, "rb").read() == open(right_out, "rb").read(),
          "canonical files are byte-for-byte identical")
    check(sha(left_out) == sha(right_out) == p["sha256"],
          "digest matches the bytes actually written")

    cmp_report = run_json("rdf", "compare", "--left",
                          os.path.join(case, "left.nq"), "--right",
                          os.path.join(case, "right.nq"))
    check(cmp_report["status"] == "equivalent", "compare reports equivalent")
    corr = cmp_report["blank_node_correspondence"]
    check(sorted(corr) == ["g", "x", "y"],
          "correspondence names the left input labels: %s" % sorted(corr))
    check(len(set(corr.values())) == 3,
          "correspondence is one-to-one")

    # Substitute the correspondence into the left dataset and require that
    # exactly the right dataset results (independent parser).
    left_quads = V.parse(os.path.join(case, "left.nq"))
    right_quads = V.parse(os.path.join(case, "right.nq"))
    mapped = V.apply_map(left_quads, corr)
    check(mapped == right_quads,
          "correspondence substituted into left reproduces every right quad")
    # And the reverse direction loses nothing.
    inverse = {v: k for k, v in corr.items()}
    check(V.apply_map(right_quads, inverse) == left_quads,
          "inverse correspondence reproduces every left quad")

    # The normalized file itself is a valid canonical N-Quads document, and
    # substituting the *reported* map into the input yields its quads.
    V.assert_canonical_form(left_out)
    run_map = p["blank_node_map"]
    out_quads = V.parse(left_out)
    V.assert_map_is_bijection(run_map, V.bnode_labels(left_quads),
                              V.bnode_labels(out_quads))
    check(V.apply_map(left_quads, run_map) == out_quads,
          "reported label map maps the input exactly onto the output file")
    log("  canonical bytes:\n" + open(left_out, encoding="utf-8").read())


def _degree_profile(quads):
    from collections import Counter
    out = Counter()
    for quad in quads:
        for i, term in enumerate(quad):
            if term[0] == "bnode":
                out[(term[1], i)] += 1
    return sorted(out.values())


def phase_cycle_vs_two(work):
    phase("one 6-cycle vs two disconnected 3-cycles")
    case = os.path.join(FIX, "cases", "case2_ring_vs_two")
    left = os.path.join(case, "left.nq")
    right = os.path.join(case, "right.nq")
    left_out = os.path.join(work, "case2_left.nq")
    right_out = os.path.join(work, "case2_right.nq")
    p = run_json("rdf", "normalize", "--input", left, "--output", left_out)
    q = run_json("rdf", "normalize", "--input", right, "--output", right_out)

    lq, rq = V.parse(left), V.parse(right)
    check(len(lq) == len(rq) == 6, "both datasets contain 6 quads")
    check(p["blank_nodes"] == q["blank_nodes"] == 6,
          "both datasets contain 6 blank nodes")
    check(_degree_profile(lq) == _degree_profile(rq),
          "per-node in/out degree profiles are identical")
    check(p["sha256"] != q["sha256"],
          "canonical digests differ (%s vs %s)"
          % (p["sha256"][:12], q["sha256"][:12]))

    proc = run("rdf", "compare", "--left", left, "--right", right,
               expect=1)
    report = json.loads(proc.stdout)
    check(report["status"] == "different",
          "compare reports different with exit code 1")
    diff = report["first_difference"]
    check(diff["kind"] in ("byte_mismatch", "left_ended", "right_ended"),
          "first-difference kind is a byte comparison result: " +
          diff["kind"])
    check("byte_offset" in diff and "line" in diff and "column" in diff,
          "first difference carries offset/line/column (%d @ line %d)"
          % (diff["byte_offset"], diff["line"]))
    check("business" not in json.dumps(report).lower()
          and "minimal" not in json.dumps(report).lower(),
          "report never claims a minimal business change")
    log("  left  digest: " + p["sha256"])
    log("  right digest: " + q["sha256"])
    log("  first byte difference: offset %d, line %d, col %d"
        % (diff["byte_offset"], diff["line"], diff["column"]))


def phase_graph_names_and_literals(work):
    phase("blank graph name reused as a term, self-loop, escapes, literals")
    case = os.path.join(FIX, "cases", "case3_graphname_edges", "combined.nq")
    out = os.path.join(work, "case3.nq")
    p = run_json("rdf", "normalize", "--input", case, "--output", out)
    check(p["quad_count"] == 9,
          "9 quads after the xsd:string duplicates collapse")
    check(p["duplicate_occurrences"] == 1,
          "the plain and xsd:string forms are the same RDF 1.1 term")

    V.assert_canonical_form(out)
    quads = V.parse(out)
    with_graph = [q for q in quads if len(q) == 4]
    check(len(with_graph) == 3,
          "3 statements keep a named graph (%d total quads)" % len(quads))
    graph_terms = {q[3] for q in with_graph}
    check(len(graph_terms) == 1,
          "all named statements share one graph name term")
    graph_label = next(iter(graph_terms))[1]
    used_as_term = any(
        any(t == ("bnode", graph_label) for t in q[:3]) for q in quads)
    check(used_as_term,
          "the graph name %r also appears as an ordinary term" % graph_label)
    inside_own_graph = [
        q for q in quads
        if len(q) == 4 and ("bnode", graph_label) in q[:3]
        and q[3] == ("bnode", graph_label)]
    check(len(inside_own_graph) == 2,
          "two statements place the graph node inside its own graph")

    # self loop
    self_loops = [q for q in quads if q[0] == q[2]]
    check(len(self_loops) == 1, "one self-loop survived canonicalization")

    texts = {q[2] for q in quads if q[2][0] == "lit"}
    langs = {t[2] for t in texts if t[2]}
    typed = {t[3] for t in texts if t[3]}
    check("zh-hant" in langs,
          "language tag was lowercased, not dropped: %s" % sorted(langs))
    check("七号架 — finals" in {t[1] for t in texts},
          "non-ASCII lexical form kept unchanged")
    check("a \"quoted\" value" in {t[1] for t in texts},
          "escaped quote decodes to a real quote in the lexical form")
    check("01" in {t[1] for t in texts} and
          "http://www.w3.org/2001/XMLSchema#integer" in typed,
          '"01"^^xsd:integer kept as its own term')
    check(any(t[1] == "1" and t[3].endswith("#integer") for t in texts),
          'the distinct "1"^^xsd:integer term was not merged with "01"')
    check(not any(t[3].endswith("#string") for t in texts),
          "xsd:string datatype IRI is suppressed in canonical form")
    check(V.has_duplicate_lines(out) is False,
          "no duplicate statement lines in canonical output")
    log("  canonical bytes:\n" + open(out, encoding="utf-8").read())


def phase_empty(work):
    phase("empty dataset")
    src = os.path.join(FIX, "samples", "empty.nq")
    out = os.path.join(work, "empty.nq")
    p = run_json("rdf", "normalize", "--input", src, "--output", out)
    empty_digest = hashlib.sha256(b"").hexdigest()
    check(os.path.getsize(out) == 0, "canonical file is zero bytes")
    check(p["sha256"] == empty_digest,
          "digest is SHA-256 of the empty string: " + empty_digest)
    check(p["quad_count"] == 0 and p["blank_nodes"] == 0,
          "0 quads, 0 blank nodes")
    check(p["blank_node_map"] == {}, "empty label map")
    report = run_json("rdf", "compare", "--left", src, "--right", out)
    check(report["status"] == "equivalent"
          and report["blank_node_correspondence"] == {},
          "an empty file and the empty canonical file are equivalent")


def phase_official(work, permutations=5):
    phase("W3C RDFC-1.0 official tests + label/line permutations")
    off = os.path.join(FIX, "official")
    results = []
    perm_results = []
    sha256_tests = 0
    for name in OFFICIAL_TESTS:
        inp = os.path.join(off, name + "-in.nq")
        expected = os.path.join(off, name + "-rdfc10.nq")
        out = os.path.join(work, name + ".out.nq")
        p = run_json("rdf", "normalize", "--input", inp, "--output", out)
        same = open(out, "rb").read() == open(expected, "rb").read()
        check(same, "%s: canonical bytes equal the official expected file"
              % name)
        sha256_tests += 1
        # Independently confirm the map and the quad-level round trip.
        input_quads = V.parse(inp)
        out_quads = V.parse(out)
        expected_quads = V.parse(expected)
        check(out_quads == expected_quads,
              "%s: parsed output quads equal parsed official quads" % name)
        V.assert_map_is_bijection(p["blank_node_map"],
                                  V.bnode_labels(input_quads),
                                  V.bnode_labels(out_quads))
        check(V.apply_map(input_quads, p["blank_node_map"]) == out_quads,
              "%s: reported map substitutes input onto output" % name)
        V.assert_canonical_form(out)
        map_path = os.path.join(off, name + "-rdfc10map.json")
        if os.path.exists(map_path):
            with open(map_path, encoding="utf-8") as f:
                official_map = json.load(f)
            check(p["blank_node_map"] == official_map,
                  "%s: issued identifiers map equals the official map test"
                  % name)
        if name in CROSS_GRAPH_TESTS:
            check(any(len(q) == 4 for q in out_quads),
                  "%s: named-graph statements present (cross-graph sharing)"
                  % name)
        results.append({"test": name, "sha256": p["sha256"],
                        "work_used": p["work"]["used"],
                        "byte_identical": same})

        # Random blank-label renamings and line shuffles of the input must
        # produce the *same* official expected bytes.  The equivalence is
        # checked on real quads, never on arbitrary symmetric node maps.
        for seed in range(permutations):
            text, renaming = permuted_input(inp, seed=seed,
                                            add_duplicates=(seed % 2 == 1))
            perm_in = os.path.join(work, "%s.perm%d.nq" % (name, seed))
            with open(perm_in, "w", encoding="utf-8", newline="\n") as f:
                f.write(text)
            perm_out = os.path.join(work, "%s.perm%d.out.nq" % (name, seed))
            try:
                pj = run_json("rdf", "normalize", "--input", perm_in,
                              "--output", perm_out)
            except AssertionError as e:
                perm_results.append({"test": name, "seed": seed,
                                     "ok": False, "error": str(e)})
                check(False, "%s perm seed %d: normalize failed" % (name, seed))
                continue
            same_expected = open(perm_out, "rb").read() == \
                open(expected, "rb").read()
            perm_quads = V.parse(perm_in)
            mapped = V.apply_map(perm_quads, pj["blank_node_map"])
            ok = same_expected and mapped == expected_quads
            perm_results.append({
                "test": name, "seed": seed, "labels_renamed":
                len(renaming), "duplicates_added": seed % 2 == 1,
                "byte_identical_to_official": same_expected,
                "quads_map_onto_official": mapped == expected_quads,
                "ok": ok})
            check(ok, "%s: permutation seed %d still equals the official "
                      "expected output" % (name, seed))
    check(sha256_tests >= 12,
          "checked %d official positive SHA-256 tests (>= 12 required)"
          % sha256_tests)
    with open(os.path.join(work, "official_results.json"), "w",
              encoding="utf-8") as f:
        json.dump({"source": "https://w3c.github.io/rdf-canon/tests/"
                            "manifest.ttl (snapshot 5ffaaa1)",
                   "tests": results, "permutations": perm_results},
                  f, indent=2)
    log("  %d official tests, %d permutation runs recorded in "
        "official_results.json" % (len(results), len(perm_results)))


def phase_resource_limit(work):
    phase("resource limit, atomic output, broken last line")
    src = os.path.join(FIX, "samples", "symmetric_ring.nq")
    out = os.path.join(work, "ring.nq")
    # Pre-existing output that must survive a failed run byte-for-byte.
    previous = b"previous output that must not be lost\n"
    with open(out, "wb") as f:
        f.write(previous)

    proc = run("rdf", "normalize", "--input", src, "--output", out,
               "--max-work", "3", expect=3)
    err = json.loads(proc.stderr)
    check(err["code"] == "resource_limit",
          "tiny budget exits 3 with code resource_limit (not 'different')")
    check(err["used"] == err["max_work"] == 3,
          "resource_limit reports the used count: %d" % err["used"])
    check(open(out, "rb").read() == previous,
          "previous output file bytes are unchanged after the failure")
    check(not any(f.startswith("ring.nq.tmp")
                  for f in os.listdir(work)),
          "no half-written temporary file is left behind")

    # A broken last line is rejected and also must not touch the output.
    bad = os.path.join(FIX, "samples", "last_line_bad.nq")
    proc = run("rdf", "normalize", "--input", bad, "--output", out,
               expect=2)
    err = json.loads(proc.stderr)
    check(err["code"] == "syntax_error" and err["line"] == 3,
          "broken last line rejected with line 3 (column %d)"
          % err.get("column", -1))
    check(open(out, "rb").read() == previous,
          "syntax failure leaves the old output untouched")

    # Compare with a bad side must fail, never report 'different'.
    proc = run("rdf", "compare", "--left", src, "--right", bad, expect=2)
    err = json.loads(proc.stderr)
    check(err.get("status") != "different" and err["code"] == "syntax_error",
          "a syntax error during compare is a failure, not 'different'")

    # Same input, bigger budget: completes.
    p = run_json("rdf", "normalize", "--input", src, "--output", out,
                 "--max-work", "1000")
    check(p["work"]["used"] <= 1000,
          "raised budget completes the same input using %d units"
          % p["work"]["used"])
    V.assert_canonical_form(out)
    check(open(out, "rb").read().endswith(b".\n"),
          "completed output replaced the previous bytes")
    check(p["blank_nodes"] == 6, "6 blank nodes canonicalized on the ring")

    # Compare of the ring against itself: equivalent, same shared budget.
    report = run_json("rdf", "compare", "--left", src, "--right", src,
                      "--max-work", "1000")
    check(report["status"] == "equivalent"
          and report["work_used"] <= 1000,
          "compare shares one budget across both sides (used %d)"
          % report["work_used"])


def phase_existing_commands(work):
    phase("existing intake / withdrawal / nested / BPE command paths")
    store = os.path.join(work, "store")
    run("--store", store, "init")
    data = os.path.join(work, "batch.jsonl")
    with open(data, "w", encoding="utf-8", newline="\n") as f:
        f.write(json.dumps({"study": "s1", "person": "p1",
                            "observation": "o1", "edition": 1,
                            "display_name": "Ann", "measure": 7,
                            "memo": "m"}) + "\n")
    p = run_json("--store", store, "import", "--batch-tag", "b1",
                 "--input", data)
    check(p.get("accepted") == 1, "import still accepts a valid batch")
    q = run_json("--store", store, "query", "--study", "s1")
    check(len(q["observations"]) == 1, "query still returns the observation")

    run("--store", store, "withdraw", "register", "--withdrawal-id", "w1",
        "--study", "s1", "--person", "p1")
    st = run_json("--store", store, "withdraw", "status",
                  "--withdrawal-id", "w1")
    check(st["withdrawals"][0]["state"] in
          ("registered", "editions_cleared", "completed"),
          "withdrawal still registers (state %s)"
          % st["withdrawals"][0]["state"])

    plan = os.path.join(work, "plan.json")
    with open(plan, "w", encoding="utf-8") as f:
        json.dump({"version": 1, "root": {"name": "participants",
                                          "columns": []}, "tables": []}, f)
    np_ = subprocess.run(
        [sys.executable, "-m", "rvault", "--store", store, "nested", "plan",
         "show", "--plan", plan], cwd=ROOT, env=ENV, capture_output=True,
        text=True)
    check(np_.returncode in (0, 4),
          "nested plan validation runs (rc=%d%s)"
          % (np_.returncode,
             "; ijson optional dependency reported"
             if np_.returncode == 4 else ""))

    db = os.path.join(work, "bpe.db")
    corpus = os.path.join(work, "corpus.jsonl")
    with open(corpus, "w", encoding="utf-8", newline="\n") as f:
        f.write(json.dumps({"id": "a", "weight": 1, "text": "abab abab"})
                + "\n")
    b = run_json("bpe", "train", "--database", db, "--input", corpus,
                 "--max-merges", "2", "--min-count", "1")
    check(b.get("status") == "ok" and b.get("rounds_done") == 2,
          "bpe train still runs (rounds_done=%s)" % b.get("rounds_done"))


def phase_no_store(work):
    phase("rdf entry points do not require --store")
    p = run_json("rdf", "normalize", "--input",
                 os.path.join(FIX, "cases", "case1_rename", "left.nq"))
    check(p["status"] == "canonicalized", "normalize works without --store")
    q = run_json("rdf", "compare", "--left",
                 os.path.join(FIX, "cases", "case1_rename", "left.nq"),
                 "--right", os.path.join(FIX, "cases", "case1_rename",
                                         "right.nq"))
    check(q["status"] == "equivalent", "compare works without --store")
    # A missing file is exit 4 (unreadable object), never 'different'.
    proc = subprocess.run(
        [sys.executable, "-m", "rvault", "rdf", "normalize", "--input",
         os.path.join(work, "does-not-exist.nq")],
        cwd=ROOT, env=ENV, capture_output=True, text=True)
    check(proc.returncode == 4,
          "missing input file exits 4 (unreadable, not 'different')")
    # A non-positive budget is refused by the argument parser.
    proc = subprocess.run(
        [sys.executable, "-m", "rvault", "rdf", "compare", "--left",
         os.path.join(FIX, "cases", "case2_ring_vs_two", "left.nq"),
         "--right", os.path.join(FIX, "cases", "case2_ring_vs_two",
                                 "left.nq"),
         "--max-work", "0"],
        cwd=ROOT, env=ENV, capture_output=True, text=True)
    check(proc.returncode == 2 and "positive integer" in proc.stderr,
          "--max-work 0 is rejected as a usage error")


def main():
    work = tempfile.mkdtemp(prefix="rvault_rdfcanon_demo_")
    log("working directory: %s" % work)
    try:
        phase_same_catalogue(work)
        phase_cycle_vs_two(work)
        phase_graph_names_and_literals(work)
        phase_empty(work)
        phase_official(work)
        phase_resource_limit(work)
        phase_no_store(work)
        phase_existing_commands(work)
    except Exception:
        log("\nDEMO FAILED; artifacts kept in: %s" % work)
        raise
    with open(os.path.join(work, "demo.log"), "w", encoding="utf-8") as f:
        f.write("\n".join(LOG_LINES) + "\n")
    if FAILURES:
        log("\n%d CHECK(S) FAILED" % len(FAILURES))
        for f in FAILURES:
            log("  - " + f)
        return 1
    log("\nALL RDF CANONICALIZATION DEMO PHASES PASSED")
    log("artifacts, reports and permutation log: %s" % work)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
