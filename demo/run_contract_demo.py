#!/usr/bin/env python3
"""End-to-end demo for versioned upstream input contracts.

Everything this demo touches (store, generated fixtures, logs, sync markers)
lives in a fresh temporary directory printed at the start; override with
RVAULT_CONTRACT_WORK.  All expectations are literal (hand-computed in
demo/expected_contracts.py); verify_contracts.py compares the real database
and receipt files against them using its own SQL.

Usage:  python3 demo/run_contract_demo.py
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
WORK = os.environ.get("RVAULT_CONTRACT_WORK") or tempfile.mkdtemp(
    prefix="rvault-contract-")
STORE = os.path.join(WORK, "store")
GEN = os.path.join(WORK, "input")
LOGS = os.path.join(WORK, "logs")
SYNC = os.path.join(WORK, "sync")

sys.path.insert(0, DEMO)
import contract_specs as S  # noqa: E402

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
    if cond:
        print("ok  ", msg)
    else:
        _failures.append(msg)
        print("FAIL", msg)


def write_jsonl(name, rows):
    path = os.path.join(GEN, name)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    return path


def fields(name):
    """Materialise one contract document inside the run's temp directory."""
    path = os.path.join(GEN, "fields_" + name + ".json")
    if not os.path.exists(path):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(S.DOCUMENTS[name], f)
    return path


def register(provider, material, version, doc, predecessor=None,
             compatibility=None, ok=(0,)):
    args = ["contract", "register", "--provider", provider,
            "--material-type", material, "--version", str(version),
            "--fields", fields(doc)]
    if predecessor is not None:
        args += ["--predecessor", str(predecessor)]
    if compatibility is not None:
        args += ["--compatibility", compatibility]
    return rv(*args, ok=ok)


def show(provider, material, version):
    return rvj("contract", "show", "--provider", provider,
               "--material-type", material, "--version", str(version))


def imp(tag, rows, provider=None, material=None, version=None, ok=(0,)):
    args = ["import", "--batch-tag", tag, "--input", write_jsonl(tag + ".jsonl", rows)]
    if provider is not None:
        args += ["--provider", provider, "--material-type", material,
                 "--contract-version", str(version)]
    r = rv(*args, ok=ok)
    return json.loads(r.stdout)


def verify(cp):
    r = subprocess.run([sys.executable, os.path.join(DEMO, "verify_contracts.py"),
                        "--store", STORE, "--checkpoint", cp],
                       capture_output=True, text=True)
    log("verify_%s.log" % cp, r.stdout + r.stderr)
    if r.returncode != 0:
        print(r.stdout)
        print(r.stderr, file=sys.stderr)
        raise SystemExit("verify %s FAILED" % cp)
    print("== checkpoint %s ok" % cp)


def changes_sig(entries):
    return [(e["field"], e["kind"], e["compatible"]) for e in entries]


def db_contracts():
    conn = sqlite3.connect(os.path.join(STORE, "vault.sqlite3"))
    try:
        return [(r[0], r[1], r[2], r[3], r[4]) for r in conn.execute(
            "SELECT provider, material_type, version, predecessor, compatibility "
            "FROM ct_contracts ORDER BY provider, material_type, version")]
    finally:
        conn.close()


def db_count(table):
    conn = sqlite3.connect(os.path.join(STORE, "vault.sqlite3"))
    try:
        return conn.execute("SELECT COUNT(*) FROM " + table).fetchone()[0]
    finally:
        conn.close()


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


# --------------------------------------------------------------------------
# phases
# --------------------------------------------------------------------------
def phase_register_initial():
    print("-- phase 1: register two initial contracts with different field types")
    rv("init")
    r = rvj(*["contract", "register", "--provider", "labA",
              "--material-type", "obs", "--version", "1",
              "--fields", fields("labA_obs_v1")])
    check(r["status"] == "recorded" and r["compatibility"] == "initial"
          and r["classification"] == "initial",
          "labA/obs v1 published as the initial version")
    check([c["kind"] for c in r["changes"]] == ["field_defined"] * 7,
          "initial change list defines all seven fields")

    r = rvj(*["contract", "register", "--provider", "labB",
              "--material-type", "measurement", "--version", "1",
              "--fields", fields("labB_measurement_v1")])
    check(r["status"] == "recorded" and r["field_count"] == 9,
          "labB/measurement v1 published with nine fields")
    check(r["classification"] == "initial",
          "labB v1 is initial (no predecessor)")

    s = show("labA", "obs", 1)
    check([(f["name"], f["type"], f["target"]) for f in s["fields"]] == [
        ("study", "string", "study"), ("person", "string", "person"),
        ("obs_id", "string", "observation"), ("rev", "integer", "edition"),
        ("label", "string", "display_name"), ("value", "integer", "measure"),
        ("note", "string", "memo")],
        "show v1: provider-side names mapped by meaning to canonical fields")

    s = show("labB", "measurement", 1)
    check({f["name"]: f["type"] for f in s["fields"]} == {
        "study": "string", "subject": "string", "sample": "string",
        "attempt": "integer", "title": "string", "reading": "integer",
        "remark": "string", "verified": "boolean", "batch": "integer"},
        "show labB v1: different names AND a boolean/integer type mix")

    # identical re-send is an idempotent confirmation, never a new version
    r = rvj(*["contract", "register", "--provider", "labA",
              "--material-type", "obs", "--version", "1",
              "--fields", fields("labA_obs_v1")])
    check(r["status"] == "duplicate" and r["content_digest"],
          "identical re-send of a published version is idempotent")
    check(db_count("ct_contracts") == 2, "two contract versions in the store")
    verify("cp_initial")


def phase_compare_upgrade():
    print("-- phase 2: field-by-field compare, then a compatible upgrade")
    # compare BEFORE registering: shows the reviewable change list
    r = rvj("contract", "compare", "--provider", "labA",
            "--material-type", "obs", "--against", "1",
            "--fields", fields("labA_obs_v2_compatible"))
    check(changes_sig(r["changes"]) ==
          [("note", "nullable_relaxed", True), ("site", "field_added", True),
           ("value", "type_widened", True)],
          "compare v2 against v1: relaxed null / added optional / widened type")
    check(r["classification"] == "compatible", "compare classifies as compatible")

    r = rvj("contract", "register", "--provider", "labA", "--material-type",
            "obs", "--version", "2", "--predecessor", "1",
            "--compatibility", "compatible", "--fields",
            fields("labA_obs_v2_compatible"))
    check(r["status"] == "recorded" and r["classification"] == "compatible",
          "v2 published as a compatible upgrade of v1")
    check(changes_sig(r["changes"]) ==
          [("note", "nullable_relaxed", True), ("site", "field_added", True),
           ("value", "type_widened", True)],
          "registered change list matches the explicit comparison")
    s = show("labA", "obs", 2)
    check(s["predecessor"] == 1 and s["compatibility"] == "compatible",
          "stored v2 points at predecessor 1 with compatibility 'compatible'")
    check([(f["name"], f["type"], f["nullable"]) for f in s["fields"]][5:8] ==
          [("value", "number", False), ("note", "string", True),
           ("site", "string", False)],
          "v2 field definitions: widened value, relaxed note, optional site")

    r = rvj("contract", "register", "--provider", "labB", "--material-type",
            "measurement", "--version", "2", "--predecessor", "1",
            "--compatibility", "compatible", "--fields",
            fields("labB_measurement_v2_compatible"))
    check(changes_sig(r["changes"]) == [("batch", "type_widened", True)],
          "labB v2 widens only `batch` integer -> number")
    check(db_count("ct_contracts") == 4, "four versions registered so far")


def phase_branch():
    print("-- phase 3: a new branch with deliberately incompatible changes")
    r = rvj("contract", "compare", "--provider", "labA", "--material-type", "obs",
            "--against", "2", "--fields", fields("labA_obs_v3_branch"))
    check(changes_sig(r["changes"]) ==
          [("flag", "field_added", False), ("note", "field_removed", False)],
          "compare v3 against v2: removed note, added REQUIRED flag")
    check(r["classification"] == "branch_required",
          "compare classifies v3 as branch-required")

    # The same document declared 'compatible' must be refused, never
    # disguised: this is what "may not pass off as a compatible upgrade" means.
    r = register("labA", "obs", 3, "labA_obs_v3_branch", predecessor=2,
                 compatibility="compatible", ok=(2,))
    e = json.loads(r.stdout or r.stderr)
    check(e["code"] == "incompatible_change"
          and changes_sig(e["changes"]) ==
          [("flag", "field_added", False), ("note", "field_removed", False)],
          "incompatible document with 'compatible' is refused (no disguise)")
    check(("labA", "obs", 3, 2, "branch") not in db_contracts(),
          "refused attempt left no version 3 row")

    r = rvj("contract", "register", "--provider", "labA", "--material-type",
            "obs", "--version", "3", "--predecessor", "2",
            "--compatibility", "branch", "--fields", fields("labA_obs_v3_branch"))
    check(r["status"] == "recorded" and r["classification"] == "branch_required",
          "v3 published as an explicit new branch of v2")
    s = show("labA", "obs", 3)
    check(s["predecessor"] == 2 and s["compatibility"] == "branch",
          "stored v3 references the existing predecessor 2 as a branch")
    check(db_count("ct_contracts") == 5, "five versions total after the branch")
    verify("cp_register")


def phase_rejections():
    print("-- phase 4: registration rejection battery (DB reopened every time)")
    exp = [("labA", "obs", 1, None, "initial"),
           ("labA", "obs", 2, 1, "compatible"),
           ("labA", "obs", 3, 2, "branch"),
           ("labB", "measurement", 1, None, "initial"),
           ("labB", "measurement", 2, 1, "compatible")]
    # a private provider clone so attacked version 3 can never collide with
    # the real labA v3 branch
    register("labRej", "obs", 1, "labA_obs_v1")
    register("labRej", "obs", 2, "labA_obs_v2_compatible", predecessor=1,
             compatibility="compatible")

    for doc, code, entries in [
            ("reject_r1_delete_field", "incompatible_change",
             [("site", "field_removed")]),
            ("reject_r2_tighten_nullable", "incompatible_change",
             [("note", "nullable_tightened")]),
            ("reject_r3_narrow_type", "incompatible_change",
             [("value", "type_narrowed")]),
            ("reject_r4_add_required", "incompatible_change",
             [("phase", "field_added")]),
            ("reject_r5_change_meaning", "incompatible_change",
             [("label", "target_changed"), ("note", "target_changed")]),
            ("reject_r6_required_change", "incompatible_change",
             [("site", "default_removed"), ("site", "required_changed")]),
            ("reject_r7_default_change", "incompatible_change",
             [("site", "default_changed")]),
            ("reject_r8_null_default", "invalid_contract", []),
            ("reject_r9_empty_field_name", "invalid_contract", [])]:
        r = register("labRej", "obs", 3, doc, predecessor=2,
                     compatibility="compatible", ok=(2,))
        payload = json.loads(r.stderr or r.stdout)
        label = doc.replace("reject_", "")
        ok = payload["code"] == code
        if entries:
            ok = ok and [(e["field"], e["kind"])
                         for e in payload.get("changes", [])] == entries
        check(ok, "%s rejected with %s%s" % (
            label, code, (" and change list " + str(entries)) if entries else ""))

    # registry-structure rejections
    r = register("labB", "measurement", 5, "labB_measurement_v2_compatible",
                 predecessor=2, compatibility="compatible", ok=(2,))
    check(json.loads(r.stdout or r.stderr)["code"] == "version_gap",
          "skipped version number (5 after 2) rejected")
    r = register("labB", "measurement", 3, "labB_measurement_v2_compatible",
                 predecessor=99, compatibility="compatible", ok=(2,))
    check(json.loads(r.stdout or r.stderr)["code"] == "unknown_predecessor",
          "nonexistent predecessor rejected")
    r = register("labB", "measurement", 3, "labB_measurement_v2_compatible",
                 predecessor=3, compatibility="branch", ok=(2,))
    check(json.loads(r.stdout or r.stderr)["code"] == "cycle_detected",
          "a version naming itself as predecessor is a cycle, rejected")
    r = register("labB", "measurement", 3, "labB_measurement_v2_compatible",
                 predecessor=None, compatibility="compatible", ok=(2,))
    check(json.loads(r.stdout or r.stderr)["code"] == "predecessor_required",
          "new version without a predecessor rejected")

    # A longer cycle cannot be built through the public CLI (numbering is
    # strictly max+1), so the defensive predecessor-graph walk is exercised
    # against a deliberately corrupted registry: point v1's predecessor at
    # v2 in a private store copy and try to extend from v2.
    r = _cycle_via_corrupt_registry()
    check(r == "cycle_detected",
          "predecessor-graph cycle in a corrupted registry is detected "
          "(defensive chain walk)")

    # content mismatch for an already published version
    r = register("labA", "obs", 2, "labA_obs_v3_branch", predecessor=1,
                 compatibility="compatible", ok=(2,))
    check(json.loads(r.stdout or r.stderr)["code"] == "content_conflict",
          "re-sending a published version with different content rejected")
    r = rvj(*["contract", "register", "--provider", "labA",
              "--material-type", "obs", "--version", "2", "--predecessor", "1",
              "--compatibility", "compatible",
              "--fields", fields("labA_obs_v2_compatible")])
    check(r["status"] == "duplicate",
          "identical re-send still confirms idempotently after conflicts")

    # the whole tower is exactly the intended versions (plus the rejection
    # clone); none of the refused attempts added anything
    check(db_contracts() == exp + [
        ("labRej", "obs", 1, None, "initial"),
        ("labRej", "obs", 2, 1, "compatible")],
        "contract tower unchanged by every rejected registration (reopened DB)")
    check(db_count("ct_batches") == 0,
          "no batch rows were produced by registration attempts")
    verify("cp_register")


def phase_import_defaults():
    print("-- phase 5: contract-checked import fills optional defaults")
    laba = [
        {"study": "laba", "person": "pa-1", "obs_id": "L1", "rev": 1,
         "label": "Alpha One", "value": 10},
        {"study": "laba", "person": "pa-2", "obs_id": "L2", "rev": 1,
         "label": "Alpha Two", "value": 20, "note": None},
        {"study": "laba", "person": "pa-3", "obs_id": "L3", "rev": 1,
         "label": "Alpha Three", "value": 10.5, "note": "north"},
        {"study": "laba", "person": "pa-1", "obs_id": "L1", "rev": 2,
         "label": "Alpha One again", "value": 11, "note": "second"},
    ]
    labb = [
        {"study": "labb", "subject": "pb-1", "sample": "M1", "attempt": 1,
         "title": "Beta One", "reading": 7},
        {"study": "labb", "subject": "pb-2", "sample": "M2", "attempt": 1,
         "title": "Beta Two", "reading": 8, "remark": "see note"},
    ]
    r = imp("a-good", laba, "labA", "obs", 2)
    check(r["status"] == "accepted" and r["accepted"] == 4,
          "four labA v2 lines accepted")
    check(r.get("contract", {}).get("version") == 2,
          "receipt summary names the exact contract version used")

    r = imp("b-good", labb, "labB", "measurement", 2)
    check(r["status"] == "accepted" and r["accepted"] == 2,
          "two labB v2 lines accepted")

    q = rvj("query", "--study", "laba")
    got = {o["observation"]: (o["measure"], o["memo"], o["edition"],
                              o["display_name"]) for o in q["observations"]}
    check(got == {
        "L1": (11, "second", 2, "Alpha One again"),
        "L2": (20, "", 1, "Alpha Two"),
        "L3": (10.5, "north", 1, "Alpha Three")},
        "defaults filled (note ''), explicit null -> default, fractional "
        "measure preserved, non-default memo kept")

    q = rvj("query", "--study", "labb")
    got = {o["observation"]: (o["measure"], o["memo"]) for o in q["observations"]}
    check(got == {"M1": (7, ""), "M2": (8, "see note")},
          "labB defaults filled (remark ''), explicit remark preserved")
    verify("cp_import")


def phase_reopen():
    print("-- phase 6: reopen the database; results survive unchanged")
    q = rvj("query", "--study", "laba")
    check([(o["observation"], o["measure"], o["memo"]) for o in q["observations"]]
          == [("L1", 11, "second"), ("L2", 20, ""), ("L3", 10.5, "north")],
          "fresh process reads the same stored defaults and values")
    check(db_contracts() == [
        ("labA", "obs", 1, None, "initial"),
        ("labA", "obs", 2, 1, "compatible"),
        ("labA", "obs", 3, 2, "branch"),
        ("labB", "measurement", 1, None, "initial"),
        ("labB", "measurement", 2, 1, "compatible"),
        ("labRej", "obs", 1, None, "initial"),
        ("labRej", "obs", 2, 1, "compatible")],
        "contract versions survive process restart (reopened DB)")
    verify("cp_import")


def phase_batch_rejection():
    print("-- phase 7: valid lines first, illegal last line -> whole batch "
          "rejected, no residue, diagnostics carry no payload")
    rows = [
        {"study": "laba", "person": "pk-1", "obs_id": "K1", "rev": 1,
         "label": "Mixed One", "value": 1},
        {"study": "laba", "person": "pk-2", "obs_id": "K2", "rev": 1,
         "label": "Mixed Two", "value": 2},
        {"study": "laba", "person": "pk-3", "obs_id": "K3", "rev": 1,
         "label": "Mixed Three", "value": 3},
        # illegal line: unknown field AND wrong type, in one line
        {"study": "laba", "person": "pk-4", "obs_id": "K4", "rev": 1,
         "label": "LEAK-NAME", "value": "LEAK-FIELD", "secret": "LEAK-MEMO"},
    ]
    r = imp("mixed-bad", rows, "labA", "obs", 2, ok=(2,))
    errs = r["errors"]
    check(r["status"] == "rejected", "mixed batch rejected as a whole")
    got = [(e.get("line"), e.get("field"), e.get("reason")) for e in errs]
    check(got == [(4, "secret", "unknown_field"),
                  (4, "value", "type_mismatch")],
          "diagnostics identify line 4, field name and error category")
    check(all(e.get("line") == 4 for e in errs),
          "no diagnostics point at the legal first three lines")
    check(not any("LEAK" in json.dumps(r) for _ in [0]),
          "diagnostics never echo display_name / measure / memo payloads")
    check(not any(e.get("field") == "label" and "value" in json.dumps(e)
                  for e in errs),
          "diagnostics carry field NAMES, not field values")
    check(("laba", "K1") not in {(e[0], e[1]) for e in _editions()}
          and ("laba", "K2") not in {(e[0], e[1]) for e in _editions()}
          and ("laba", "K3") not in {(e[0], e[1]) for e in _editions()},
          "not even the legal first three lines landed in the store")
    verify("cp_rejected")

    # A missing required field, a non-null null and a bad canonical domain
    # value are all contract rejections with their own categories.
    cases = [
        ([{"study": "laba", "person": "q1", "obs_id": "K9", "rev": 1,
           "value": 5}], ("label", "missing_required_field")),
        ([{"study": "laba", "person": "q1", "obs_id": "K9", "rev": 1,
           "label": "x", "value": None}], ("value", "null_not_allowed")),
        ([{"study": "laba", "person": "q1", "obs_id": "K9", "rev": 0,
           "label": "x", "value": 5}], ("rev", "value_out_of_range")),
        ([{"study": "laba", "person": "q1", "obs_id": "K9", "rev": 1,
           "label": "x", "value": True}], ("value", "type_mismatch")),
    ]
    for i, (line, expected) in enumerate(cases):
        r = imp("case-%d" % i, line, "labA", "obs", 2, ok=(2,))
        e = r["errors"][0]
        check((e.get("field"), e.get("reason")) == expected,
              "category %s/%s reported for its own violation"
              % expected)
    check(db_count("batches") == 2,
          "only the two accepted contract batches exist (no orphan batch rows)")
    # the accepted batches are recorded with their protocol and version
    check(db_count("ct_batches") == 2, "two protocol/version batch rows")

    # A v3-shaped line must be refused by v2 and only accepted by explicitly
    # selecting v3 - the import never falls back to another version.
    v3line = [{"study": "laba", "person": "pa-9", "obs_id": "B1", "rev": 1,
               "label": "Branch", "value": 3, "flag": "checked"}]
    r = imp("v3-as-v2", v3line, "labA", "obs", 2, ok=(2,))
    check([(e.get("line"), e.get("field"), e.get("reason"))
           for e in r["errors"]] == [(1, "flag", "unknown_field")],
          "a v3-shaped line is rejected by v2 (unknown `flag`; never silently "
          "re-routed to v3)")
    r = imp("v3-explicit", v3line, "labA", "obs", 3)
    check(r["status"] == "accepted" and r["accepted"] == 1,
          "the same line is accepted when --contract-version 3 is explicit")

    wrongver = [{"study": "laba", "person": "pa-9", "obs_id": "B9",
                 "rev": 1, "label": "WrongVer", "value": 4, "flag": "checked"}]
    r = imp("wrong-version", wrongver, "labA", "obs", 9, ok=(4,))
    check(r["status"] == "rejected"
          and r["errors"][0]["code"] == "unknown_contract_version",
          "unknown contract version rejects the batch with exit code 4")
    check(("laba", "B9") not in {(e[0], e[1]) for e in _editions()},
          "unknown-version batch stored nothing")

    # the batch credential cannot switch protocol
    r = imp("a-good", [{"study": "other"}], "labA", "obs", 2, ok=(2,))
    check(any("different content" in e.get("reason", "") for e in r["errors"]),
          "re-delivering a used batch_tag with different bytes is rejected")


def _editions():
    conn = sqlite3.connect(os.path.join(STORE, "vault.sqlite3"))
    try:
        return [tuple(r) for r in conn.execute(
            "SELECT study, observation, edition, person, display_name, measure, "
            "memo, batch_tag FROM editions")]
    finally:
        conn.close()


def phase_race():
    print("-- phase 8: two processes race for the same successor version")
    exp_before = db_count("ct_contracts")
    # Both processes try to publish version 3 of labB/measurement (max+1 from
    # v2).  A only widens `batch` (compatible); B also widens `reading`
    # (compatible too, but a DIFFERENT document).  Exactly one may win; the
    # loser must be rejected and leave no half version behind.
    write_race_docs()
    pa = spawn(["contract", "register", "--provider", "labB",
                "--material-type", "measurement", "--version", "3",
                "--predecessor", "2", "--compatibility", "compatible",
                "--fields", os.path.join(GEN, "race_a.json")],
               ["contract.register.before_commit"])
    wait_reached("contract.register.before_commit")
    pb = spawn(["contract", "register", "--provider", "labB",
                "--material-type", "measurement", "--version", "3",
                "--predecessor", "2", "--compatibility", "compatible",
                "--fields", os.path.join(GEN, "race_b.json")], [])
    time.sleep(1.0)
    check(pb.poll() is None,
          "second registrar blocks while the first holds the write lock "
          "(it cannot validate against a stale snapshot)")
    release("contract.register.before_commit")
    out_a, err_a = pa.communicate()
    out_b, err_b = pb.communicate()
    check(pa.returncode == 0 and json.loads(out_a)["status"] == "recorded",
          "first registrar commits version 3")
    check(pb.returncode == 2 and json.loads(out_b or err_b)["code"] == "content_conflict",
          "second registrar loses with content_conflict")
    check(db_count("ct_contracts") == exp_before + 1,
          "exactly one new version exists after the race (reopened DB)")
    rows = [r for r in db_contracts() if r[:3] == ("labB", "measurement", 3)]
    check(len(rows) == 1 and rows[0][3] == 2,
          "the winner is one complete version 3 with predecessor 2")
    changed = rvj("contract", "show", "--provider", "labB", "--material-type",
                  "measurement", "--version", "3")
    check([(f["name"], f["type"], f["has_default"], f["default"])
           for f in changed["fields"]][5] == ("reading", "integer", False, None),
          "the losing process's `reading` change is absent (whole-version "
          "winner, not a merged half)")
    check([(f["name"], f["type"]) for f in changed["fields"]][8] ==
          ("batch", "number"),
          "the surviving version 3 carries the winner's `batch` widening")
    check(db_count("ct_fields") == 7 + 8 + 8 + 7 + 8 + 9 + 9 + 9,
          "field rows belong to complete versions only "
          "(labA 7+8+8, labRej 7+8, labB 9+9+9)")


def write_race_docs():
    labb = json.load(open(fields("labB_measurement_v2_compatible")))["fields"]
    a = [dict(x) for x in labb]
    b = [dict(x) for x in labb]
    for x in b:
        if x["name"] == "reading":
            x["type"] = "number"
    json.dump({"fields": a}, open(os.path.join(GEN, "race_a.json"), "w"))
    json.dump({"fields": b}, open(os.path.join(GEN, "race_b.json"), "w"))


def phase_legacy():
    print("-- phase 9: the original fixed-field import still works unchanged")
    tail0 = rvj("feed", "status")["tail_seq"]
    r = imp("legacy-good", [{
        "study": "legacy", "person": "pl-1", "observation": "G1",
        "edition": 1, "display_name": "Legacy One", "measure": 100,
        "memo": "m1"}])
    check(r["status"] == "accepted" and r["accepted"] == 1,
          "legacy import without contract selectors accepted")
    check("protocol" not in r, "legacy receipt carries no contract fields")
    check(rvj("feed", "status")["tail_seq"] == tail0 + 1,
          "legacy import still emits its change-feed notification")
    # original fixed-field validation still applies
    r = imp("legacy-bad", [{
        "study": "legacy", "person": "pl-1", "observation": "G2",
        "edition": 1, "display_name": "Legacy Two", "measure": 1.5,
        "memo": "m"}], ok=(2,))
    check("integer" in r["errors"][0]["reason"],
          "legacy protocol still rejects a non-integer measure")
    q = rvj("query", "--study", "legacy")
    check([o["observation"] for o in q["observations"]] == ["G1"],
          "failed legacy batch left no row")
    verify("cp_legacy")


def phase_withdrawal_and_feed():
    print("-- phase 10: withdrawal and notifications still work with contract data")
    r = rvj("withdraw", "register", "--withdrawal-id", "W1",
            "--study", "laba", "--person", "pa-1")
    check(r["state"] == "completed" and r["editions_removed"] == 2,
          "withdrawal erases the two labA editions of pa-1")
    q = rvj("query", "--study", "laba")
    check([o["observation"] for o in q["observations"]] == ["B1", "L2", "L3"],
          "L1 is hidden by the barrier; contract metadata is untouched")
    refresh, removed = _feed_sets()
    check(("laba", "L1") in removed,
          "withdrawal emitted a remove notification for L1")
    last_refresh = _last_seq("refresh", "laba", "L1")
    first_remove = _first_seq("remove", "laba", "L1")
    check(last_refresh < first_remove,
          "no refresh is emitted for L1 after its removal")
    rvj("feed", "register", "--consumer", "idx", "--from-seq", "1")
    out = rv("feed", "pull", "--consumer", "idx").stdout
    check("op=refresh" in out and "op=remove" in out,
          "a consumer sees both refresh and remove notifications")
    check("LEAK" not in out and "Alpha One" not in out,
          "notifications carry identifiers only, no payload")
    verify("cp_withdrawn")


def _cycle_via_corrupt_registry():
    """Corrupt a private copy of the registry into a predecessor cycle and
    prove the defensive walk refuses to extend it.  Returns the error code."""
    root = os.path.join(WORK, "corrupt")
    os.makedirs(root, exist_ok=True)
    store_dir = os.path.join(root, "store")
    env = dict(BASE_ENV)
    subprocess.run([sys.executable, "-m", "rvault", "--store", store_dir, "init"],
                   capture_output=True, text=True, env=env)
    for v, pred, comp in ((1, None, None), (2, 1, "compatible")):
        args = ["contract", "register", "--provider", "cyc",
                "--material-type", "obs", "--version", str(v),
                "--fields", fields("labA_obs_v2_compatible")]
        if pred is not None:
            args += ["--predecessor", str(pred), "--compatibility", comp]
        subprocess.run([sys.executable, "-m", "rvault", "--store", store_dir]
                       + args, capture_output=True, text=True, env=env)
    conn = sqlite3.connect(os.path.join(store_dir, "vault.sqlite3"))
    conn.execute("UPDATE ct_contracts SET predecessor=2 WHERE provider='cyc' "
                 "AND material_type='obs' AND version=1")
    conn.commit()
    conn.close()
    # v1 -> 2 and v2 -> 1: extending from predecessor 2 must see the cycle.
    p = subprocess.run(
        [sys.executable, "-m", "rvault", "--store", store_dir,
         "contract", "register", "--provider", "cyc", "--material-type", "obs",
         "--version", "3", "--predecessor", "2", "--compatibility", "branch",
         "--fields", fields("labA_obs_v2_compatible")],
        capture_output=True, text=True, env=env)
    try:
        return json.loads(p.stdout or p.stderr)["code"]
    except Exception:
        return "unexpected:" + (p.stdout + p.stderr).strip()


def _last_seq(op, study, observation):
    conn = sqlite3.connect(os.path.join(STORE, "vault.sqlite3"))
    try:
        return conn.execute(
            "SELECT COALESCE(MAX(seq), -1) FROM cf_events WHERE op=? AND "
            "study=? AND observation=?", (op, study, observation)).fetchone()[0]
    finally:
        conn.close()


def _first_seq(op, study, observation):
    conn = sqlite3.connect(os.path.join(STORE, "vault.sqlite3"))
    try:
        return conn.execute(
            "SELECT COALESCE(MIN(seq), 1<<62) FROM cf_events WHERE op=? AND "
            "study=? AND observation=?", (op, study, observation)).fetchone()[0]
    finally:
        conn.close()


def _feed_sets():
    conn = sqlite3.connect(os.path.join(STORE, "vault.sqlite3"))
    try:
        refresh = {(a, b) for a, b in conn.execute(
            "SELECT study, observation FROM cf_events WHERE op='refresh'")}
        removed = {(a, b) for a, b in conn.execute(
            "SELECT study, observation FROM cf_events WHERE op='remove'")}
        return refresh, removed
    finally:
        conn.close()


def main():
    for d in (GEN, LOGS, SYNC):
        os.makedirs(d, exist_ok=True)
    print("work dir: %s" % WORK)
    phase_register_initial()
    phase_compare_upgrade()
    phase_branch()
    phase_rejections()
    phase_import_defaults()
    phase_reopen()
    phase_batch_rejection()
    phase_race()
    phase_legacy()
    phase_withdrawal_and_feed()
    print("=" * 60)
    if _failures:
        print("FAILURES (%d):" % len(_failures))
        for f in _failures:
            print("  -", f)
        return 1
    print("all contract checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
