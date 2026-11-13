#!/usr/bin/env python3
"""End-to-end demo driver for rvault.

Walks two studies plus a concurrency study through intake, revision,
bundling, withdrawal, re-import attempts, crash + resume, and concurrent
bundle/withdrawal interleavings.  After each phase it calls verify.py, which
compares the real database and files against the hand-written expected.py.

All synchronization between processes uses sync-point marker files and
SIGKILL - never fixed sleeps.

Usage:  python3 demo/run_demo.py            (fresh run, deletes old work dir)
        RVAULT_KEEP=1 python3 demo/run_demo.py   (keep work dir afterwards)
"""
from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import sys
import time

DEMO = os.path.dirname(os.path.abspath(__file__))
PROJ = os.path.dirname(DEMO)
FIX = os.path.join(DEMO, "fixtures")
WORK = os.path.join(DEMO, "work")
STORE = os.path.join(WORK, "store")
LOGS = os.path.join(WORK, "logs")
SYNCDIR_ROOT = os.path.join(WORK, "sync")

sys.path.insert(0, DEMO)
import expected as E  # noqa: E402

BASE_ENV = dict(os.environ)
BASE_ENV["PYTHONPATH"] = PROJ

_step = 0


def log(name, text):
    with open(os.path.join(LOGS, name), "a", encoding="utf-8") as f:
        f.write(text)


def rv(*args, ok=(0,), tag=None):
    """Run one rvault CLI command."""
    global _step
    _step += 1
    cmd = [sys.executable, "-m", "rvault", "--store", STORE] + list(args)
    r = subprocess.run(cmd, capture_output=True, text=True, env=BASE_ENV)
    name = "%02d_%s" % (_step, tag or args[0])
    log(name + ".out", r.stdout)
    log(name + ".err", r.stderr)
    if r.returncode not in ok:
        raise SystemExit("command %s failed rc=%d\nstdout:\n%s\nstderr:\n%s"
                         % (cmd, r.returncode, r.stdout, r.stderr))
    return r


def rv_json(*args, **kw):
    return json.loads(rv(*args, **kw).stdout)


def imp(tag, fixture, **kw):
    return rv_json("import", "--batch-tag", tag,
                   "--input", os.path.join(FIX, fixture), **kw)


def verify(cp):
    r = subprocess.run([sys.executable, os.path.join(DEMO, "verify.py"),
                        "--store", STORE, "--checkpoint", cp],
                       capture_output=True, text=True)
    log("verify_%s.log" % cp, r.stdout + r.stderr)
    if r.returncode != 0:
        print(r.stdout)
        print(r.stderr, file=sys.stderr)
        raise SystemExit("verify %s FAILED" % cp)
    print("== checkpoint %s ok" % cp)


def expect(cond, msg):
    if not cond:
        raise SystemExit("EXPECTATION FAILED: " + msg)


def expect_absent(text, needles, what):
    for n in needles:
        expect(n not in text, "%s must not contain %r" % (what, n))


# ---------------------------------------------------------------- sync helpers
def spawn(args, wait_points):
    """Start an rvault process paused at the given sync points."""
    syncdir = os.path.join(SYNCDIR_ROOT, "run%d" % _step)
    os.makedirs(syncdir, exist_ok=True)
    env = dict(BASE_ENV)
    env["RVAULT_SYNC_DIR"] = syncdir
    env["RVAULT_SYNC_WAIT"] = ",".join(wait_points)
    p = subprocess.Popen([sys.executable, "-m", "rvault", "--store", STORE] + args,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                         text=True, env=env)
    return p, syncdir


def wait_reached(syncdir, name, timeout=60.0):
    path = os.path.join(syncdir, name + ".reached")
    deadline = time.time() + timeout
    while time.time() < deadline:
        if os.path.exists(path):
            return
        time.sleep(0.05)
    raise SystemExit("sync point never reached: " + name)


def release(syncdir, name):
    with open(os.path.join(syncdir, name + ".proceed"), "w") as f:
        f.write("go")


def bundle_content(bundle_id):
    with open(os.path.join(STORE, "bundles", bundle_id + ".json"),
              encoding="utf-8") as f:
        return json.load(f)


def strip_ts(content):
    content = dict(content)
    content.pop("created_at", None)
    return content


# ---------------------------------------------------------------- phases
def phase1_intake():
    print("-- phase 1: intake of two studies with revisions")
    r = imp("alpha-b1", "alpha_b1.jsonl")
    expect((r["status"], r["accepted"]) == ("accepted", 4), "alpha-b1 accepted 4")
    imp("alpha-b2", "alpha_b2.jsonl")
    imp("alpha-b3", "alpha_b3.jsonl")
    imp("beta-b1", "beta_b1.jsonl")
    verify("cp1")

    q = rv_json("query", "--study", "alpha")
    got = [(o["observation"], o["person"], o["edition"], o["display_name"],
            o["measure"], o["memo"]) for o in q["observations"]]
    expect(got == [tuple(t) for t in E.PJ_ALPHA_CP1],
           "CLI query alpha matches hand-computed projection")

    a = rv_json("aggregate", "--study", "alpha",
                "--groups", os.path.join(FIX, "groups_alpha.json"))
    expect(a["groups"] == [{"group_code": "G1", "persons": 2, "measure_total": 73},
                           {"group_code": "G2", "persons": 1, "measure_total": 30}],
           "CLI aggregate alpha matches hand-computed totals")

    h = rv_json("history", "--study", "alpha", "--observation", "A1")
    expect([e["edition"] for e in h["editions"]] == [1, 2, 3],
           "A1 keeps all three editions in history")


def phase2_duplicate():
    print("-- phase 2: exact resend of alpha-b1 is idempotent")
    r = imp("alpha-b1", "alpha_b1.jsonl")
    expect(r["status"] == "duplicate", "resent batch reported as duplicate")
    expect(r["accepted"] == 4 and r["withdrawn_suppressed"] == 0,
           "pre-withdrawal duplicate shows all accepted")
    verify("cp2")


def phase3_rejects():
    print("-- phase 3: tampered resend, corrupt lines, ownership conflict")
    # same batch_tag, different bytes -> rejected
    tampered = os.path.join(WORK, "alpha_b1_tampered.jsonl")
    with open(os.path.join(FIX, "alpha_b1.jsonl"), encoding="utf-8") as f:
        data = f.read().replace('"measure": 10', '"measure": 15')
    with open(tampered, "w", encoding="utf-8") as f:
        f.write(data)
    r = rv("import", "--batch-tag", "alpha-b1", "--input", tampered, ok=(2,))
    expect("already used" in r.stdout, "tampered resend rejected")
    expect_absent(r.stdout + r.stderr, ["Ann Lee", "ann-e1"], "tamper rejection")

    r = rv_json("import", "--batch-tag", "alpha-bad",
                "--input", os.path.join(FIX, "alpha_bad.jsonl"), ok=(2,))
    expect(r["status"] == "rejected", "corrupt batch rejected")
    expect([e["line"] for e in r["errors"]] == [2, 3, 4],
           "corrupt lines reported with positions")
    expect_absent(json.dumps(r), ["Bob Kim", '"ok"'], "corrupt-batch errors")

    r = rv_json("import", "--batch-tag", "alpha-bad2",
                "--input", os.path.join(FIX, "alpha_bad2.jsonl"), ok=(2,))
    expect(r["errors"][0]["reason"] == "ownership_conflict",
           "ownership conflict rejects the batch")
    expect_absent(json.dumps(r), ["Mal Ory", "Bob Kim", "mal-x"],
                  "ownership error message")
    verify("cp3")


def phase4_bundles():
    print("-- phase 4: bundles with overlapping participants")
    r = rv_json("bundle", "create", "--study", "alpha",
                "--groups", os.path.join(FIX, "groups_alpha.json"))
    expect(r["bundle_id"] == "B000001", "first bundle id")
    rv_json("bundle", "create", "--study", "beta",
            "--groups", os.path.join(FIX, "groups_beta.json"))
    rv_json("bundle", "create", "--study", "beta",
            "--groups", os.path.join(FIX, "groups_beta.json"))

    out = rv_json("bundle", "read", "--bundle-id", "B000001")
    expect(strip_ts(out) == E.BUNDLE_B000001, "bundle read returns exact content")
    lst = rv_json("bundle", "list")
    expect([b["bundle_id"] for b in lst["bundles"]] ==
           ["B000001", "B000002", "B000003"], "bundle list shows active bundles")
    verify("cp4")


def phase5_prehistory_withdrawal():
    print("-- phase 5: withdrawal registered before any data for that person")
    r = rv_json("withdraw", "register", "--withdrawal-id", "W0",
                "--study", "alpha", "--person", "zed")
    expect(r["state"] == "completed" and r["editions_removed"] == 0,
           "W0 completes with nothing to erase")
    r = imp("alpha-bz", "alpha_bz.jsonl")
    expect(r["accepted"] == 1 and r["withdrawn_suppressed"] == 1,
           "zed suppressed on first-ever delivery, bob accepted")
    verify("cp5")


def phase6_withdrawal():
    print("-- phase 6: withdrawal W1 erases ann from study alpha only")
    r = rv_json("withdraw", "register", "--withdrawal-id", "W1",
                "--study", "alpha", "--person", "ann")
    expect(r["state"] == "completed", "W1 completed")
    expect(r["editions_removed"] == 4, "W1 removed 4 edition rows")
    expect(r["bundles"]["cleared"] == ["B000001"], "W1 deleted B000001")

    r = rv("bundle", "read", "--bundle-id", "B000001", ok=(4,))
    expect("not readable" in r.stderr, "deleted bundle not readable")
    h = rv_json("history", "--study", "alpha", "--observation", "A1")
    expect(h["editions"] == [], "A1 history empty after withdrawal")
    q = rv_json("query", "--study", "beta")
    expect([o["observation"] for o in q["observations"]] == ["B1", "B2"],
           "beta unaffected: same person string lives on independently")
    verify("cp6")


def phase7_post_withdrawal_traffic():
    print("-- phase 7: re-import attempts after withdrawal")
    r = imp("alpha-b4", "alpha_b4.jsonl")
    expect(r["accepted"] == 2 and r["withdrawn_suppressed"] == 2,
           "mixed batch: bob/cid accepted, ann suppressed (old + new edition, "
           "new observation)")

    r = imp("alpha-b1", "alpha_b1.jsonl")
    expect(r["status"] == "duplicate", "old batch_tag still idempotent")
    expect(r["withdrawn_suppressed"] == 2 and r["accepted"] == 2,
           "duplicate receipt reflects current withdrawal registry")
    expect_absent(json.dumps(r), ["Ann Lee", "ann-e1"], "duplicate receipt")

    r = rv("import", "--batch-tag", "alpha-b6",
           "--input", os.path.join(FIX, "alpha_b6.jsonl"), ok=(2,))
    expect("edition_conflict" in r.stdout, "changed old edition rejected")
    expect_absent(r.stdout + r.stderr, ["999", "tampered", "Bob Kim"],
                  "edition-conflict error")

    r = imp("alpha-b7", "alpha_b7.jsonl")
    expect(r["accepted_existing"] == 1, "identical old edition is idempotent")
    verify("cp7")


def phase8_new_bundle():
    print("-- phase 8: remaining participants still build bundles")
    r = rv_json("bundle", "create", "--study", "alpha",
                "--groups", os.path.join(FIX, "groups_alpha.json"))
    expect(r["bundle_id"] == "B000004", "post-withdrawal bundle id")
    expect(strip_ts(bundle_content("B000004")) == E.BUNDLE_B000004,
           "B000004 content matches hand-computed expectation")
    verify("cp8")


def phase9_overlapping_withdrawal():
    print("-- phase 9: overlapping withdrawal W2 = {ann, cid}")
    r = rv_json("withdraw", "register", "--withdrawal-id", "W2",
                "--study", "alpha", "--person", "ann", "--person", "cid")
    expect(r["state"] == "completed" and r["editions_removed"] == 2,
           "W2 removes cid's two editions (ann already gone)")
    expect(r["bundles"]["cleared"] == ["B000004"], "W2 deletes B000004")
    verify("cp9")


def phase10_retry_and_conflict():
    print("-- phase 10: retry of completed withdrawal; changed set conflicts")
    r = rv_json("withdraw", "register", "--withdrawal-id", "W1",
                "--study", "alpha", "--person", "ann")
    expect(r["state"] == "completed" and r["editions_removed"] == 4,
           "retrying completed W1 is a no-op")
    r = rv("withdraw", "register", "--withdrawal-id", "W1",
           "--study", "alpha", "--person", "ann", "--person", "bob", ok=(2,))
    expect("different study or person set" in r.stderr,
           "same id with changed set conflicts")
    st = rv("withdraw", "status", "--withdrawal-id", "W1")
    expect_absent(st.stdout, ["Ann Lee", "ann-e1", '"measure"'],
                  "withdrawal status output")
    verify("cp10")


def phase11_bundle_after_overlap():
    print("-- phase 11: bundle of remaining participants")
    r = rv_json("bundle", "create", "--study", "alpha",
                "--groups", os.path.join(FIX, "groups_alpha.json"))
    expect(r["bundle_id"] == "B000005", "B000005 id")
    expect(strip_ts(bundle_content("B000005")) == E.BUNDLE_B000005,
           "B000005 content")
    verify("cp11")


def phase12_kill_after_register():
    print("-- phase 12: SIGKILL after barrier registration, before erasure")
    p, syncdir = spawn(["withdraw", "register", "--withdrawal-id", "W4",
                        "--study", "beta", "--person", "dan"],
                       ["withdraw.after_register"])
    wait_reached(syncdir, "withdraw.after_register")
    p.kill()
    p.wait()
    expect(p.returncode == -signal.SIGKILL, "W4 process killed")

    st = rv_json("withdraw", "status", "--withdrawal-id", "W4")
    expect(st["withdrawals"][0]["state"] == "registered",
           "W4 left in state 'registered'")
    q = rv_json("query", "--study", "beta")
    expect([o["observation"] for o in q["observations"]] == ["B1"],
           "barrier hides dan immediately even though his rows still exist")
    verify("cp12")  # verifier confirms the rows are still physically there


def phase13_resume_with_missing_bundle():
    print("-- phase 13: externally lost bundle + resume")
    os.remove(os.path.join(STORE, "bundles", "B000003.json"))  # simulated loss
    r = rv_json("withdraw", "resume", "--withdrawal-id", "W4")
    w = r["withdrawals"][0]
    expect(w["state"] == "completed", "W4 completed after resume")
    expect(w["bundles"]["cleared"] == ["B000002"], "B000002 deleted on resume")
    expect(w["bundles"]["missing"] == ["B000003"],
           "externally lost bundle recorded as missing")
    verify("cp13")


def phase14_kill_mid_bundles():
    print("-- phase 14: SIGKILL after editions cleared and one bundle deleted")
    rv_json("bundle", "create", "--study", "beta",
            "--groups", os.path.join(FIX, "groups_beta.json"))   # B000006
    rv_json("bundle", "create", "--study", "beta",
            "--groups", os.path.join(FIX, "groups_beta.json"))   # B000007
    p, syncdir = spawn(["withdraw", "register", "--withdrawal-id", "W7",
                        "--study", "beta", "--person", "ann"],
                       ["withdraw.after_bundle_delete.1"])
    wait_reached(syncdir, "withdraw.after_bundle_delete.1")
    p.kill()
    p.wait()
    st = rv_json("withdraw", "status", "--withdrawal-id", "W7")
    w = st["withdrawals"][0]
    expect(w["state"] == "editions_cleared", "W7 stopped after editions stage")
    expect(w["bundles"]["cleared"] == ["B000006"]
           and w["bundles"]["pending"] == ["B000007"],
           "one bundle cleared, one still pending")
    verify("cp14")


def phase15_resume_finishes():
    print("-- phase 15: resume finishes the remaining bundle")
    r = rv_json("withdraw", "resume", "--withdrawal-id", "W7")
    w = r["withdrawals"][0]
    expect(w["state"] == "completed", "W7 completed")
    expect(w["bundles"]["cleared"] == ["B000006", "B000007"], "both cleared")
    verify("cp15")


def phase16_concurrency():
    print("-- phase 16: concurrent bundle creation vs withdrawal")
    imp("conc-b1", "conc_b1.jsonl")

    # run A: withdrawal lands between the bundle's read and its registration
    p, syncdir = spawn(["bundle", "create", "--study", "conc",
                        "--groups", os.path.join(FIX, "groups_conc.json")],
                       ["bundle.after_read"])
    wait_reached(syncdir, "bundle.after_read")
    w = rv_json("withdraw", "register", "--withdrawal-id", "W5",
                "--study", "conc", "--person", "peg")
    expect(w["state"] == "completed", "W5 completed while bundle paused")
    release(syncdir, "bundle.after_read")
    out, err = p.communicate()
    expect(p.returncode == 3, "bundle aborted once withdrawal registered")
    expect("aborted" in out, "abort reported")
    expect(not os.path.exists(os.path.join(STORE, "bundles", "B000008.json")),
           "no bundle file leaked")
    expect(os.listdir(os.path.join(STORE, "tmp")) == [], "no tmp file leaked")

    # run B: bundle registered first; the withdrawal must find and delete it
    p, syncdir = spawn(["bundle", "create", "--study", "conc",
                        "--groups", os.path.join(FIX, "groups_conc.json")],
                       ["bundle.after_register"])
    wait_reached(syncdir, "bundle.after_register")
    expect(strip_ts(bundle_content("B000008")) == E.BUNDLE_B000008,
           "B000008 registered (quinn only, peg already withdrawn)")
    w = rv_json("withdraw", "register", "--withdrawal-id", "W6",
                "--study", "conc", "--person", "quinn")
    expect(w["bundles"]["cleared"] == ["B000008"],
           "withdrawal found and deleted the just-registered bundle")
    release(syncdir, "bundle.after_register")
    out, err = p.communicate()
    expect(p.returncode == 0, "bundle create itself had succeeded")

    # lock recovery: kill a process holding the study lock, then proceed
    imp("lockt-b1", "lockt_b1.jsonl")
    p, syncdir = spawn(["bundle", "create", "--study", "lockt"],
                       ["bundle.in_critical"])
    wait_reached(syncdir, "bundle.in_critical")
    p.kill()
    p.wait()
    expect(p.returncode == -signal.SIGKILL, "lock holder killed")
    r = rv_json("bundle", "create", "--study", "lockt")
    expect(r["bundle_id"] == "B000009",
           "study lock recovered after SIGKILL, new bundle created")
    verify("cp16")


def phase17_residue():
    print("-- phase 17: residue audit")
    verify("cp17")


def phase18_strange_path_refusal():
    print("-- phase 18: tampered registry path is never followed for deletion")
    imp("vandal-b1", "vandal_b1.jsonl")
    r = rv_json("bundle", "create", "--study", "vandal")
    expect(r["bundle_id"] == "B000010", "B000010 id")

    outside = os.path.join(WORK, "outside_target.txt")
    with open(outside, "w", encoding="utf-8") as f:
        f.write("must-not-be-deleted")
    # fault injection: corrupt the registry so the bundle's stored path
    # points outside the managed area
    import sqlite3
    conn = sqlite3.connect(os.path.join(STORE, "vault.sqlite3"))
    conn.execute("UPDATE bundles SET path=? WHERE bundle_id='B000010'",
                 ("../../../../outside_target.txt",))
    conn.commit()
    conn.close()

    w = rv_json("withdraw", "register", "--withdrawal-id", "WV",
                "--study", "vandal", "--person", "vic")
    expect(w["state"] == "completed", "WV completed")
    expect(w["anomalies"] == 1, "path mismatch flagged as anomaly")
    expect(w["bundles"]["cleared"] == ["B000010"],
           "canonical bundle file still erased")
    with open(outside, encoding="utf-8") as f:
        expect(f.read() == "must-not-be-deleted",
               "file outside the managed area was never touched")
    verify("cp18")


def main():
    if os.path.exists(WORK):
        shutil.rmtree(WORK)
    os.makedirs(LOGS)
    os.makedirs(SYNCDIR_ROOT)

    rv("init")
    phase1_intake()
    phase2_duplicate()
    phase3_rejects()
    phase4_bundles()
    phase5_prehistory_withdrawal()
    phase6_withdrawal()
    phase7_post_withdrawal_traffic()
    phase8_new_bundle()
    phase9_overlapping_withdrawal()
    phase10_retry_and_conflict()
    phase11_bundle_after_overlap()
    phase12_kill_after_register()
    phase13_resume_with_missing_bundle()
    phase14_kill_mid_bundles()
    phase15_resume_finishes()
    phase16_concurrency()
    phase17_residue()
    phase18_strange_path_refusal()

    print("\nDEMO OK - all phases and checkpoints passed")
    print("store kept at: %s" % STORE)
    if not os.environ.get("RVAULT_KEEP"):
        pass  # work dir intentionally left for inspection; delete manually


if __name__ == "__main__":
    main()
