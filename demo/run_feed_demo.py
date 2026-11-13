#!/usr/bin/env python3
"""End-to-end demo for the change-feed (notification) outlet.

Two real consumer processes (``live`` and ``slow``) walk through revisions,
withdrawals and retention.  Every pull's line protocol is compared against
hand-written expectations (demo/expected_feed.py) and the database is checked
independently (demo/verify_feed.py, own SQL) after each phase.

Interleaving uses sync points / SIGKILL only - no fixed sleeps.  Feed sync
points: feed.commit.finalize, feed.register.before_commit,
feed.trim.after_commit, feed.delivery.event.N, feed.delivery.during_frame,
feed.after_ack, feed.after_disable; plus the existing withdraw.after_register.
Setting RVAULT_CF_SYNC_DIR/RVAULT_CF_SYNC_WAIT makes a feed command pause at a
named point; RVAULT_SYNC_DIR/RVAULT_SYNC_WAIT does the same for the existing
withdraw/bundle points.

Usage: python3 demo/run_feed_demo.py
The work directory demo/work_feed is rebuilt on every run.
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
FIX = os.path.join(DEMO, "feed_fixtures")
WORK = os.path.join(DEMO, "work_feed")
MAIN = os.path.join(WORK, "main")
EDGE = os.path.join(WORK, "edge")
BIG = os.path.join(WORK, "big")
LOGS = os.path.join(WORK, "logs")
SYNC = os.path.join(WORK, "sync")

sys.path.insert(0, DEMO)
import expected_feed as E           # noqa: E402
import verify_feed as V             # noqa: E402

BASE_ENV = dict(os.environ)
BASE_ENV["PYTHONPATH"] = PROJ
_step = 0
_failures = []


# ---------------------------------------------------------------- utilities
def log(name, text):
    with open(os.path.join(LOGS, name), "a", encoding="utf-8") as f:
        f.write(text)


def rv(store, *args, ok=(0,), tag=None):
    global _step
    _step += 1
    cmd = [sys.executable, "-m", "rvault", "--store", store] + list(args)
    r = subprocess.run(cmd, capture_output=True, text=True, env=BASE_ENV)
    name = "%03d_%s" % (_step, tag or args[0] if args else "cmd")
    log(name + ".out", r.stdout)
    log(name + ".err", r.stderr)
    if r.returncode not in ok:
        raise SystemExit("command failed rc=%d: %s\nstdout:\n%s\nstderr:\n%s"
                         % (r.returncode, " ".join(cmd), r.stdout, r.stderr))
    return r


def rvj(store, *args, **kw):
    return json.loads(rv(store, *args, **kw).stdout)


def imp(store, tag, fixture, ok=(0,)):
    return rv(store, "import", "--batch-tag", tag,
              "--input", os.path.join(FIX, fixture), ok=ok)


GEN = os.path.join(WORK, "gen")


def gen_import(store, tag, rows, ok=(0,)):
    """Write rows to a generated fixture under the work dir and import it."""
    os.makedirs(GEN, exist_ok=True)
    path = os.path.join(GEN, tag + ".jsonl")
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    return rv(store, "import", "--batch-tag", tag, "--input", path, ok=ok)


def check(cond, msg):
    if not cond:
        _failures.append(msg)
        print("FAIL", msg)
    else:
        print("ok  ", msg)


def _verifier(*args):
    r = subprocess.run([sys.executable, os.path.join(DEMO, "verify_feed.py"),
                        "--store", args[0]] + list(args[1:]),
                       capture_output=True, text=True)
    log("verify_%d.log" % _step, r.stdout + r.stderr)
    if r.returncode != 0:
        print(r.stdout)
        print(r.stderr, file=sys.stderr)
        raise SystemExit("independent verification failed: %s" % (args,))
    return r.stdout


def dbcheck(store, cp, edge=False):
    args = ["--db-checkpoint", cp]
    if edge:
        args.append("--edge")
    _verifier(store, *args)
    print("== db checkpoint %s ok" % cp)


def pull(store, consumer, limit, expected, edge=False, replay=None,
         expect_empty=False):
    """Run one pull, store its output, and compare it with expected.
    expected=None means "not checked against a delivery literal" (used only
    for empty pulls, which are checked for emptiness)."""
    global _step
    _step += 1
    path = os.path.join(LOGS, "%03d_pull_%s.txt" % (_step, consumer))
    r = rv(store, "feed", "pull", "--consumer", consumer,
           "--limit", str(limit), tag="pull_" + consumer)
    with open(path, "w", encoding="utf-8") as f:
        f.write(r.stdout)
    if expected is not None:
        args = ["--pull-json", path, "--expected", expected]
        if edge:
            args.append("--edge")
        _verifier(store, *args)
    else:
        kv0 = V.parse_pull(r.stdout)[0]
        check(kv0.get("empty") == "1",
              "pull %s is a recognisable empty result" % consumer)
        return r.stdout, kv0
    kv = V.parse_pull(r.stdout)[0]
    if replay is not None:
        check(kv.get("replay") == str(replay),
              "%s replay flag is %s" % (consumer, replay))
    print("== pull %s %s ok (%s..%s, %s events)"
          % (consumer, expected, kv.get("from"), kv.get("through"),
             kv.get("events")))
    return r.stdout, kv


def token_of(text):
    line = [ln for ln in text.splitlines() if ln.startswith("START")][0]
    return dict(x.split("=", 1) for x in line.split()[1:])["delivery"]


def ack(store, consumer, tok, ok=(0,)):
    r = rv(store, "feed", "ack", "--consumer", consumer, "--token", tok, ok=ok,
           tag="ack_" + consumer)
    return None if r.returncode else json.loads(r.stdout)


def status(store, consumer=None):
    args = ["feed", "status"] + (["--consumer", consumer] if consumer else [])
    return rvj(store, *args)


def trim(store, through=None, ok=(0,)):
    args = ["feed", "trim"] + (["--through-seq", str(through)]
                               if through is not None else [])
    r = rv(store, *args, ok=ok, tag="trim")
    return (None if r.returncode else json.loads(r.stdout)), r


def spawn(store, args, wait_points, syncdir):
    os.makedirs(syncdir, exist_ok=True)
    env = dict(BASE_ENV)
    env["RVAULT_CF_SYNC_DIR"] = syncdir
    env["RVAULT_CF_SYNC_WAIT"] = ",".join(wait_points)
    # the existing withdraw/bundle sync points read RVAULT_SYNC_DIR; both
    # namespaces share the same directory in this demo
    env["RVAULT_SYNC_DIR"] = syncdir
    env["RVAULT_SYNC_WAIT"] = ",".join(wait_points)
    return subprocess.Popen([sys.executable, "-m", "rvault", "--store", store] + args,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            text=True, env=env)


def wait_reached(syncdir, name, timeout=60.0):
    path = os.path.join(syncdir, name + ".reached")
    deadline = time.time() + timeout
    while time.time() < deadline:
        if os.path.exists(path):
            return
        time.sleep(0.02)
    raise SystemExit("sync point never reached: " + name)


def release(syncdir, name):
    with open(os.path.join(syncdir, name + ".proceed"), "w") as f:
        f.write("go")


def projection(store, study="obs"):
    """Source-side projection, read through the existing query command."""
    q = rvj(store, "query", "--study", study)
    return [(o["observation"], o["person"], o["edition"])
            for o in q["observations"]]


def expect_projection(store, key, study="obs"):
    got = projection(store, study)
    check(got == [tuple(x) for x in E.MAIN_PROJECTION[key]],
          "source projection %s == hand-computed (%s)" % (key, got))


# ---------------------------------------------------------------- main walk
def phase_install_and_register():
    print("-- phase 1: install feed, register consumer before any feed traffic")
    rv(MAIN, "init")
    reg = rvj(MAIN, "feed", "register", "--consumer", "live")
    check(reg["mode"] == "now" and reg["start_seq"] == 1,
          "new consumer defaults to the current tail (no history)")
    dbcheck(MAIN, "main_start")


def phase_first_import():
    print("-- phase 2: first batch -> one committed frame (4 objects)")
    imp(MAIN, "f1", "obs_f1.jsonl")
    expect_projection(MAIN, "after_f1")
    out, kv = pull(MAIN, "live", 3, "live_1")
    # re-pull while the delivery is still pending: same range and same token,
    # even though the sender already printed everything
    out2, kv2 = pull(MAIN, "live", 3, "live_1", replay=1)
    check(token_of(out) == token_of(out2),
          "unacknowledged re-pull replays the same token")
    check(kv2.get("through") == kv.get("through"),
          "unacknowledged re-pull replays the same range")
    check(kv.get("events") == "4", "first frame exceeds target 3 and is sent whole (4 events)")
    tok = token_of(out)
    res = ack(MAIN, "live", tok)
    check(res["acked_seq"] == 4, "ack moves the position to the frame end (4)")
    again = ack(MAIN, "live", tok)
    check(again["replayed"] is True and again["acked_seq"] == 4,
          "re-acking the same token is idempotent")
    # second token attempt after ack: non-current token must not move anything
    st = status(MAIN, "live")
    check(st["consumer"]["acked_seq"] == 4 and "pending_delivery" not in st["consumer"],
          "acknowledged delivery is gone from the pending view")


def phase_revisions_and_duplicates():
    print("-- phase 3: revisions, identical redeliveries, legal older edition")
    imp(MAIN, "f2", "obs_f2.jsonl")   # ob1 e2, ob3 e6 + two identical rows
    expect_projection(MAIN, "after_f2")
    imp(MAIN, "f3", "obs_f3.jsonl")   # ob3 e2 (older) + identical ob4 e1
    expect_projection(MAIN, "after_f3")
    # While frame 2 is unacknowledged another batch lands (frame 3).  The
    # next pull must still replay frame 2's exact range and token.
    out_first, kv_first = pull(MAIN, "live", 100, "live_2")
    imp(MAIN, "f4", "obs_f4.jsonl")   # ob5 e1..e3, ob3 e7,e8 -> frame 3
    expect_projection(MAIN, "after_f4")
    out_second, kv_second = pull(MAIN, "live", 100, "live_2", replay=1)
    check(token_of(out_first) == token_of(out_second),
          "new writes while unacknowledged do not change the token")
    check((kv_first["from"], kv_first["through"])
          == (kv_second["from"], kv_second["through"]),
          "new writes while unacknowledged do not change the range")
    check(int(kv_second["head"]) == int(kv_first["head"]) + 2,
          "the new frame is visible in the head counter but is not delivered early")
    ack(MAIN, "live", token_of(out_first))
    st = status(MAIN, "live")
    check(st["consumer"]["acked_seq"] == 6,
          "acking the pinned range advances only to its end (6)")

    out, _ = pull(MAIN, "live", 3, "live_3")
    ack(MAIN, "live", token_of(out))
    st = status(MAIN, "live")
    check(st["consumer"]["acked_seq"] == 8,
          "older legal edition and identical redeliveries produced no events "
          "(position 8)")


def phase_rejected_batch():
    print("-- phase 4: rejected batch leaves no orphan notification")
    r = rv(MAIN, "import", "--batch-tag", "bad", "--input",
           os.path.join(FIX, "obs_bad.jsonl"), ok=(2,))
    check("rejected" in r.stdout, "corrupt batch rejected")
    dbcheck(MAIN, "after_reject")          # tail still 8, no frame for 'bad'
    before = status(MAIN)["tail_seq"]
    imp(MAIN, "g6", "obs_g6.jsonl")        # new observation ob6
    after = status(MAIN)["tail_seq"]
    check(after == before + 1,
          "next accepted write produces exactly one new event (tail %d->%d)"
          % (before, after))
    expect_projection(MAIN, "after_g6")
    out, _ = pull(MAIN, "live", 100, "live_4")
    check([ln for ln in out.splitlines() if ln.startswith("EVENT")] == [
        "EVENT seq=9 frame=4 ordinal=1 op=refresh study=obs observation=ob6"],
        "the only event after the rejected batch is ob6")
    ack(MAIN, "live", token_of(out))


def phase_register_slow():
    print("-- phase 5: second consumer registers at an explicit retained position")
    # a default registration (now) would receive nothing; prove the backlog
    # command shows it without creating a delivery
    rvj(MAIN, "feed", "register", "--consumer", "peeker")
    before = status(MAIN, "peeker")["consumer"]
    pv = rv(MAIN, "feed", "pull", "--consumer", "peeker", "--preview").stdout
    check(pv.startswith("START preview=1 empty=1"), "a from-now consumer is empty")
    after = status(MAIN, "peeker")["consumer"]
    check(before.get("pending_delivery") == after.get("pending_delivery"),
          "preview created no delivery and no token")
    rvj(MAIN, "feed", "disable", "--consumer", "peeker")
    reg = rvj(MAIN, "feed", "register", "--consumer", "slow", "--from-seq", "1")
    check(reg["start_seq"] == 1 and reg["acked_seq"] == 0,
          "explicit start is accepted while retained")
    # registering late must not move the already-running consumer
    check(status(MAIN, "live")["consumer"]["acked_seq"] == 9,
          "a second registration does not advance another consumer")


def phase_withdrawal_at_barrier():
    print("-- phase 6: remove is consumable at barrier commit, before file cleanup")
    global _step
    _step += 1
    syncdir = os.path.join(SYNC, "w1")
    p = spawn(MAIN, ["withdraw", "register", "--withdrawal-id", "W1",
                     "--study", "obs", "--person", "alice"],
              ["withdraw.after_register"], syncdir)
    wait_reached(syncdir, "withdraw.after_register")
    # the withdrawal process is paused after its barrier commit and before
    # the editions stage: payload rows still exist, but every read path
    # already hides them (the barrier is what the notification tracks).
    h = rvj(MAIN, "history", "--study", "obs", "--observation", "ob1")
    check(h["editions"] == [],
          "the barrier hides the source before erasure has run")
    # ... but the remove frame is already committed and consumable
    out, kv = pull(MAIN, "live", 1, "live_5")
    check(kv.get("events") == "4",
          "remove frame exceeds target 1 and is still sent whole")
    check(all(" op=remove " in ln for ln in out.splitlines()
              if ln.startswith("EVENT")),
          "all events are removals")
    dbcheck(MAIN, "during_w1")
    release(syncdir, "withdraw.after_register")
    res = p.communicate()
    check(p.returncode == 0, "withdrawal completed after resume (%r)" % (res[1][:80],))
    ack(MAIN, "live", token_of(out))
    expect_projection(MAIN, "after_w1")


def phase_post_withdrawal_intake():
    print("-- phase 7: post-withdrawal intake is suppressed; bob's object refreshes")
    imp(MAIN, "post", "obs_post.jsonl")
    expect_projection(MAIN, "after_post")
    out, _ = pull(MAIN, "live", 100, "live_6")
    evs = [ln for ln in out.splitlines() if ln.startswith("EVENT")]
    check(evs == ["EVENT seq=14 frame=6 ordinal=1 op=refresh study=obs "
                  "observation=ob7"],
          "suppressed lines for the withdrawn person produce no refresh")
    ack(MAIN, "live", token_of(out))


def phase_withdrawals_and_no_repeat():
    print("-- phase 8: second withdrawal; repeat withdrawal produces nothing")
    rvj(MAIN, "withdraw", "register", "--withdrawal-id", "W2",
        "--study", "obs", "--person", "bob", "--person", "alice")
    out, _ = pull(MAIN, "live", 100, "live_7")
    ack(MAIN, "live", token_of(out))
    expect_projection(MAIN, "after_w2")
    tail_before = status(MAIN)["tail_seq"]
    # W2 above overlapped W1 (alice already barred); only bob's newly
    # claimed objects were announced, which the live_7 pull verified.
    rvj(MAIN, "withdraw", "register", "--withdrawal-id", "W3",
        "--study", "obs", "--person", "alice")
    tail_after = status(MAIN)["tail_seq"]
    check(tail_after == tail_before,
          "re-withdrawing an already removed person emits no new remove")
    rvj(MAIN, "withdraw", "register", "--withdrawal-id", "W4",
        "--study", "obs", "--person", "cara")
    out, _ = pull(MAIN, "live", 100, "live_8")
    ack(MAIN, "live", token_of(out))
    expect_projection(MAIN, "after_w4")


def phase_slow_and_retention():
    print("-- phase 9: slow consumer limits trimming; progressive trim; catch-up")
    exp = iter(E.MAIN_TRIMS)

    def expect_trim(why):
        res, _ = trim(MAIN)
        e = next(exp)
        ok = ((res["trimmed_frames"], res["trimmed_events"],
               res["retention_floor"])
              == (e["trimmed_frames"], e["trimmed_events"],
                  e["retention_floor"]))
        check(ok, why + " (frames=%d events=%d floor=%d)"
              % (res["trimmed_frames"], res["trimmed_events"],
                 res["retention_floor"]))

    # live has acked 17, slow 0 -> nothing may be trimmed
    expect_trim("an unacknowledged slow consumer blocks all trimming")
    out, _ = pull(MAIN, "slow", 2, "slow_1")
    tok = token_of(out)
    expect_trim("a pending (unacknowledged) delivery also blocks trimming")
    ack(MAIN, "slow", tok)
    expect_trim("after slow acks frame 1: frame 1 trimmed, floor 4")
    out, _ = pull(MAIN, "slow", 2, "slow_2")
    ack(MAIN, "slow", token_of(out))
    expect_trim("after slow acks frame 2: floor 6")
    out, _ = pull(MAIN, "slow", 4, "slow_3")
    ack(MAIN, "slow", token_of(out))
    expect_trim("frames 3 and 4 trimmed together once fully acked")
    out, _ = pull(MAIN, "slow", 3, "slow_4")
    ack(MAIN, "slow", token_of(out))
    expect_trim("frame 5 trimmed")
    out, _ = pull(MAIN, "slow", 3, "slow_5")
    ack(MAIN, "slow", token_of(out))
    expect_trim("frames 6 and 7 trimmed")
    out, _ = pull(MAIN, "slow", 100, "slow_6")
    ack(MAIN, "slow", token_of(out))
    expect_trim("last frame trimmed; floor 17")
    res, _ = trim(MAIN)
    check(res["trimmed_frames"] == 0, "trimming an empty feed is a no-op")
    dbcheck(MAIN, "main_final")


def phase_gap_diagnosis():
    print("-- phase 10: an expired start reports the gap, never silently moves")
    r = rv(MAIN, "feed", "register", "--consumer", "replay_old",
           "--from-seq", "1", ok=(6,))
    err = json.loads(r.stderr)
    check(err["code"] == "start_out_of_range", "gap is reported as such")
    check(err["requested_seq"] == 1 and err["retention_floor"] == 17
          and err["earliest_available_seq"] == 18,
          "gap diagnostics carry requested position, floor and earliest available")
    check("replay_old" not in [c["name"] for c in
                               status(MAIN)["enabled_consumers"]],
          "refused registration created no consumer")
    # a still-retained position works
    reg = rvj(MAIN, "feed", "register", "--consumer", "late", "--from-seq", "18")
    check(reg["start_seq"] == 18, "the earliest available position is accepted")
    rvj(MAIN, "feed", "disable", "--consumer", "late")


# ---------------------------------------------------------------- edge cases
def _seed_edge():
    rv(EDGE, "init")
    rvj(EDGE, "feed", "register", "--consumer", "edge", "--from-seq", "1")
    for tag, rows in (("e1", [("o1", 1), ("o2", 1)]),
                      ("e2", [("o1", 2)]),
                      ("e3", [("o2", 2)])):
        with open(os.path.join(FIX, "e_%s.jsonl" % tag), "w", encoding="utf-8") as f:
            for obs, ed in rows:
                f.write(json.dumps({"study": "s1", "person": "pp",
                                    "observation": obs, "edition": ed,
                                    "display_name": "Pn", "measure": ed,
                                    "memo": "m"}) + "\n")
        imp(EDGE, tag, "e_%s.jsonl" % tag)


def _seed_edge_imports(names):
    """Write one single-object fixture per import and apply it.
    names: (import tag, observation, edition)."""
    for n, obs, ed in names:
        gen_import(EDGE, n, [{"study": "s1", "person": "pp",
                              "observation": obs, "edition": ed,
                              "display_name": "Pn", "measure": ed,
                              "memo": "m"}])


def phase_edge_crash_and_lifecycle():
    print("-- phase 11: edge store - crash, replay, lifecycle, concurrency")
    rv(EDGE, "init")
    rvj(EDGE, "feed", "register", "--consumer", "edge", "--from-seq", "1")
    # e1 is a two-object batch; e2 and e3 are one object each
    gen_import(EDGE, "e1", [{"study": "s1", "person": "pp",
                             "observation": obs, "edition": 1,
                             "display_name": "Pn", "measure": 1,
                             "memo": "m"} for obs in ("o1", "o2")])
    _seed_edge_imports([("e2", "o1", 2), ("e3", "o2", 2)])

    out, _ = pull(EDGE, "edge", 3, "edge_1", edge=True)   # frames 1+2
    ack(EDGE, "edge", token_of(out))                      # -> 3

    # --- sender killed after the first event line was printed
    global _step
    _step += 1
    syncdir = os.path.join(SYNC, "crash_send")
    env = dict(BASE_ENV)
    env["RVAULT_CF_SYNC_DIR"] = syncdir
    env["RVAULT_CF_SYNC_WAIT"] = "feed.delivery.event.1"
    part = os.path.join(WORK, "edge", "partial_pull.txt")
    os.makedirs(os.path.dirname(part), exist_ok=True)
    with open(part, "w", encoding="utf-8") as f:
        p = subprocess.Popen([sys.executable, "-m", "rvault", "--store", EDGE,
                              "feed", "pull", "--consumer", "edge"],
                             stdout=f, stderr=subprocess.PIPE, text=True, env=env)
    wait_reached(syncdir, "feed.delivery.event.1")
    p.kill()
    p.wait()
    check(p.returncode == -signal.SIGKILL, "sender killed mid-output")
    partial = open(part, encoding="utf-8").read()
    check("ENDFRAME" not in partial and "END " not in partial,
          "recorded output is visibly truncated (no ENDFRAME/END)")
    # the pending delivery survived; a new process replays the same range/token
    st = status(EDGE, "edge")["consumer"]
    tok = st["pending_delivery"]["token"]
    check(st["pending_delivery"]["from_seq"] == 4
          and st["pending_delivery"]["through_seq"] == 4,
          "pending delivery survives the killed sender")
    out, kv = pull(EDGE, "edge", 100, "edge_2", edge=True, replay=1)
    check(token_of(out) == tok, "replay uses the same token after the crash")
    check("FRAME id=3" in out, "replayed delivery includes the whole frame")

    # --- ack committed, then the acking process is killed before it reports
    _step += 1
    syncdir = os.path.join(SYNC, "crash_ack")
    p = spawn(EDGE, ["feed", "ack", "--consumer", "edge", "--token", tok],
              ["feed.after_ack"], syncdir)
    wait_reached(syncdir, "feed.after_ack")
    p.kill()
    p.wait()
    check(p.returncode == -signal.SIGKILL, "acking process killed after commit")
    st = status(EDGE, "edge")["consumer"]
    check(st["acked_seq"] == 4, "ack survived the crash (read from a new process)")
    again = ack(EDGE, "edge", tok)
    check(again["replayed"] is True and again["acked_seq"] == 4,
          "re-acking the same token after restart changes nothing")
    out, _ = pull(EDGE, "edge", 100, None, edge=True)
    check("empty=1" in out.splitlines()[0], "no phantom delivery after the ack")

    # --- four concurrent pulls of the same consumer
    _seed_edge_imports([("e4", "o3", 1)])
    procs = []
    os.makedirs(os.path.join(WORK, "edge"), exist_ok=True)
    for i in range(4):
        path = os.path.join(WORK, "edge", "concurrent_%d.txt" % i)
        fh = open(path, "w", encoding="utf-8")
        procs.append((subprocess.Popen(
            [sys.executable, "-m", "rvault", "--store", EDGE, "feed", "pull",
             "--consumer", "edge", "--limit", "1"],
            stdout=fh, stderr=subprocess.PIPE, text=True, env=BASE_ENV), fh, path))
    outs = []
    for proc, fh, path in procs:
        proc.wait()
        fh.close()
        outs.append(open(path, encoding="utf-8").read())
    toks = {token_of(o) for o in outs}
    check(len(toks) == 1, "four concurrent pulls produced ONE delivery/token")
    check(all(len([ln for ln in o.splitlines()
                   if ln.startswith("EVENT")]) == 1 for o in outs),
          "every concurrent pull shows the same single-event frame")
    dbcheck(EDGE, "edge_concurrent_pending", edge=True)
    # one pull's output is a complete delivery; acknowledge it
    check(all(V.parse_pull(o)[0].get("delivery") in toks for o in outs),
          "all concurrent outputs carry the same pending token")
    ack(EDGE, "edge", toks.pop())                     # -> 5

    # --- foreign token / unknown consumer cannot advance anything
    reg = rvj(EDGE, "feed", "register", "--consumer", "other", "--from-seq", "5")
    check(reg["acked_seq"] == 4, "other starts just before frame 4")
    gen_import(EDGE, "e5", [{"study": "s1", "person": "pp",
                             "observation": "o4", "edition": 1,
                             "display_name": "Pn", "measure": 1, "memo": "m"}])
    out_other, _ = pull(EDGE, "other", 100, "edge_other", edge=True)
    ack(EDGE, "other", token_of(out_other))            # other -> 6
    out, _ = pull(EDGE, "edge", 100, "edge_4", edge=True)
    tok_e5 = token_of(out)
    r = rv(EDGE, "feed", "ack", "--consumer", "other", "--token", tok_e5, ok=(2,))
    check("another consumer" in r.stderr, "foreign token is rejected")
    check(status(EDGE, "other")["consumer"]["acked_seq"] == 6,
          "foreign token did not move the other consumer")
    r = rv(EDGE, "feed", "ack", "--consumer", "ghost", "--token", tok_e5, ok=(2,))
    check("unknown consumer" in r.stderr, "unknown consumer cannot ack")

    # --- disable revokes the outstanding credential; re-enable is a new life
    dis = rvj(EDGE, "feed", "disable", "--consumer", "edge")
    check(dis["revoked_deliveries"] == 1, "disable revokes the pending delivery")
    check(len(status(EDGE)["enabled_consumers"]) == 1, "other stays enabled")
    r = rv(EDGE, "feed", "ack", "--consumer", "edge", "--token", tok_e5, ok=(2,))
    check("disabled" in r.stderr, "a disabled consumer cannot ack")
    r = rv(EDGE, "feed", "register", "--consumer", "edge", ok=(2,))
    check("new lifecycle" in r.stderr,
          "re-enabling without an explicit start is refused")
    r = rv(EDGE, "feed", "register", "--consumer", "edge", "--reactivate",
           ok=(2,))
    check("explicit start" in r.stderr,
          "re-enabling from 'now' is refused: a start must be chosen")
    reg = rvj(EDGE, "feed", "register", "--consumer", "edge", "--reactivate",
              "--from-seq", "1")
    check(reg["generation"] == 2, "re-enable creates generation 2")
    r = rv(EDGE, "feed", "ack", "--consumer", "edge", "--token", tok_e5, ok=(2,))
    check("unknown token" in r.stderr,
          "credential from the old lifecycle is invalid after re-enable")
    out, _ = pull(EDGE, "edge", 100, "edge_replay_1", edge=True)
    ack(EDGE, "edge", token_of(out))
    dbcheck(EDGE, "edge_reborn", edge=True)

    # --- everyone catches up; e6/e7 arrive
    _seed_edge_imports([("e6", "o5", 1), ("e7", "o6", 1)])
    rvj(EDGE, "feed", "register", "--consumer", "ra", "--from-seq", "6")
    out, _ = pull(EDGE, "edge", 100, "edge_catch_up", edge=True)
    ack(EDGE, "edge", token_of(out))
    out, _ = pull(EDGE, "other", 100, "other_catch_up", edge=True)
    ack(EDGE, "other", token_of(out))
    out, _ = pull(EDGE, "ra", 100, "ra_catch_up", edge=True)
    ack(EDGE, "ra", token_of(out))
    dbcheck(EDGE, "edge_events", edge=True)


def phase_edge_races():
    print("-- phase 12: trim vs new registration - one explainable outcome")
    global _step
    # race A: the trim commits first, then the old start is refused
    _step += 1
    syncdir = os.path.join(SYNC, "race_a")
    p = spawn(EDGE, ["feed", "trim", "--through-seq", "4"],
              ["feed.trim.after_commit"], syncdir)
    wait_reached(syncdir, "feed.trim.after_commit")
    release(syncdir, "feed.trim.after_commit")
    out, err = p.communicate()
    check(p.returncode == 0, "trim committed")
    trim_res = json.loads(out)
    check(trim_res["retention_floor"] == 4 and trim_res["trimmed_frames"] == 3,
          "trim moved the floor to 4 (frames 1-3 fully acknowledged)")
    r = rv(EDGE, "feed", "register", "--consumer", "race_old",
           "--from-seq", "2", ok=(6,))
    err = json.loads(r.stderr)
    check(err["retention_floor"] == 4 and err["earliest_available_seq"] == 5,
          "registration after the trim reports the gap and the earliest position")
    check("race_old" not in status(EDGE)["enabled_consumers"] and
          "race_old" not in status(EDGE)["disabled_consumers"],
          "refused registration is atomic: no consumer row")
    dbcheck(EDGE, "race_a", edge=True)

    # race B: registration commits while holding the feed lock; the trim that
    # was already waiting then sees the new consumer and its retention hold
    _step += 1
    syncdir = os.path.join(SYNC, "race_b")
    p = spawn(EDGE, ["feed", "register", "--consumer", "race_keep",
                     "--from-seq", "5"], ["feed.register.before_commit"], syncdir)
    wait_reached(syncdir, "feed.register.before_commit")
    _step += 1
    # The registration is paused while holding the feed lock; the trim asks
    # for the same lock and must block.  Instead of sleeping, prove liveness
    # from the database: the registration not having committed yet is the
    # only reason a trim would still be running.
    trim_proc = subprocess.Popen(
        [sys.executable, "-m", "rvault", "--store", EDGE, "feed", "trim"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=BASE_ENV)
    deadline = time.time() + 60
    blocked = False
    while time.time() < deadline:
        if trim_proc.poll() is not None:
            break
        # the paused registration has not created the consumer row yet, so
        # the only correct behaviour for a trim is to keep waiting
        if "race_keep" not in status(EDGE)["enabled_consumers"] and \
                status(EDGE)["enabled_consumers"] and trim_proc.poll() is None:
            blocked = True
            break
        time.sleep(0.02)
    check(blocked and trim_proc.poll() is None,
          "trim waits for the feed lock instead of racing the registration")
    release(syncdir, "feed.register.before_commit")
    out, _ = p.communicate()
    check(p.returncode == 0, "registration committed")
    tout, terr = trim_proc.communicate()
    check(trim_proc.returncode == 0, "waiting trim completes after the commit")
    tres = json.loads(tout)
    # every enabled consumer is checked: edge(8) other(8) ra(8) race_keep(4),
    # so the watermark stays at 4 and nothing more is trimmed.  The events
    # race_keep was promised (from seq 5) are still retained.
    check(tres["retention_floor"] == 4 and tres["trimmed_frames"] == 0,
          "trim only removed frames every enabled consumer acked; the new "
          "consumer's start position 5 is still retained")
    dbcheck(EDGE, "race_b", edge=True)

    # --- no enabled consumers: unbounded trim refused, explicit bound works
    for c in ("edge", "other", "ra", "race_keep"):
        rvj(EDGE, "feed", "disable", "--consumer", c)
    st = status(EDGE)
    check(st["enabled_consumers"] == [], "all consumers disabled")
    r = rv(EDGE, "feed", "trim", ok=(2,))
    check("explicit" in r.stderr,
          "with no enabled consumer an unbounded trim is refused")
    res, _ = trim(EDGE, through=6)
    check(res["trimmed_frames"] == 2 and res["trimmed_events"] == 2
          and res["retention_floor"] == 6,
          "explicit upper bound trims only up to that bound (frames 4, 5)")
    dbcheck(EDGE, "no_consumers", edge=True)
    r = rv(EDGE, "feed", "register", "--consumer", "late2", "--from-seq", "6",
           ok=(6,))
    err = json.loads(r.stderr)
    check(err["retention_floor"] == 6 and err["earliest_available_seq"] == 7,
          "after the bound, position 6 is reported gone with the earliest 7")


def phase_big_frame():
    print("-- phase 13: a committed frame far larger than the pull target")
    rv(BIG, "init")
    n = E.BIG_EVENTS
    src = os.path.join(WORK, "big_rows.jsonl")
    with open(src, "w", encoding="utf-8") as f:
        for i in range(n):
            f.write(json.dumps({"study": "big", "person": "pp",
                                "observation": "o%06d" % i, "edition": 1,
                                "display_name": "Pn", "measure": i, "memo": "m"}) + "\n")
    rv(BIG, "import", "--batch-tag", "big", "--input", src)
    rvj(BIG, "feed", "register", "--consumer", "bigc", "--from-seq", "1")
    st = status(BIG)
    check(st["tail_seq"] == n and st["events_retained"] == n,
          "one committed frame holds %d events" % n)
    # stream it in a child; the child reports its own peak RSS, the parent
    # consumes the protocol line by line, so neither side holds the frame.
    code = r"""
import json, os, resource, sys
from rvault.store import Store
from rvault import changefeed
out = sys.stdout.buffer
lines = 0
first = last = None
start_line = end_line = frame_line = None
for line in changefeed.iter_delivery(Store(sys.argv[1]), "bigc", 100):
    data = line.encode()
    out.write(data)
    lines += 1
    text = data.decode()
    if text.startswith("START"):
        start_line = text.strip()
    elif text.startswith("FRAME"):
        frame_line = text.strip()
    elif text.startswith("EVENT"):
        parts = dict(p.split(b"=", 1) for p in data.split()[1:])
        if first is None:
            first = int(parts[b"seq"])
        last = int(parts[b"seq"])
    elif text.startswith("END "):
        end_line = text.strip()
out.flush()
ru = resource.getrusage(resource.RUSAGE_SELF)
sys.stderr.write(json.dumps({"maxrss_kb": ru.ru_maxrss, "lines": lines,
                             "first": first, "last": last,
                             "start": start_line, "frame": frame_line,
                             "end": end_line}) + "\n")
"""
    p = subprocess.Popen([sys.executable, "-c", code, BIG],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                         env=BASE_ENV)
    event_count = 0
    start_line = frame_line = end_line = None
    frame_headers = []
    while True:
        line = p.stdout.readline()
        if not line:
            break
        text = line.decode()
        if text.startswith("START"):
            start_line = text.strip()
        elif text.startswith("FRAME"):
            frame_headers.append(text.strip())
        elif text.startswith("END "):
            end_line = text.strip()
        elif text.startswith("EVENT"):
            event_count += 1
    p.wait()
    report = json.loads(p.stderr.readline())
    check(report["lines"] == n + 4,
          "start + frame + %d events + endframe + end = %d lines"
          % (n, n + 4))
    check(event_count == n, "parent observed every event exactly once")
    check(report["first"] == 1 and report["last"] == n,
          "first/last seq of the frame are 1..%d" % n)
    check(start_line is not None and "events=%d" % n in start_line,
          "START announces the whole oversized frame")
    check(start_line.split("target=")[1].split()[0] == "100",
          "target was 100 events, the frame was sent whole anyway")
    check(len(frame_headers) == 1 and
          frame_headers[0].split("events=")[1] == str(n),
          "FRAME header reports the real event count")
    check(end_line == "END events=%d frames=1 empty=0" % n,
          "END carries the true event count")
    check(report["maxrss_kb"] < 60 * 1024,
          "sender peak RSS %d kB (frame body ~%.1f MB) proves record-by-record "
          "streaming, not a resident full frame"
          % (report["maxrss_kb"], (n * 62) / 1024.0 / 1024.0))
    # ack and trim so the big frame leaves the store
    tok = start_line.split("delivery=")[1].split()[0]
    res = ack(BIG, "bigc", tok)
    check(res["acked_seq"] == n, "big frame acknowledged to its end")
    res, _ = trim(BIG)
    check(res["retention_floor"] == n and res["trimmed_frames"] == 1,
          "big frame trimmed after acknowledgement")
    return report["maxrss_kb"]


def phase_commit_visibility():
    """Interleave a pull with a producer that is paused AFTER its notification
    rows are written but BEFORE its COMMIT.  The consumer must see nothing
    until the business transaction commits, then exactly the frame."""
    print("-- phase 14: notifications are invisible until the business commit")
    store = os.path.join(WORK, "commit_vis")
    rv(store, "init")
    rvj(store, "feed", "register", "--consumer", "cv")
    os.makedirs(GEN, exist_ok=True)
    fixture = os.path.join(GEN, "cv.jsonl")
    with open(fixture, "w", encoding="utf-8") as f:
        for obs in ("oc1", "oc2"):
            f.write(json.dumps({"study": "cv", "person": "pp",
                                "observation": obs, "edition": 1,
                                "display_name": "Pn", "measure": 1,
                                "memo": "m"}) + "\n")
    global _step
    _step += 1
    syncdir = os.path.join(SYNC, "commit_vis")
    p = spawn(store, ["import", "--batch-tag", "cv1", "--input", fixture],
              ["feed.commit.finalize"], syncdir)
    wait_reached(syncdir, "feed.commit.finalize")
    # business rows and feed rows are both in the open transaction; neither
    # is visible to any other connection yet
    r = rv(store, "feed", "pull", "--consumer", "cv")
    check(r.stdout.startswith("START empty=1"),
          "an uncommitted business operation is not consumable")
    q = rvj(store, "query", "--study", "cv")
    check(q["observations"] == [], "nor is its payload visible to the source")
    release(syncdir, "feed.commit.finalize")
    p.communicate()
    check(p.returncode == 0, "producer committed")
    out = rv(store, "feed", "pull", "--consumer", "cv").stdout
    evs = [ln for ln in out.splitlines() if ln.startswith("EVENT")]
    check(evs == ["EVENT seq=1 frame=1 ordinal=1 op=refresh study=cv observation=oc1",
                  "EVENT seq=2 frame=1 ordinal=2 op=refresh study=cv observation=oc2"],
          "the whole frame becomes consumable exactly at COMMIT")
    ack(store, "cv", token_of(out))


# ---------------------------------------------------------------- legacy store
def phase_legacy_install():
    print("-- phase 15: installing on a store that already has data")
    legacy = os.path.join(WORK, "legacy")
    os.makedirs(legacy)
    # build the pre-feed schema exactly (no cf_* tables) with three objects
    import sqlite3
    sys.path.insert(0, PROJ)
    import rvault.db as db
    conn = sqlite3.connect(os.path.join(legacy, "vault.sqlite3"))
    sch = db.SCHEMA
    conn.executescript(sch[:sch.index("CREATE TABLE IF NOT EXISTS cf_meta")])
    conn.execute("INSERT INTO observation_owner VALUES ('obs','oba','alice')")
    conn.execute("INSERT INTO observation_owner VALUES ('obs','obb','bob')")
    for ed, memo in ((1, "a"), (2, "b")):
        conn.execute("INSERT INTO editions VALUES ('obs','obb',?,'bob','Bob',? ,?,'old')"
                     % ("", ""), (ed, ed, memo)) if False else None
    conn.execute("INSERT INTO editions VALUES ('obs','oba',1,'alice','Alice',1,'x','old')")
    conn.execute("INSERT INTO editions VALUES ('obs','obb',1,'bob','Bob',1,'y','old')")
    conn.execute("INSERT INTO editions VALUES ('obs','obb',2,'bob','Bob',2,'z','old')")
    conn.commit()
    conn.close()
    rvj(legacy, "feed", "register", "--consumer", "legacy", "--from-seq", "1")
    st = status(legacy)
    check(st["tail_seq"] == 0 and st["events_retained"] == 0,
          "installation did not back-fill history")
    gen_import(legacy, "new1", [{"study": "obs", "person": "bob",
                                 "observation": "obb", "edition": 3,
                                 "display_name": "Bob", "measure": 3,
                                 "memo": "new"}])
    _verifier(legacy, "--legacy")
    print("== legacy store: only the post-install write is notified")
    r = rv(legacy, "feed", "pull", "--consumer", "legacy")
    out = r.stdout
    check("observation=obb" in out and "observation=oba" not in out,
          "the only notification is for the newly revised object")
    ack(legacy, "legacy", token_of(out))
    r = rv(legacy, "feed", "pull", "--consumer", "legacy")
    check(r.stdout.startswith("START empty=1") and "END events=0" in r.stdout,
          "no second notification for the historical objects")


# ---------------------------------------------------------------- main
def main():
    if os.path.exists(WORK):
        shutil.rmtree(WORK)
    os.makedirs(LOGS)
    os.makedirs(SYNC)
    phase_install_and_register()
    phase_first_import()
    phase_revisions_and_duplicates()
    phase_rejected_batch()
    phase_register_slow()
    phase_withdrawal_at_barrier()
    phase_post_withdrawal_intake()
    phase_withdrawals_and_no_repeat()
    phase_slow_and_retention()
    phase_gap_diagnosis()
    phase_edge_crash_and_lifecycle()
    phase_edge_races()
    rss = phase_big_frame()
    phase_commit_visibility()
    phase_legacy_install()
    r = subprocess.run([sys.executable, os.path.join(DEMO, "verify_feed_txn.py")],
                       capture_output=True, text=True)
    log("verify_txn.log", r.stdout + r.stderr)
    check(r.returncode == 0, "producer-transaction / concurrent-producer checks")
    print(r.stdout.strip())
    print("\nFEED DEMO COMPLETE  (big-frame sender peak RSS: %d kB)" % rss)
    if _failures:
        print("%d EXPECTATION(S) FAILED" % len(_failures))
        return 1
    print("all change-feed expectations passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
