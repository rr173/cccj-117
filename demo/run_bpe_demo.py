#!/usr/bin/env python3
"""End-to-end demonstration of the independent BPE subsystem.

Every artifact (corpora, training databases, vocab files, encoded files,
measurement logs) is created fresh under one temporary directory; nothing is
written into an existing rvault store. The script:

  phase 1-2  hand-computed aaaa and rich-corpus answers, read back from the
             real SQLite database with demo/verify_bpe.py's own SQL;
  phase 3    slow full-recount reference vs. the incremental engine over
             shuffled orders and overlapping repeated pairs (the reference
             never calls the engine's counting/replacement functions);
  phase 4    encode/decode round trip including combining chars, emoji,
             spaces, newlines and empty strings;
  phase 5    SIGKILL at the deterministic mid-round stop point, then resume:
             the round is redone with no lost doc, no double count subtraction
             and no consumed token id;
  phase 6    two advancing processes meet at the flock: one waits, no
             divergent merge rounds are produced;
  phase 7    changed corpus / changed min_count / bad lines / bool weights
             are rejected with line numbers and leave no half corpus;
  phase 8    wrong-vocab decode, unknown token and non-UTF-8 decode refused;
  phase 9    30k+ sparse corpus: only the few hot docs are read or rewritten
             each round; per-round I/O counts and peak RSS are logged;
  phase 10   existing intake/withdrawal/nested commands keep working.

Usage:  python3 demo/run_bpe_demo.py
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)
sys.path.insert(0, HERE)

from rvault import bpe  # noqa: E402
import expected_bpe as expected  # noqa: E402
import verify_bpe as verify  # noqa: E402
from bpe_slow_ref import read_corpus, slow_train, slow_encode  # noqa: E402

PHASE = 0
LOG_LINES = []


def log(msg):
    print(msg)
    LOG_LINES.append(msg)


def phase(title):
    global PHASE
    PHASE += 1
    log("\n=== phase %d: %s ===" % (PHASE, title))


def write_jsonl(path, rows):
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        for obj in rows:
            f.write(json.dumps(obj, ensure_ascii=False,
                               separators=(",", ":")) + "\n")


def read_jsonl(path):
    with open(path, "r", encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def _base_env(**extra):
    env = dict(os.environ)
    env["PYTHONPATH"] = ROOT + os.pathsep + env.get("PYTHONPATH", "")
    env.update(extra)
    return env


def cli(*args, env=None, check=True):
    cmd = [sys.executable, "-m", "rvault"] + list(args)
    p = subprocess.run(cmd, cwd=ROOT, env=env or _base_env(), text=True,
                       capture_output=True)
    if check and p.returncode != 0:
        raise AssertionError("command failed (%d): %s\nSTDERR: %s"
                             % (p.returncode, cmd, p.stderr))
    return p


def expect_reject(*args, env=None):
    p = cli(*args, env=env, check=False)
    assert p.returncode in (2, 4, 5), (p.returncode, p.stderr)
    payload = json.loads(p.stderr)
    log("  rejected as expected: %s %s"
        % (payload.get("code"),
           json.dumps(payload.get("errors", ""), ensure_ascii=False)[:200]))
    return payload


# ---------------------------------------------------------------------------
def phase_aaaa(work):
    phase("weighted aaaa: the two hand-computed rounds")
    path = os.path.join(work, "aaaa.jsonl")
    write_jsonl(path, [{"id": "x", "weight": 2, "text": "aaaa"}])
    db = os.path.join(work, "aaaa.db")
    out = bpe.train(db, path, max_merges=3, min_count=1)
    assert out["stop_reason"] == "no_pairs_left"
    rounds = bpe.round_rows(db)
    assert len(rounds) == len(expected.AAAA_ROUNDS)
    for got_r, exp_r in zip(rounds, expected.AAAA_ROUNDS):
        for k, v in exp_r.items():
            gk = {"left": "left_id", "right": "right_id"}.get(k, k)
            assert got_r[gk] == v, (got_r, exp_r)
    summary = verify.verify_database(db, expected.AAAA_ROUNDS,
                                     expected.AAAA_FINAL)
    log("  rounds read back from SQLite: %s"
        % json.dumps(rounds, ensure_ascii=False))
    log("  independent SQL verification: %s" % summary)


def phase_rich(work):
    phase("rich corpus: tie-break, duplicate text ids, false cross-text "
          "pairs, empty text, non-ASCII")
    path = os.path.join(work, "rich.jsonl")
    rows = [
        {"id": "a", "weight": 1, "text": "abc"},
        {"id": "b", "weight": 2, "text": "ba"},
        {"id": "c", "weight": 2, "text": "abc"},
        {"id": "d", "weight": 1, "text": ""},
        {"id": "e", "weight": 1, "text": "é"},
        {"id": "f", "weight": 1, "text": "\nb"},
    ]
    write_jsonl(path, rows)
    db = os.path.join(work, "rich.db")
    bpe.train(db, path, max_merges=10, min_count=1)

    # The false cross-text pairs must never exist in any maintained table.
    conn = verify.open_db(db)
    try:
        for left, right in expected.RICH_FALSE_PAIRS:
            hit = conn.execute(
                "SELECT COUNT(*) c FROM bp_pairs WHERE left_id=? AND right_id=?",
                (left, right)).fetchone()["c"]
            assert hit == 0, (left, right)
    finally:
        conn.close()
    log("  false cross-text pairs %s absent from bp_pairs"
        % expected.RICH_FALSE_PAIRS)

    got = bpe.round_rows(db)
    for got_r, exp_r in zip(got, expected.RICH_ROUNDS):
        for k, v in exp_r.items():
            gk = {"left": "left_id", "right": "right_id"}.get(k, k)
            assert got_r[gk] == v, (got_r, exp_r)
    log("  per-round winners/counts match the hand calculation:")
    for r in got:
        log("    round %d: (%d,%d)->%d count=%d hit_docs=%d positions=%d"
            % (r["round"], r["left_id"], r["right_id"], r["token_id"],
               r["weighted_count"], r["docs_rewritten"],
               r["positions_replaced"]))
    verify.verify_database(db, expected.RICH_ROUNDS, expected.RICH_FINAL)
    log("  final token sequences: %s" % expected.RICH_FINAL)


def phase_reference(work):
    phase("slow full-recount reference vs. the disk-backed engine")
    rng_seeds = [1, 7, 42]
    materials = [
        # overlapping repeated pairs inside one text
        [("x", 2, "aaaaaaaa"), ("y", 3, "ababab"), ("z", 1, "éaa")],
        [("p", 5, "mississippi"), ("q", 2, "miss"), ("r", 4, "ssiss")],
        [("u", 1, "a"), ("v", 1, ""), ("w", 7, "ttttttt")],
        [("m1", 2, "abcabcabc"), ("m2", 1, "bcabca"), ("m3", 3, "cab")],
    ]
    case_no = 0
    for material in materials:
        for seed in rng_seeds:
            case_no += 1
            order = list(material)
            # Deterministic shuffles with varying orders.
            import random
            random.Random(seed * 100 + case_no).shuffle(order)
            path = os.path.join(work, "ref_%d.jsonl" % case_no)
            write_jsonl(path, [{"id": i, "weight": w, "text": t}
                               for i, w, t in order])

            ref_docs = read_corpus(path)
            ref_merges, ref_final, ref_rounds = slow_train(
                ref_docs, max_merges=50, min_count=1)
            ref_final = dict(ref_final)

            db = os.path.join(work, "ref_%d.db" % case_no)
            bpe.train(db, path, max_merges=50, min_count=1)
            conn = verify.open_db(db)
            try:
                engine_merges = [(r["round"], r["token_id"], r["left_id"],
                                  r["right_id"], r["weighted_count"])
                                 for r in conn.execute(
                                     "SELECT * FROM bp_rounds ORDER BY round")]
                engine_final = {
                    r["doc_id"]: verify._unpack(r["tokens"])
                    for r in conn.execute("SELECT doc_id,tokens FROM bp_docs")}
            finally:
                conn.close()
            assert engine_merges == ref_merges
            assert engine_final == ref_final
            verify.verify_database(db)
    log("  %d material x shuffle cases: identical merge sequences and final "
        "token sequences" % case_no)

    # Encoder cross-check on unseen texts.
    db = os.path.join(work, "ref_1.db")
    vocab = bpe.vocab_document(db)
    merges = [(m["left"], m["right"], m["token"]) for m in vocab["merges"]]
    unseen = ["aabbabab", "", "组合字符👩‍👩‍👧", "x" * 30, "\t\n "]
    for text in unseen:
        assert bpe.encode_bytes(text.encode("utf-8"),
                                merges) == slow_encode(text, merges), text
    log("  encoder matches the reference on %d unseen texts (empty, emoji "
        "ZWJ sequence, whitespace included)" % len(unseen))


def phase_codec(work):
    phase("streaming encode/decode: reversible, not guaranteed to compress")
    db = os.path.join(work, "rich.db")
    vocab_path = os.path.join(work, "rich.vocab.json")
    doc = bpe.vocab_document(db)
    with open(vocab_path, "w", encoding="utf-8", newline="\n") as f:
        json.dump(doc, f, ensure_ascii=False)
    enc_in = os.path.join(work, "enc_in.jsonl")
    write_jsonl(enc_in, expected.RICH_ENCODE_CASES)
    enc_out = os.path.join(work, "enc_out.jsonl")
    dec_out = os.path.join(work, "dec_out.jsonl")
    summary = bpe.encode_file(vocab_path, enc_in, enc_out)
    log("  encoded %d records (vocab %s)"
        % (summary["records"], summary["vocab_digest"][:12]))
    bpe.decode_file(vocab_path, enc_out, dec_out)
    assert read_jsonl(dec_out) == expected.RICH_ENCODE_CASES
    # Byte-level identity, not just JSON equality.
    with open(enc_in, "rb") as f1, open(dec_out, "rb") as f2:
        assert f1.read() == f2.read()
    log("  decoded bytes are identical to the input bytes (combining marks, "
        "emoji, spaces, newline, empty string)")
    verify.verify_encoded_file(
        doc, enc_out, {c["id"]: c["text"]
                       for c in expected.RICH_ENCODE_CASES})


def _train_subprocess(env, db, corpus, max_merges, min_count=1):
    return subprocess.Popen(
        [sys.executable, "-m", "rvault", "bpe", "train",
         "--database", db, "--input", corpus,
         "--max-merges", str(max_merges), "--min-count", str(min_count)],
        cwd=ROOT, env=env, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def phase_crash(work):
    phase("SIGKILL mid-round then resume: the round is redone atomically")
    corpus = os.path.join(work, "rich.jsonl")
    db = os.path.join(work, "crash.db")
    sync_dir = os.path.join(work, "sync")
    os.makedirs(sync_dir, exist_ok=True)
    env = _base_env(RVAULT_SYNC_DIR=sync_dir,
                    RVAULT_SYNC_WAIT="bpe.round.after_doc")
    p = _train_subprocess(env, db, corpus, max_merges=10)
    # Wait until round 1 has rewritten its first doc and is blocked before
    # confirming the round.
    deadline = time.time() + 60
    while not os.path.exists(os.path.join(sync_dir,
                                          "bpe.round.after_doc.reached")):
        if p.poll() is not None:
            raise AssertionError("training exited early: %s" % p.stderr.read())
        assert time.time() < deadline
        time.sleep(0.02)
    time.sleep(0.1)  # let the staging transaction settle
    os.kill(p.pid, signal.SIGKILL)
    p.wait()
    log("  process killed after a partial round 1 rewrite (SIGKILL)")

    # Inspect the real database from a fresh process.
    conn = verify.open_db(db)
    try:
        meta = {r["key"]: r["value"]
                for r in conn.execute("SELECT key,value FROM bp_meta")}
        assert meta["state"] == "merging"
        assert int(meta["rounds_done"]) == 0
        assert int(meta["next_token"]) == 256  # no token id consumed
        assert conn.execute(
            "SELECT COUNT(*) c FROM bp_vocab").fetchone()["c"] == 0
        assert conn.execute(
            "SELECT COUNT(*) c FROM bp_rounds").fetchone()["c"] == 0
        staged = conn.execute(
            "SELECT COUNT(*) c FROM bp_round_docs").fetchone()["c"]
        planned = conn.execute(
            "SELECT COUNT(*) c FROM bp_round_plan").fetchone()["c"]
        assert 0 < staged <= planned
        # Maintained tables still describe the pre-round state.
        cnt = conn.execute(
            "SELECT count FROM bp_pairs WHERE left_id=97 AND right_id=98"
        ).fetchone()["count"]
        assert cnt == 3
    finally:
        conn.close()
    log("  on-disk check: state=merging, no vocab/round row, no token id "
        "consumed, pair counts still at pre-round values, %d/%d docs staged"
        % (staged, planned))

    # Resume must discard staging and redo the whole round.
    out = bpe.resume(db, corpus, max_merges=10, min_count=1)
    assert out["rounds_done"] == len(expected.RICH_ROUNDS)
    verify.verify_database(db, expected.RICH_ROUNDS, expected.RICH_FINAL)
    conn = verify.open_db(db)
    try:
        assert conn.execute(
            "SELECT COUNT(*) c FROM bp_round_docs").fetchone()["c"] == 0
    finally:
        conn.close()
    log("  resume redid the round cleanly; full expected result restored")


def phase_concurrent(work):
    phase("two advancing processes meet at the flock")
    corpus = os.path.join(work, "rich.jsonl")
    db = os.path.join(work, "rich.db")
    # First finish training in a copy that is deliberately stopped early.
    base = os.path.join(work, "concur.db")
    bpe.train(base, corpus, max_merges=2, min_count=1)
    lock = base + ".lock"

    env_wait = _base_env(
        RVAULT_SYNC_WAIT="bpe.round.before_confirm")
    sync_dir = os.path.join(work, "sync2")
    os.makedirs(sync_dir, exist_ok=True)
    env_wait["RVAULT_SYNC_DIR"] = sync_dir
    p1 = subprocess.Popen(
        [sys.executable, "-m", "rvault", "bpe", "resume",
         "--database", base, "--input", corpus,
         "--max-merges", "4", "--min-count", "1", "--lock", lock],
        cwd=ROOT, env=env_wait, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    deadline = time.time() + 60
    while not os.path.exists(os.path.join(
            sync_dir, "bpe.round.before_confirm.reached")):
        if p1.poll() is not None:
            raise AssertionError(p1.stderr.read())
        assert time.time() < deadline
        time.sleep(0.02)
    # p1 holds the exclusive flock; p2 must block, not diverge.
    t0 = time.time()
    p2 = subprocess.Popen(
        [sys.executable, "-m", "rvault", "bpe", "resume",
         "--database", base, "--input", corpus,
         "--max-merges", "10", "--min-count", "1", "--lock", lock],
        cwd=ROOT, text=True, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)
    time.sleep(1.0)
    assert p2.poll() is None, "second process must block on the flock"
    log("  second process blocked while the first holds the lock "
        "(%.1fs elapsed)" % (time.time() - t0))
    # Release p1 by letting it proceed.
    open(os.path.join(sync_dir,
                      "bpe.round.before_confirm.proceed"), "w").close()
    out1, err1 = p1.communicate(timeout=60)
    assert p1.returncode == 0, err1
    out2, err2 = p2.communicate(timeout=60)
    assert p2.returncode == 0, err2
    rounds1 = json.loads(out1)
    rounds2 = json.loads(out2)
    log("  process A: %s" % {k: rounds1[k] for k in
                             ("rounds_run_now", "rounds_done")})
    log("  process B: %s" % {k: rounds2[k] for k in
                             ("rounds_run_now", "rounds_done")})
    # Exactly one coherent merge sequence exists in the database.
    verify.verify_database(base, expected.RICH_ROUNDS, expected.RICH_FINAL)
    conn = verify.open_db(base)
    try:
        n = conn.execute("SELECT COUNT(*) c FROM bp_rounds").fetchone()["c"]
        ids = [r["token_id"] for r in conn.execute(
            "SELECT token_id FROM bp_vocab ORDER BY round")]
    finally:
        conn.close()
    assert n == len(expected.RICH_ROUNDS) and ids == list(range(256, 261))
    log("  one coherent merge sequence, consecutive token ids 256..260")


def phase_rejections(work):
    phase("bad input, duplicate ids, illegal types and identity mismatches")
    good = os.path.join(work, "rich.jsonl")

    # 1. Bad lines with line numbers; nothing may be written.
    bad = os.path.join(work, "bad.jsonl")
    with open(bad, "w", encoding="utf-8", newline="\n") as f:
        f.write('{"id":"ok1","weight":1,"text":"a"}\n')
        f.write('not json at all\n')
        f.write('{"id":"ok2","weight":true,"text":"b"}\n')  # bool weight
        f.write('{"id":"ok3","weight":1.5,"text":"c"}\n')   # float weight
        f.write('{"id":"","weight":1,"text":"d"}\n')         # empty id
        f.write('{"id":"ok4","weight":0,"text":"e"}\n')      # non-positive
        f.write('{"id":"ok5","weight":1}\n')                 # missing text
        f.write('{"id":"ok6","weight":1,"text":"f","x":1}\n')  # extra field
        f.write('{"id":"ok1","weight":1,"text":"g"}\n')      # duplicate id
        f.write('{"id":"ok7","weight":1,"text":12}\n')       # text not str
    target = os.path.join(work, "should_not_exist.db")
    payload = expect_reject("bpe", "train", "--database", target,
                            "--input", bad, "--max-merges", "5",
                            "--min-count", "1")
    assert not os.path.exists(target)
    lines = sorted(e["line"] for e in payload["errors"])
    assert lines == [2, 3, 4, 5, 6, 7, 8, 9, 10], lines
    log("  all 9 bad lines reported with line numbers; no database created")

    # 2. Re-training onto an existing database is refused.
    existing = os.path.join(work, "rich.db")
    expect_reject("bpe", "train", "--database", existing,
                  "--input", good, "--max-merges", "5", "--min-count", "1")

    # 3. Changed corpus cannot resume an old state.
    changed = os.path.join(work, "changed.jsonl")
    write_jsonl(changed, [{"id": "a", "weight": 1, "text": "DIFFERENT"}])
    expect_reject("bpe", "resume", "--database", existing,
                  "--input", changed, "--max-merges", "10")

    # 4. Changed min_count cannot resume an old state.
    expect_reject("bpe", "resume", "--database", existing,
                  "--input", good, "--max-merges", "10",
                  "--min-count", "999")

    # 5. A resume without the input still works (identity comes from disk).
    out = bpe.resume(existing, max_merges=10)
    assert out["rounds_done"] == len(expected.RICH_ROUNDS)
    log("  resume without re-supplying the corpus uses the stored identity")


def phase_decode_errors(work):
    phase("wrong vocabulary, unknown token and non-UTF-8 are refused")
    vocab_path = os.path.join(work, "rich.vocab.json")
    other_db = os.path.join(work, "aaaa.db")
    other_vocab = os.path.join(work, "aaaa.vocab.json")
    doc2 = bpe.vocab_document(other_db)
    with open(other_vocab, "w", encoding="utf-8", newline="\n") as f:
        json.dump(doc2, f, ensure_ascii=False)
    enc_out = os.path.join(work, "enc_out.jsonl")

    # 1. Encoded file bound to a different vocab: silent misdecode refused.
    p = expect_reject("bpe", "decode", "--vocab", other_vocab,
                      "--input", enc_out,
                      "--output", os.path.join(work, "nope1.jsonl"))
    assert p["code"] == "vocab_mismatch"

    # 2. Unknown token id beyond the vocabulary range.
    bad_enc = os.path.join(work, "bad_enc.jsonl")
    vocab = json.load(open(vocab_path, encoding="utf-8"))
    with open(bad_enc, "w", encoding="utf-8", newline="\n") as f:
        f.write(json.dumps({"format": bpe.ENCODED_FORMAT,
                            "version": bpe.FORMAT_VERSION,
                            "vocab_digest": vocab["vocab_digest"]}) + "\n")
        f.write(json.dumps({"id": "z", "tokens": [97, 999999]}) + "\n")
    p = expect_reject("bpe", "decode", "--vocab", vocab_path,
                      "--input", bad_enc,
                      "--output", os.path.join(work, "nope2.jsonl"))
    assert p["code"] == "unknown_token"

    # 3. Tokens that expand to bytes which are not valid UTF-8.
    bad_utf8 = os.path.join(work, "bad_utf8.jsonl")
    with open(bad_utf8, "w", encoding="utf-8", newline="\n") as f:
        f.write(json.dumps({"format": bpe.ENCODED_FORMAT,
                            "version": bpe.FORMAT_VERSION,
                            "vocab_digest": vocab["vocab_digest"]}) + "\n")
        f.write(json.dumps({"id": "z", "tokens": [0xFF, 0xFE]}) + "\n")
    p = expect_reject("bpe", "decode", "--vocab", vocab_path,
                      "--input", bad_utf8,
                      "--output", os.path.join(work, "nope3.jsonl"))
    assert p["code"] == "invalid_utf8"

    # 4. Tampered vocabulary file fails its own digest.
    tampered = os.path.join(work, "tampered.vocab.json")
    evil = json.load(open(vocab_path, encoding="utf-8"))
    evil["min_count"] = 12345
    with open(tampered, "w", encoding="utf-8", newline="\n") as f:
        json.dump(evil, f)
    expect_reject("bpe", "decode", "--vocab", tampered,
                  "--input", enc_out,
                  "--output", os.path.join(work, "nope4.jsonl"))

    # 5. Malformed encoded header.
    bad_header = os.path.join(work, "bad_header.jsonl")
    with open(bad_header, "w", encoding="utf-8", newline="\n") as f:
        f.write(json.dumps({"format": "something-else"}) + "\n")
    expect_reject("bpe", "decode", "--vocab", vocab_path,
                  "--input", bad_header,
                  "--output", os.path.join(work, "nope5.jsonl"))
    log("  every malformed/foreign input was explicitly refused")


def _sample_rss(pid, total_timeout=300, interval=0.05):
    """Sample /proc/<pid>/status VmRSS; return (peak_kb) when the pid exits.

    Sampling stops as soon as the process disappears; the timeout only guards
    against a runaway process.
    """
    peak = 0
    deadline = time.time() + total_timeout
    while time.time() < deadline:
        try:
            with open("/proc/%d/status" % pid) as f:
                for line in f:
                    if line.startswith("VmRSS:"):
                        peak = max(peak, int(line.split()[1]))
                        break
        except FileNotFoundError:
            return peak
        # Stop once the child has exited.
        try:
            done, _ = os.waitpid(pid, os.WNOHANG)
            if done:
                return peak
        except ChildProcessError:
            return peak
        time.sleep(interval)
    return peak


def _cold_char(i: int) -> str:
    """A permutation over a wide code-point space.

    A coprime stride over ~44k code points spreads the cold characters
    across many three-byte UTF-8 lead/continuation combinations, so no cold
    byte pair accumulates a high count.  All code points stay <= U+FFFF so
    each text is exactly three bytes.
    """
    base, span, stride = 0x4E00, 0x4E00, 7919
    return chr(base + (i * stride) % span)


def phase_sparse(work):
    phase("30k sparse corpus: only hot docs are read or rewritten")
    corpus = os.path.join(work, "sparse.jsonl")
    hot_n = 20
    cold_n = 30000
    min_count = 20000
    rows = [{"id": "hot-%02d" % i, "weight": 100, "text": "th" * 64}
            for i in range(hot_n)]
    for i in range(cold_n):
        rows.append({"id": "cold-%05d" % i, "weight": 1,
                     "text": _cold_char(i)})
    write_jsonl(corpus, rows)

    # Guard: the cold side never contains the hot pair and its byte pairs all
    # stay well below min_count (they are count-1 at the doc level).
    cold_pair_docs = {}
    for obj in rows[hot_n:]:
        data = obj["text"].encode("utf-8")
        for j in range(len(data) - 1):
            pair = (data[j], data[j + 1])
            cold_pair_docs[pair] = cold_pair_docs.get(pair, 0) + 1
    max_cold = max(cold_pair_docs.values())
    assert (ord("t"), ord("h")) not in cold_pair_docs
    assert max_cold < min_count, max_cold
    log("  corpus: %d hot docs (weight 100, 'th'x64) + %d cold docs; "
        "largest cold byte-pair count is %d (< min_count %d); the hot pair "
        "is absent from every cold doc"
        % (hot_n, cold_n, max_cold, min_count))

    db = os.path.join(work, "sparse.db")
    t0 = time.time()
    proc = subprocess.Popen(
        [sys.executable, "-m", "rvault", "bpe", "train",
         "--database", db, "--input", corpus,
         "--max-merges", "20", "--min-count", str(min_count)],
        cwd=ROOT, env=_base_env(), text=True, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)
    peak_kb = _sample_rss(proc.pid, total_timeout=300)
    out, err = proc.communicate(timeout=300)
    assert proc.returncode == 0, err
    elapsed = time.time() - t0
    summary = json.loads(out)
    assert summary["stop_reason"] == "below_min_count"
    assert summary["rounds_done"] == 4

    conn = verify.open_db(db)
    try:
        rounds = [dict(r) for r in conn.execute(
            "SELECT * FROM bp_rounds ORDER BY round")]
        # Every round touches only the 20 hot docs.
        for r in rounds:
            assert r["docs_hit"] == hot_n and r["docs_rewritten"] == hot_n
        # Cold docs are never rewritten: they keep one uint32 per original
        # byte, every token id is a base byte (<256) and the sequence length
        # equals the raw byte length.
        cold_total = conn.execute(
            "SELECT COUNT(*) c FROM bp_docs WHERE doc_id LIKE 'cold-%'"
        ).fetchone()["c"]
        untouched = conn.execute(
            "SELECT COUNT(*) c FROM bp_docs WHERE doc_id LIKE 'cold-%' "
            "AND seq_len = length(raw) "
            "AND length(tokens) = 4 * length(raw)").fetchone()["c"]
        assert cold_total == cold_n and untouched == cold_n, untouched
        # No learned token (>=256) ever appears in a cold doc.
        learned_in_cold = 0
        for r in conn.execute(
                "SELECT tokens FROM bp_docs WHERE doc_id LIKE 'cold-%'"):
            if any(t >= 256 for t in verify._unpack(r["tokens"])):
                learned_in_cold += 1
        assert learned_in_cold == 0
        cold_rows = conn.execute(
            "SELECT COUNT(*) c FROM bp_pair_hits WHERE doc_id LIKE 'cold-%'"
        ).fetchone()["c"]
        # Each 3-byte cold doc contributes exactly two count-1 hit rows.
        assert cold_rows == 2 * cold_n, cold_rows
        # The hot pair lineage in the vocabulary.
        vocab = [dict(r) for r in conn.execute(
            "SELECT * FROM bp_vocab ORDER BY round")]
    finally:
        conn.close()
    log("  per-round actual doc reads/rewrites (all rounds):")
    for r in rounds:
        log("    round %d: (%d,%d)->%d count=%d docs_read=%d "
            "docs_rewritten=%d positions=%d"
            % (r["round"], r["left_id"], r["right_id"], r["token_id"],
               r["weighted_count"], r["docs_hit"], r["docs_rewritten"],
               r["positions_replaced"]))
    verify.verify_database(db)
    line = ("sparse: docs=%d hot=%d rounds=%d elapsed_s=%.2f "
            "sampled_peak_rss_kb=%d (cold docs never read or rewritten: "
            "%d/%d untouched)"
            % (hot_n + cold_n, hot_n, len(rounds), elapsed, peak_kb,
               untouched, cold_n))
    log("  " + line)
    with open(os.path.join(work, "sparse_measurements.log"),
              "w", encoding="utf-8") as f:
        f.write(line + "\n")
        for r in rounds:
            f.write(json.dumps(r) + "\n")


def phase_existing_commands(work):
    phase("existing intake / withdrawal / nested commands keep working")
    store = os.path.join(work, "vault")
    p = cli("--store", store, "init")
    assert p.returncode == 0
    batch = os.path.join(work, "batch.jsonl")
    write_jsonl(batch, [{
        "study": "s1", "person": "p1", "observation": "o1", "edition": 1,
        "display_name": "A", "measure": 10, "memo": ""}])
    p = cli("--store", store, "import", "--batch-tag", "b1",
            "--input", batch)
    assert p.returncode == 0
    p = cli("--store", store, "query", "--study", "s1")
    assert '"observation": "o1"' in p.stdout
    p = cli("--store", store, "withdraw", "register",
            "--withdrawal-id", "W1", "--study", "s1", "--person", "p1")
    assert p.returncode == 0
    p = cli("--store", store, "withdraw", "status", "--withdrawal-id", "W1")
    assert p.returncode == 0
    # Nested subsystem: it remains a separate module behind the documented
    # optional ijson dependency. Either a working plan validation or the
    # documented missing-dependency response (exit 4) proves the command path
    # is untouched; it must never touch the BPE database.
    plan = os.path.join(work, "plan.json")
    with open(plan, "w", encoding="utf-8") as f:
        json.dump({"version": 1,
                   "root": {"name": "participants", "columns": []},
                   "tables": []}, f)
    p = cli("--store", store, "nested", "plan", "show", "--plan", plan,
            check=False)
    assert p.returncode in (0, 4), p.stderr
    if p.returncode == 4:
        assert json.loads(p.stderr)["code"] == "missing_dependency"
        log("  nested command path intact (ijson optional dependency "
            "reported as documented)")
    else:
        log("  nested plan validation works as before")
    log("  init/import/query/withdraw all behave as before")


def main():
    work = tempfile.mkdtemp(prefix="rvault_bpe_demo_")
    log("working directory: %s" % work)
    try:
        phase_aaaa(work)
        phase_rich(work)
        phase_reference(work)
        phase_codec(work)
        phase_crash(work)
        phase_concurrent(work)
        phase_rejections(work)
        phase_decode_errors(work)
        phase_sparse(work)
        phase_existing_commands(work)
    except Exception:
        log("\nDEMO FAILED; artifacts kept in: %s" % work)
        raise
    with open(os.path.join(work, "demo.log"), "w", encoding="utf-8") as f:
        f.write("\n".join(LOG_LINES) + "\n")
    log("\nALL BPE DEMO PHASES PASSED")
    log("artifacts, databases and measurement log: %s" % work)


if __name__ == "__main__":
    main()
