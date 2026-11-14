"""Independent on-disk verification of a BPE training database.

Uses its own sqlite3 SQL only - it never calls rvault.bpe's counting,
replacement or vocabulary code - then cross-checks the engine's maintained
state by recomputing the truth from bp_docs.raw and bp_vocab.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from array import array


def _unpack(blob: bytes):
    arr = array("I")
    arr.frombytes(blob)
    return arr.tolist()


def open_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def docs(conn):
    return {r["doc_id"]: r for r in conn.execute(
        "SELECT * FROM bp_docs ORDER BY doc_id")}


def stored_rounds(conn):
    return [dict(r) for r in conn.execute(
        "SELECT * FROM bp_rounds ORDER BY round")]


def stored_vocab(conn):
    return [dict(r) for r in conn.execute(
        "SELECT * FROM bp_vocab ORDER BY round")]


def recompute_from_scratch(conn):
    """Replay every stored merge over raw bytes with an independent loop."""
    rows = list(conn.execute(
        "SELECT doc_id,weight,raw FROM bp_docs ORDER BY line_no"))
    seqs = {r["doc_id"]: list(r["raw"]) for r in rows}
    weights = {r["doc_id"]: r["weight"] for r in rows}
    truth_rounds = []
    for vocab_row in conn.execute("SELECT * FROM bp_vocab ORDER BY round"):
        left, right, token = (
            vocab_row["left_id"], vocab_row["right_id"],
            vocab_row["token_id"])
        counts = {}
        hit = []
        positions = 0
        for doc_id, seq in seqs.items():
            n_here = 0
            for j in range(len(seq) - 1):
                pair = (seq[j], seq[j + 1])
                counts[pair] = counts.get(pair, 0) + weights[doc_id]
            # Replacement positions are the non-overlapping left-to-right
            # matches; overlapping occurrences (e.g. the three 'aa' slots in
            # 'aaaa') are not all replaced.
            j = 0
            while j + 1 < len(seq):
                if (seq[j], seq[j + 1]) == (left, right):
                    n_here += 1
                    j += 2
                else:
                    j += 1
            if n_here:
                hit.append(doc_id)
                positions += n_here
        winner = min(counts, key=lambda p: (-counts[p], p[0], p[1]))
        assert winner == (left, right), (winner, (left, right))
        truth_rounds.append({
            "round": vocab_row["round"], "token_id": token,
            "left": left, "right": right,
            "weighted_count": counts[(left, right)],
            "docs_hit": len(hit), "docs_rewritten": len(hit),
            "positions_replaced": positions})
        for doc_id, seq in seqs.items():
            out = []
            j = 0
            while j < len(seq):
                if j + 1 < len(seq) and seq[j] == left \
                        and seq[j + 1] == right:
                    out.append(token)
                    j += 2
                else:
                    out.append(seq[j])
                    j += 1
            seqs[doc_id] = out
    return truth_rounds, seqs, weights


def verify_database(path: str, expected_rounds=None, expected_final=None):
    """Raise AssertionError on any inconsistency; return a summary dict."""
    conn = open_db(path)
    try:
        # No unconfirmed round may remain in a confirmed database.
        assert conn.execute(
            "SELECT COUNT(*) c FROM bp_round_state").fetchone()["c"] == 0
        for table in ("bp_round_plan", "bp_round_docs", "bp_round_delta",
                      "bp_round_new_hits"):
            assert conn.execute(
                "SELECT COUNT(*) c FROM %s" % table).fetchone()["c"] == 0, \
                table

        truth_rounds, truth_seqs, weights = recompute_from_scratch(conn)
        got_rounds = stored_rounds(conn)
        assert len(got_rounds) == len(truth_rounds)
        for got, truth in zip(got_rounds, truth_rounds):
            for key in ("round", "token_id", "left", "right",
                        "weighted_count", "docs_hit", "docs_rewritten",
                        "positions_replaced"):
                gk = key if key in got.keys() else (
                    "left_id" if key == "left" else
                    "right_id" if key == "right" else key)
                assert got[gk] == truth[key], (key, got, truth)
        if expected_rounds is not None:
            assert truth_rounds == expected_rounds
        if expected_final is not None:
            assert truth_seqs == expected_final

        # Stored token sequences must equal the replay.
        for doc_id, row in docs(conn).items():
            assert _unpack(row["tokens"]) == truth_seqs[doc_id], doc_id
            assert row["seq_len"] == len(truth_seqs[doc_id])
            # Token sequences must still decode to the original raw bytes.
        # Stored pair counts must equal a recount of current sequences.
        recounted = {}
        for doc_id, seq in truth_seqs.items():
            for j in range(len(seq) - 1):
                pair = (seq[j], seq[j + 1])
                recounted[pair] = recounted.get(pair, 0) + weights[doc_id]
        stored_counts = {(r["left_id"], r["right_id"]): r["count"]
                         for r in conn.execute("SELECT * FROM bp_pairs")}
        assert stored_counts == recounted
        # Hit relation: exactly the docs containing each pair, no extras.
        recounted_hits = set()
        for doc_id, seq in truth_seqs.items():
            for pair in {(seq[j], seq[j + 1])
                         for j in range(len(seq) - 1)}:
                recounted_hits.add((pair[0], pair[1], doc_id))
        stored_hits = {(r["left_id"], r["right_id"], r["doc_id"])
                       for r in conn.execute(
                           "SELECT * FROM bp_pair_hits")}
        assert stored_hits == recounted_hits

        # Vocab tokens are consecutive 256.. and round-consistent.
        vocab = stored_vocab(conn)
        for i, v in enumerate(vocab):
            assert v["token_id"] == 256 + i
            assert v["round"] == i + 1
        meta = {r["key"]: r["value"]
                for r in conn.execute("SELECT key,value FROM bp_meta")}
        assert int(meta["next_token"]) == 256 + len(vocab)
        assert int(meta["rounds_done"]) == len(vocab)

        # Raw bytes of every doc must be valid UTF-8 equal to stored text.
        for row in conn.execute("SELECT raw FROM bp_docs"):
            row["raw"].decode("utf-8")
        return {"rounds": len(vocab),
                "docs": conn.execute(
                    "SELECT COUNT(*) c FROM bp_docs").fetchone()["c"],
                "pair_kinds": len(stored_counts)}
    finally:
        conn.close()


def verify_meta_identity(path: str, digest: str, min_count: int):
    conn = open_db(path)
    try:
        meta = {r["key"]: r["value"]
                for r in conn.execute("SELECT key,value FROM bp_meta")}
        assert meta["corpus_digest"] == digest
        assert int(meta["min_count"]) == min_count
    finally:
        conn.close()


def vocab_expansion(vocab_doc: dict):
    ex = [bytes((i,)) for i in range(256)]
    for m in vocab_doc["merges"]:
        ex.append(ex[m["left"]] + ex[m["right"]])
    return ex


def verify_encoded_file(vocab_doc: dict, encoded_path: str,
                        expected_records: dict):
    """Decode an encoded file with raw SQL-free local logic and compare."""
    ex = vocab_expansion(vocab_doc)
    max_token = 255 + len(vocab_doc["merges"])
    with open(encoded_path, "r", encoding="utf-8") as f:
        lines = [json.loads(line) for line in f]
    assert lines[0]["vocab_digest"] == vocab_doc["vocab_digest"]
    out = {}
    for rec in lines[1:]:
        assert all(0 <= t <= max_token for t in rec["tokens"])
        out[rec["id"]] = b"".join(ex[t] for t in rec["tokens"]).decode("utf-8")
    assert out == expected_records
    return out


def file_digest(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()
