"""Independent slow reference implementation of the BPE training rules.

This is deliberately a separate, naive algorithm: every round it fully
re-counts every adjacent pair across the whole corpus and rescans every
sequence for replacement. It must NOT import or call anything from
rvault.bpe's counting or replacement code - it exists precisely to cross-check
the disk-backed incremental implementation against a second, readable
construction of the same rules.

Rules reproduced here (from the task contract):
  * tokens 0..255 are the UTF-8 bytes of text (verbatim, no normalization);
  * each doc carries a positive integer weight;
  * pair counts include overlapping positions and are summed per weight;
  * the winning pair is (highest count, then lowest left id, then lowest
    right id);
  * replacement inside one doc is left-to-right, non-overlapping, never
    crosses doc boundaries;
  * stop at max_merges or when the best count is below min_count.

It returns the whole merge sequence plus every doc's final token sequence, so
a caller can compare the incremental engine's on-disk result exactly.
"""
from __future__ import annotations

import json


def read_corpus(path: str):
    docs = []
    with open(path, "rb") as f:
        for raw in f:
            line = raw.rstrip(b"\n")
            obj = json.loads(line)
            docs.append((obj["id"], int(obj["weight"]),
                         list(obj["text"].encode("utf-8"))))
    return docs


def count_all_pairs(docs):
    """Full O(total tokens) recount with overlapping positions."""
    counts = {}
    for _doc_id, weight, seq in docs:
        for i in range(len(seq) - 1):
            pair = (seq[i], seq[i + 1])
            counts[pair] = counts.get(pair, 0) + weight
    return counts


def slow_replace(seq, pair):
    """Left-to-right non-overlapping replacement (independent implementation)."""
    left, right = pair
    out = []
    i = 0
    while i < len(seq):
        if i + 1 < len(seq) and seq[i] == left and seq[i + 1] == right:
            out.append(None)  # caller assigns the new token id
            i += 2
        else:
            out.append(seq[i])
            i += 1
    return out


def slow_train(docs, max_merges, min_count):
    """Return (merge_sequence, final_docs, round_records).

    merge_sequence: [(round, token_id, left, right, weighted_count), ...]
    final_docs:     [(doc_id, seq), ...] in input order
    round_records:  per-round dicts with counts and docs rewritten
    """
    # Work on copies; keep input docs untouched for repeat runs.
    work = [(doc_id, weight, list(seq)) for doc_id, weight, seq in docs]
    merge_sequence = []
    round_records = []
    next_token = 256
    for round_no in range(1, max_merges + 1):
        counts = count_all_pairs(work)
        if not counts:
            break
        # Deterministic selection: max count, then min pair ids.
        pair = min(counts, key=lambda p: (-counts[p], p[0], p[1]))
        weighted_count = counts[pair]
        if weighted_count < min_count:
            break
        docs_hit = 0
        positions = 0
        for idx, (doc_id, weight, seq) in enumerate(work):
            if pair not in ((seq[j], seq[j + 1])
                            for j in range(len(seq) - 1)):
                continue
            docs_hit += 1
            skeleton = slow_replace(seq, pair)
            new_seq = [next_token if t is None else t for t in skeleton]
            positions += sum(1 for t in skeleton if t is None)
            work[idx] = (doc_id, weight, new_seq)
        merge_sequence.append(
            (round_no, next_token, pair[0], pair[1], weighted_count))
        round_records.append({
            "round": round_no, "token_id": next_token,
            "left": pair[0], "right": pair[1],
            "weighted_count": weighted_count,
            "docs_hit": docs_hit, "docs_rewritten": docs_hit,
            "positions_replaced": positions})
        next_token += 1
    final_docs = [(doc_id, seq) for doc_id, _w, seq in work]
    return merge_sequence, final_docs, round_records


def slow_encode(text: str, merges):
    """Reference encoder: same ordered non-overlapping rule."""
    seq = list(text.encode("utf-8"))
    for left, right, token in merges:
        out = []
        i = 0
        while i < len(seq):
            if i + 1 < len(seq) and seq[i] == left and seq[i + 1] == right:
                out.append(token)
                i += 2
            else:
                out.append(seq[i])
                i += 1
        seq = out
    return seq
