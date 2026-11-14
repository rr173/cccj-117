"""Byte-pair vocabulary learning and encoding (independent BPE subsystem).

This module is deliberately independent of the seven-field observation
protocol, the mail channel, change feed and nested preparation.  It learns a
*text preprocessing vocabulary* only: it never trains a language model and
never derives research conclusions.  It only processes corpus files handed to
it by the caller; it never reads historical observations or withdrawn
material.

A training database is its own SQLite file (never vault.sqlite3) plus an
flock file next to it.  Everything needed to resume training lives on disk:

  bp_meta          training identity (input byte digest, min_count, limits),
                   next token id, round counter, round state machine
  bp_docs          every corpus line: id, line number, weight, raw UTF-8
                   bytes and the *current* token sequence (packed BLOB)
  bp_pairs         current weighted adjacent-pair counts (exact integers)
  bp_pair_hits     which docs currently contain which pair (hit relation)
  bp_vocab         learned tokens 256..: the two old tokens merged and round
  bp_rounds        per-round queryable statistics
  bp_round_*       staging area of the open (unconfirmed) round; a crash
                   leaves partial rewrites here and the round is redone

Determinism contract (identical inputs always produce the identical vocab):
  * text is encoded to UTF-8 bytes verbatim - no case folding, Unicode
    normalization or whitespace cleaning;
  * tokens 0..255 are the raw bytes; new tokens are numbered 256, 257, ...;
  * each round picks the adjacent pair with the largest weighted count,
    ties broken by (left_id, right_id) ascending;
  * overlapping positions count, but replacement is left-to-right and
    non-overlapping inside each doc, never across two docs;
  * all arithmetic is exact integer arithmetic; supported ranges are
    enforced while reading the whole file, before anything is written.

Memory contract: at most one corpus line and its token sequence is held in
Python at a time; all scale-bearing state (docs, pairs, hits, round plan)
stays in SQLite.  The round plan is materialized with INSERT ... SELECT and
walked with a cursor - never loaded into a Python list.
"""
from __future__ import annotations

import fcntl
import hashlib
import json
import os
import sqlite3
import time
from array import array
from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timezone

from .syncpoints import sync_point

# ---------------------------------------------------------------------------
# Exact-range limits. Every one of them is enforced during the full-file read
# pass (before any database write), so later merges can neither overflow nor
# fall back to floating point.  SQLite stores signed 64-bit integers.
# ---------------------------------------------------------------------------
MAX_WEIGHT = 2 ** 31 - 1
MAX_DOC_BYTES = 2 ** 31 - 1          # one text encoded as UTF-8
MAX_TOTAL_WEIGHT = 2 ** 31 - 1
MAX_WEIGHTED_POSITIONS = 2 ** 63 - 1  # upper bound on every pair count
MAX_MERGES = 2 ** 31 - 1             # keeps every token id inside uint32
MAX_MIN_COUNT = 2 ** 31 - 1
MAX_LINE_BYTES = MAX_DOC_BYTES + 4096

VOCAB_FORMAT = "rvault-bpe-vocab"
ENCODED_FORMAT = "rvault-bpe-encoded"
FORMAT_VERSION = 1
BASE_TOKENS = 256


class BpeError(Exception):
    """A rejected request with a stable error code and process exit code."""

    def __init__(self, code: str, message: str, exit_code: int = 2,
                 details=None):
        super().__init__(message)
        self.code = code
        self.exit_code = exit_code
        self.details = details or {}

    def as_dict(self) -> dict:
        d = {"code": self.code, "error": str(self)}
        d.update(self.details)
        return d


# ---------------------------------------------------------------------------
# Streaming line reader with an explicit per-line size cap.
# ---------------------------------------------------------------------------
def _iter_lines(path: str):
    """Yield (line_no, raw_line_without_newline) from a binary file.

    A trailing LF terminates a line; an LF-less final line is still a line.
    A CR immediately before LF is kept inside the line payload and therefore
    rejected by the strict JSON parser.
    """
    line_no = 0
    with open(path, "rb") as f:
        buf = b""
        while True:
            chunk = f.read(1 << 20)
            if not chunk:
                if buf:
                    line_no += 1
                    yield line_no, buf
                return
            buf += chunk
            while True:
                nl = buf.find(b"\n")
                if nl < 0:
                    break
                line_no += 1
                line = buf[:nl]
                buf = buf[nl + 1:]
                if len(line) > MAX_LINE_BYTES:
                    raise BpeError(
                        "line_too_long",
                        "line %d exceeds %d bytes" % (line_no, MAX_LINE_BYTES))
                yield line_no, line
            if len(buf) > MAX_LINE_BYTES:
                raise BpeError(
                    "line_too_long",
                    "line %d exceeds %d bytes" % (line_no + 1, MAX_LINE_BYTES))


def _reject_constant(value):
    raise ValueError("NaN/Infinity are not accepted")


def _reject_duplicate_pairs(pairs):
    keys = set()
    for k, _ in pairs:
        if k in keys:
            raise ValueError("duplicate JSON key: %r" % k)
        keys.add(k)
    return dict(pairs)


def _parse_json_line(raw: bytes, line_no: int) -> dict:
    try:
        obj = json.loads(raw, parse_constant=_reject_constant,
                         object_pairs_hook=_reject_duplicate_pairs)
    except (ValueError, RecursionError) as e:
        raise BpeError("invalid_json", "line %d is not valid JSON: %s"
                                       % (line_no, e))
    if not isinstance(obj, dict):
        raise BpeError("line_not_object",
                       "line %d must be a JSON object" % line_no)
    return obj


def _check_id(obj: dict, line_no: int) -> str:
    if "id" not in obj:
        raise BpeError("missing_field", "line %d: missing 'id'" % line_no)
    value = obj["id"]
    if not isinstance(value, str) or value == "":
        raise BpeError("invalid_id",
                       "line %d: 'id' must be a non-empty string" % line_no)
    return value


def _check_text(obj: dict, line_no: int) -> str:
    if "text" not in obj:
        raise BpeError("missing_field", "line %d: missing 'text'" % line_no)
    value = obj["text"]
    if not isinstance(value, str):
        raise BpeError("invalid_text",
                       "line %d: 'text' must be a string" % line_no)
    return value


def _check_weight(obj: dict, line_no: int) -> int:
    if "weight" not in obj:
        raise BpeError("missing_field",
                       "line %d: missing 'weight'" % line_no)
    value = obj["weight"]
    # bool is a subclass of int: an explicit boolean is never a weight.
    if isinstance(value, bool) or not isinstance(value, int):
        raise BpeError("invalid_weight",
                       "line %d: 'weight' must be a positive integer (not "
                       "boolean or float)" % line_no)
    if value <= 0 or value > MAX_WEIGHT:
        raise BpeError("weight_out_of_range",
                       "line %d: 'weight' must be in 1..%d"
                       % (line_no, MAX_WEIGHT))
    return value


def parse_corpus_line(raw: bytes, line_no: int) -> tuple:
    """Validate one training line: exactly id / weight / text."""
    obj = _parse_json_line(raw, line_no)
    if set(obj.keys()) != {"id", "weight", "text"}:
        raise BpeError(
            "bad_fields",
            "line %d: each line must have exactly id, weight, text" % line_no)
    doc_id = _check_id(obj, line_no)
    weight = _check_weight(obj, line_no)
    text = _check_text(obj, line_no)
    data = text.encode("utf-8")
    if len(data) > MAX_DOC_BYTES:
        raise BpeError("text_too_long",
                       "line %d: UTF-8 text exceeds %d bytes"
                       % (line_no, MAX_DOC_BYTES))
    return doc_id, weight, text, data


def parse_encode_line(raw: bytes, line_no: int) -> tuple:
    """Validate one encoding line: exactly id / text."""
    obj = _parse_json_line(raw, line_no)
    if set(obj.keys()) != {"id", "text"}:
        raise BpeError(
            "bad_fields",
            "line %d: each line must have exactly id, text" % line_no)
    return _check_id(obj, line_no), _check_text(obj, line_no)


# ---------------------------------------------------------------------------
# Whole-file validation pass. Nothing is written until this succeeds.
# ---------------------------------------------------------------------------
def validate_file(path: str, *, with_weight: bool) -> dict:
    """Stream the whole file once, returning identity metadata.

    Raises BpeError with every problem found (line numbers included) when the
    file is unusable.  Also enforces the exact-range limits here, so the
    later training loop never has to deal with overflow.
    """
    if not os.path.isfile(path):
        raise BpeError("unknown_input", "input file not found: " + path, 4)
    errors = []
    seen_ids = {}
    digest = hashlib.sha256()
    size = 0
    line_count = 0
    total_weight = 0
    weighted_positions = 0
    try:
        for line_no, raw in _iter_lines(path):
            digest.update(raw)
            digest.update(b"\n")
            size += len(raw) + 1
            line_count += 1
            try:
                if with_weight:
                    doc_id, weight, _text, data = parse_corpus_line(raw, line_no)
                else:
                    doc_id, text = parse_encode_line(raw, line_no)
                    weight = 1
                    data = text.encode("utf-8")
            except BpeError as e:
                errors.append({"line": line_no, "code": e.code,
                               "error": str(e)})
                continue
            if doc_id in seen_ids:
                errors.append({"line": line_no, "code": "duplicate_id",
                               "error": "duplicate id %r; first seen on line %d"
                                        % (doc_id, seen_ids[doc_id])})
                continue
            seen_ids[doc_id] = line_no
            total_weight += weight
            if len(data) >= 1:
                weighted_positions += weight * (len(data) - 1)
            if total_weight > MAX_TOTAL_WEIGHT:
                raise BpeError(
                    "total_weight_out_of_range",
                    "sum of weights exceeds %d (detected at line %d)"
                    % (MAX_TOTAL_WEIGHT, line_no))
            if weighted_positions > MAX_WEIGHTED_POSITIONS:
                raise BpeError(
                    "positions_out_of_range",
                    "weighted adjacent-position total exceeds %d "
                    "(detected at line %d)"
                    % (MAX_WEIGHTED_POSITIONS, line_no))
    except BpeError:
        raise
    if line_count == 0:
        errors.append({"line": 0, "code": "empty_file",
                       "error": "input file has no lines"})
    if errors:
        raise BpeError("invalid_corpus",
                       "input rejected with %d problem(s)" % len(errors),
                       details={"errors": errors})
    return {"corpus_digest": digest.hexdigest(), "corpus_size": size,
            "line_count": line_count, "total_weight": total_weight,
            "weighted_positions": weighted_positions}


# ---------------------------------------------------------------------------
# Token sequence packing (unsigned 32-bit little-endian; one doc in memory).
# ---------------------------------------------------------------------------
def pack_tokens(seq) -> bytes:
    return array("I", seq).tobytes()


def unpack_tokens(blob: bytes):
    arr = array("I")
    arr.frombytes(blob)
    return arr.tolist()


def adjacent_pairs(seq) -> Counter:
    """Weight-1 pair multigest for one sequence; overlapping positions count."""
    pairs = Counter()
    for i in range(len(seq) - 1):
        pairs[(seq[i], seq[i + 1])] += 1
    return pairs


def replace_pair(seq, left_id: int, right_id: int):
    """Left-to-right non-overlapping replacement; returns (new_seq, n_hits)."""
    out = []
    i = 0
    n = 0
    n_seq = len(seq)
    while i < n_seq:
        if i + 1 < n_seq and seq[i] == left_id and seq[i + 1] == right_id:
            out.append(None)  # placeholder, caller fills the new token id
            n += 1
            i += 2
        else:
            out.append(seq[i])
            i += 1
    return out, n


# ---------------------------------------------------------------------------
# Schema and connection handling.
# ---------------------------------------------------------------------------
SCHEMA = """
CREATE TABLE bp_meta(
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
CREATE TABLE bp_docs(
  doc_id TEXT PRIMARY KEY,
  line_no INTEGER NOT NULL,
  weight INTEGER NOT NULL,
  raw BLOB NOT NULL,
  tokens BLOB NOT NULL,
  seq_len INTEGER NOT NULL
);
CREATE TABLE bp_pairs(
  left_id INTEGER NOT NULL,
  right_id INTEGER NOT NULL,
  count INTEGER NOT NULL,
  PRIMARY KEY(left_id, right_id)
) WITHOUT ROWID;
CREATE TABLE bp_pair_hits(
  left_id INTEGER NOT NULL,
  right_id INTEGER NOT NULL,
  doc_id TEXT NOT NULL,
  PRIMARY KEY(left_id, right_id, doc_id)
) WITHOUT ROWID;
CREATE INDEX bp_pair_hits_doc ON bp_pair_hits(doc_id);
CREATE TABLE bp_vocab(
  token_id INTEGER PRIMARY KEY,
  left_id INTEGER NOT NULL,
  right_id INTEGER NOT NULL,
  round INTEGER NOT NULL
);
CREATE TABLE bp_rounds(
  round INTEGER PRIMARY KEY,
  token_id INTEGER NOT NULL,
  left_id INTEGER NOT NULL,
  right_id INTEGER NOT NULL,
  weighted_count INTEGER NOT NULL,
  docs_hit INTEGER NOT NULL,
  docs_rewritten INTEGER NOT NULL,
  positions_replaced INTEGER NOT NULL
);
-- Staging area of the currently open round. Regular tables on purpose:
-- a killed process leaves its partial rewrites visible on disk; recovery
-- discards them and redoes the whole round (no token id is consumed until
-- the confirm transaction).
CREATE TABLE bp_round_state(
  round INTEGER PRIMARY KEY,
  left_id INTEGER NOT NULL,
  right_id INTEGER NOT NULL,
  token_id INTEGER NOT NULL
);
CREATE TABLE bp_round_plan(
  doc_id TEXT PRIMARY KEY
);
CREATE TABLE bp_round_docs(
  doc_id TEXT PRIMARY KEY,
  old_tokens BLOB NOT NULL,
  new_tokens BLOB NOT NULL,
  new_len INTEGER NOT NULL,
  replacements INTEGER NOT NULL
);
CREATE TABLE bp_round_delta(
  doc_id TEXT NOT NULL,
  left_id INTEGER NOT NULL,
  right_id INTEGER NOT NULL,
  delta INTEGER NOT NULL
);
CREATE INDEX bp_round_delta_pair
  ON bp_round_delta(left_id, right_id);
CREATE TABLE bp_round_new_hits(
  doc_id TEXT NOT NULL,
  left_id INTEGER NOT NULL,
  right_id INTEGER NOT NULL,
  PRIMARY KEY(doc_id, left_id, right_id)
) WITHOUT ROWID;
"""


def connect(database: str) -> sqlite3.Connection:
    conn = sqlite3.connect(database, timeout=30.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA secure_delete=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def is_bpe_database(conn: sqlite3.Connection) -> bool:
    try:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='bp_meta'"
        ).fetchone()
    except sqlite3.DatabaseError as e:
        raise BpeError("database_unreadable",
                       "file is not a readable SQLite BPE database: %s" % e,
                       5)
    return row is not None


def default_lock_path(database: str) -> str:
    return os.path.abspath(database) + ".lock"


@contextmanager
def training_lock(lock_path: str, exclusive: bool, wait_seconds: float = 600.0):
    """flock(2) based occupancy; a killed holder releases it automatically."""
    os.makedirs(os.path.dirname(os.path.abspath(lock_path)) or ".",
                exist_ok=True)
    fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    flags = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
    deadline = time.time() + wait_seconds
    try:
        while True:
            try:
                fcntl.flock(fd, flags | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.time() >= deadline:
                    raise BpeError(
                        "lock_busy",
                        "another process is advancing this training database; "
                        "timed out after %s seconds" % wait_seconds)
                time.sleep(0.05)
        yield
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


# ---------------------------------------------------------------------------
# Meta helpers.
# ---------------------------------------------------------------------------
def _meta_get(conn: sqlite3.Connection, key: str):
    row = conn.execute("SELECT value FROM bp_meta WHERE key=?",
                       (key,)).fetchone()
    return None if row is None else row["value"]


def _meta_set(conn: sqlite3.Connection, key: str, value) -> None:
    conn.execute(
        "INSERT INTO bp_meta(key,value) VALUES(?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, str(value)))


def _meta_all(conn: sqlite3.Connection) -> dict:
    return {r["key"]: r["value"]
            for r in conn.execute("SELECT key,value FROM bp_meta")}


def _check_positive(name: str, value, limit: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) \
            or value <= 0 or value > limit:
        raise BpeError("invalid_parameter",
                       "%s must be a positive integer in 1..%d"
                       % (name, limit))
    return value


# ---------------------------------------------------------------------------
# Initial training: validate, then load everything in one transaction.
# ---------------------------------------------------------------------------
def train(database: str, input_path: str, max_merges: int,
          min_count: int, lock_path: str = None,
          wait_seconds: float = 600.0) -> dict:
    _check_positive("max_merges", max_merges, MAX_MERGES)
    _check_positive("min_count", min_count, MAX_MIN_COUNT)
    lock_path = lock_path or default_lock_path(database)
    parent = os.path.dirname(os.path.abspath(database))
    os.makedirs(parent, exist_ok=True)
    if os.path.exists(database):
        conn = connect(database)
        try:
            if is_bpe_database(conn):
                raise BpeError(
                    "database_initialized",
                    "training database already exists; use 'bpe resume' to "
                    "continue it (a different corpus or min_count is "
                    "rejected there)")
            raise BpeError("database_unreadable",
                           "path exists but is not a BPE training database")
        finally:
            conn.close()

    # Full-file validation first; a rejected file creates no database at all.
    info = validate_file(input_path, with_weight=True)

    with training_lock(lock_path, True, wait_seconds):
        conn = connect(database)
        try:
            conn.executescript(SCHEMA)
            _load_corpus(conn, input_path, info, min_count, max_merges)
            summary = _run_merges(conn, max_merges, min_count)
        finally:
            conn.close()
    return summary


def _load_corpus(conn: sqlite3.Connection, input_path: str, info: dict,
                 min_count: int, max_merges: int) -> None:
    conn.execute("CREATE TEMP TABLE tmp_pairs("
                 " left_id INTEGER, right_id INTEGER, occurrences INTEGER)")
    conn.execute("CREATE TEMP TABLE tmp_hits("
                 " left_id INTEGER, right_id INTEGER, doc_id TEXT)")
    conn.execute("BEGIN IMMEDIATE")
    try:
        now = datetime.now(timezone.utc).isoformat()
        for k, v in (
            ("state", "ready"),
            ("corpus_digest", info["corpus_digest"]),
            ("corpus_size", info["corpus_size"]),
            ("line_count", info["line_count"]),
            ("total_weight", info["total_weight"]),
            ("weighted_positions", info["weighted_positions"]),
            ("min_count", min_count),
            ("max_merges_requested", max_merges),
            ("next_token", BASE_TOKENS),
            ("rounds_done", 0),
            ("created_at", now),
            ("updated_at", now),
        ):
            _meta_set(conn, k, v)

        pair_batch = []
        hit_batch = []
        for line_no, raw in _iter_lines(input_path):
            doc_id, weight, _text, data = parse_corpus_line(raw, line_no)
            seq = list(data)
            blob = pack_tokens(seq)
            conn.execute(
                "INSERT INTO bp_docs(doc_id,line_no,weight,raw,tokens,seq_len)"
                " VALUES(?,?,?,?,?,?)",
                (doc_id, line_no, weight, data, blob, len(seq)))
            counts = adjacent_pairs(seq)
            for (left_id, right_id), occurrences in counts.items():
                pair_batch.append((left_id, right_id,
                                   weight * occurrences))
                hit_batch.append((left_id, right_id, doc_id))
            if len(pair_batch) >= 4096:
                conn.executemany(
                    "INSERT INTO tmp_pairs VALUES(?,?,?)", pair_batch)
                conn.executemany(
                    "INSERT OR IGNORE INTO tmp_hits VALUES(?,?,?)", hit_batch)
                pair_batch.clear()
                hit_batch.clear()
        if pair_batch:
            conn.executemany("INSERT INTO tmp_pairs VALUES(?,?,?)",
                             pair_batch)
            conn.executemany("INSERT OR IGNORE INTO tmp_hits VALUES(?,?,?)",
                             hit_batch)
        conn.execute(
            "INSERT INTO bp_pairs(left_id,right_id,count) "
            "SELECT left_id,right_id,SUM(occurrences) FROM tmp_pairs "
            "GROUP BY left_id,right_id")
        conn.execute(
            "INSERT INTO bp_pair_hits(left_id,right_id,doc_id) "
            "SELECT left_id,right_id,doc_id FROM tmp_hits")
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.execute("DROP TABLE IF EXISTS tmp_pairs")
        conn.execute("DROP TABLE IF EXISTS tmp_hits")


# ---------------------------------------------------------------------------
# Resume: identity checks, crash recovery, then the same merge loop.
# ---------------------------------------------------------------------------
def resume(database: str, input_path: str = None, max_merges: int = None,
           min_count: int = None, lock_path: str = None,
           wait_seconds: float = 600.0) -> dict:
    if not os.path.isfile(database):
        raise BpeError("unknown_database",
                       "training database not found: " + database, 4)
    lock_path = lock_path or default_lock_path(database)
    with training_lock(lock_path, True, wait_seconds):
        conn = connect(database)
        try:
            if not is_bpe_database(conn):
                raise BpeError("database_unreadable",
                               "not a BPE training database")
            stored_digest = _meta_get(conn, "corpus_digest")
            stored_min_count = int(_meta_get(conn, "min_count"))
            if min_count is not None:
                _check_positive("min_count", min_count, MAX_MIN_COUNT)
                if min_count != stored_min_count:
                    raise BpeError(
                        "parameter_mismatch",
                        "min_count=%d differs from the stored min_count=%d; "
                        "a different min_count cannot continue an old state"
                        % (min_count, stored_min_count))
            min_count = stored_min_count
            if input_path is not None:
                info = validate_file(input_path, with_weight=True)
                if info["corpus_digest"] != stored_digest:
                    raise BpeError(
                        "corpus_mismatch",
                        "input digest %s differs from the stored corpus %s; "
                        "a different corpus cannot continue an old state"
                        % (info["corpus_digest"], stored_digest))
            if max_merges is not None:
                _check_positive("max_merges", max_merges, MAX_MERGES)
            else:
                max_merges = int(_meta_get(conn, "max_merges_requested"))
            _discard_unfinished_round(conn)
            summary = _run_merges(conn, max_merges, min_count)
        finally:
            conn.close()
    return summary


def _discard_unfinished_round(conn: sqlite3.Connection) -> None:
    """Roll an interrupted round back to its last confirmed state.

    Main pair/hit/token/vocab tables were never touched by the open round;
    only staging rows exist, so dropping them makes the round fully undone.
    """
    state = _meta_get(conn, "state")
    if state != "merging":
        return
    conn.execute("BEGIN IMMEDIATE")
    try:
        for table in ("bp_round_state", "bp_round_plan", "bp_round_docs",
                      "bp_round_delta", "bp_round_new_hits"):
            conn.execute("DELETE FROM %s" % table)
        _meta_set(conn, "state", "ready")
        _meta_set(conn, "updated_at",
                  datetime.now(timezone.utc).isoformat())
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise


# ---------------------------------------------------------------------------
# The merge loop: local updates only.
# ---------------------------------------------------------------------------
def _best_pair(conn: sqlite3.Connection):
    return conn.execute(
        "SELECT left_id,right_id,count FROM bp_pairs "
        "ORDER BY count DESC, left_id ASC, right_id ASC LIMIT 1"
    ).fetchone()


def _run_merges(conn: sqlite3.Connection, max_merges: int,
                min_count: int) -> dict:
    rounds_done = int(_meta_get(conn, "rounds_done"))
    initial_rounds = rounds_done
    stop_reason = None
    while rounds_done < max_merges:
        best = _best_pair(conn)
        if best is None:
            stop_reason = "no_pairs_left"
            break
        if best["count"] < min_count:
            stop_reason = "below_min_count"
            break
        rounds_done = _do_one_round(conn, best, rounds_done, min_count)
    if stop_reason is None and rounds_done >= max_merges:
        stop_reason = "max_merges_reached"
    conn.execute("BEGIN IMMEDIATE")
    try:
        _meta_set(conn, "state", "done" if stop_reason != "max_merges_reached"
                                else "ready")
        _meta_set(conn, "max_merges_requested", max_merges)
        _meta_set(conn, "updated_at",
                  datetime.now(timezone.utc).isoformat())
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    return {"status": "ok", "rounds_run_now": rounds_done - initial_rounds,
            "rounds_done": rounds_done,
            "next_token": int(_meta_get(conn, "next_token")),
            "stop_reason": stop_reason, "min_count": min_count,
            "max_merges": max_merges}


def _do_one_round(conn: sqlite3.Connection, best, rounds_done: int,
                  min_count: int) -> int:
    round_no = rounds_done + 1
    left_id, right_id, weighted_count = (
        best["left_id"], best["right_id"], best["count"])
    next_token = int(_meta_get(conn, "next_token"))

    # --- stage 1: publish the round plan straight from the hit relation. ---
    # No Python list of candidate doc ids is ever built.
    conn.execute("BEGIN IMMEDIATE")
    try:
        _meta_set(conn, "state", "merging")
        conn.execute("INSERT INTO bp_round_state VALUES(?,?,?,?)",
                     (round_no, left_id, right_id, next_token))
        conn.execute(
            "INSERT INTO bp_round_plan(doc_id) "
            "SELECT doc_id FROM bp_pair_hits WHERE left_id=? AND right_id=? "
            "ORDER BY doc_id", (left_id, right_id))
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise

    # --- stage 2: re-read only hit docs, one at a time; commit progress. ---
    docs_hit = 0
    positions = 0
    plan_cursor = conn.execute(
        "SELECT doc_id FROM bp_round_plan ORDER BY doc_id")
    for plan_row in plan_cursor:
        doc_id = plan_row["doc_id"]
        doc = conn.execute(
            "SELECT weight,tokens FROM bp_docs WHERE doc_id=?",
            (doc_id,)).fetchone()
        old_seq = unpack_tokens(doc["tokens"])
        weight = doc["weight"]
        old_pairs = adjacent_pairs(old_seq)
        skeleton, replacements = replace_pair(old_seq, left_id, right_id)
        new_seq = [next_token if t is None else t for t in skeleton]
        new_pairs = adjacent_pairs(new_seq)
        docs_hit += 1
        positions += replacements

        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute(
                "INSERT INTO bp_round_docs VALUES(?,?,?,?,?)",
                (doc_id, pack_tokens(old_seq), pack_tokens(new_seq),
                 len(new_seq), replacements))
            keys = set(old_pairs) | set(new_pairs.keys())
            conn.executemany(
                "INSERT INTO bp_round_delta VALUES(?,?,?,?)",
                ((doc_id, l, r,
                  weight * (new_pairs.get((l, r), 0)
                            - old_pairs.get((l, r), 0)))
                 for (l, r) in keys))
            conn.executemany(
                "INSERT OR IGNORE INTO bp_round_new_hits VALUES(?,?,?)",
                ((doc_id, l, r) for (l, r) in new_pairs))
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        # Deterministic stop point: this doc's rewrite is durable, the round
        # itself is not confirmed yet.
        sync_point("bpe.round.after_doc")

    sync_point("bpe.round.before_confirm")

    # --- stage 3: confirm the whole round in one transaction. -------------
    conn.execute("BEGIN IMMEDIATE")
    try:
        # Pair counts: subtract old contributions, add new ones, set-based.
        conn.execute(
            "INSERT INTO bp_pairs(left_id,right_id,count) "
            "SELECT left_id,right_id,SUM(delta) FROM bp_round_delta "
            "GROUP BY left_id,right_id "
            "ON CONFLICT(left_id,right_id) DO UPDATE SET "
            "count=bp_pairs.count+excluded.count")
        conn.execute("DELETE FROM bp_pairs WHERE count<=0")

        # Hit relation: replace the hit rows of every rewritten doc.
        conn.execute(
            "DELETE FROM bp_pair_hits WHERE doc_id IN "
            "(SELECT doc_id FROM bp_round_plan)")
        conn.execute(
            "INSERT INTO bp_pair_hits(left_id,right_id,doc_id) "
            "SELECT left_id,right_id,doc_id FROM bp_round_new_hits")

        # Current token sequences.
        conn.execute(
            "UPDATE bp_docs SET tokens="
            "(SELECT new_tokens FROM bp_round_docs d "
            " WHERE d.doc_id=bp_docs.doc_id),"
            "seq_len=(SELECT new_len FROM bp_round_docs d "
            " WHERE d.doc_id=bp_docs.doc_id) "
            "WHERE doc_id IN (SELECT doc_id FROM bp_round_plan)")

        conn.execute("INSERT INTO bp_vocab VALUES(?,?,?,?)",
                     (next_token, left_id, right_id, round_no))
        conn.execute(
            "INSERT INTO bp_rounds VALUES(?,?,?,?,?,?,?,?)",
            (round_no, next_token, left_id, right_id, weighted_count,
             docs_hit, docs_hit, positions))
        for table in ("bp_round_state", "bp_round_plan", "bp_round_docs",
                      "bp_round_delta", "bp_round_new_hits"):
            conn.execute("DELETE FROM %s" % table)
        _meta_set(conn, "next_token", next_token + 1)
        _meta_set(conn, "rounds_done", round_no)
        _meta_set(conn, "state", "ready")
        _meta_set(conn, "updated_at",
                  datetime.now(timezone.utc).isoformat())
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    return round_no


# ---------------------------------------------------------------------------
# Read-only inspection.
# ---------------------------------------------------------------------------
def status(database: str, lock_path: str = None) -> dict:
    if not os.path.isfile(database):
        raise BpeError("unknown_database",
                       "training database not found: " + database, 4)
    # Read-only: no flock is taken. SQLite statements see only committed
    # state, and an unconfirmed round lives solely in the bp_round_* staging
    # tables while the maintained tables stay at the last confirmed round,
    # so a reader never observes a half-applied merge.
    conn = connect(database)
    try:
        if not is_bpe_database(conn):
            raise BpeError("database_unreadable",
                           "not a BPE training database")
        conn.execute("BEGIN")
        meta = _meta_all(conn)
        int_keys = ("corpus_size", "line_count", "total_weight",
                    "weighted_positions", "min_count",
                    "max_merges_requested", "next_token", "rounds_done")
        out = dict(meta)
        for k in int_keys:
            if k in out:
                out[k] = int(out[k])
        out["pair_kinds"] = conn.execute(
            "SELECT COUNT(*) c FROM bp_pairs").fetchone()["c"]
        out["doc_count"] = conn.execute(
            "SELECT COUNT(*) c FROM bp_docs").fetchone()["c"]
        out["staging_open"] = conn.execute(
            "SELECT COUNT(*) c FROM bp_round_state"
        ).fetchone()["c"]
        conn.execute("COMMIT")
        return out
    finally:
        conn.close()


def round_rows(database: str, lock_path: str = None) -> list:
    if not os.path.isfile(database):
        raise BpeError("unknown_database",
                       "training database not found: " + database, 4)
    conn = connect(database)
    try:
        if not is_bpe_database(conn):
            raise BpeError("database_unreadable",
                           "not a BPE training database")
        return [dict(r) for r in conn.execute(
            "SELECT * FROM bp_rounds ORDER BY round")]
    finally:
        conn.close()


def vocab_document(database: str, lock_path: str = None) -> dict:
    """Build the self-describing, digest-bound vocabulary document."""
    if not os.path.isfile(database):
        raise BpeError("unknown_database",
                       "training database not found: " + database, 4)
    conn = connect(database)
    try:
        if not is_bpe_database(conn):
            raise BpeError("database_unreadable",
                           "not a BPE training database")
        if _meta_get(conn, "state") == "merging":
            raise BpeError(
                "round_unconfirmed",
                "a merge round is unconfirmed (crash recovery pending); "
                "run 'bpe resume' before exporting the vocabulary")
        merges = [{"token": r["token_id"], "left": r["left_id"],
                   "right": r["right_id"], "round": r["round"]}
                  for r in conn.execute(
                      "SELECT token_id,left_id,right_id,round "
                      "FROM bp_vocab ORDER BY round")]
        payload = {
            "format": VOCAB_FORMAT,
            "version": FORMAT_VERSION,
            "base_token_count": BASE_TOKENS,
            "corpus_digest": _meta_get(conn, "corpus_digest"),
            "min_count": int(_meta_get(conn, "min_count")),
            "merges": merges,
        }
        payload["vocab_digest"] = _canonical_digest(payload)
        return payload
    finally:
        conn.close()


def _canonical_digest(payload: dict) -> str:
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def load_vocab_file(path: str) -> dict:
    if not os.path.isfile(path):
        raise BpeError("unknown_vocab", "vocabulary file not found: " + path, 4)
    try:
        with open(path, "r", encoding="utf-8") as f:
            doc = json.load(f)
    except (ValueError, OSError) as e:
        raise BpeError("invalid_vocab",
                       "vocabulary file is not valid JSON: %s" % e, 5)
    if not isinstance(doc, dict) or doc.get("format") != VOCAB_FORMAT \
            or doc.get("version") != FORMAT_VERSION \
            or not isinstance(doc.get("merges"), list) \
            or "vocab_digest" not in doc:
        raise BpeError("invalid_vocab",
                       "vocabulary file has an unknown or malformed format", 5)
    claimed = doc["vocab_digest"]
    payload = {k: v for k, v in doc.items() if k != "vocab_digest"}
    if _canonical_digest(payload) != claimed:
        raise BpeError("vocab_tampered",
                       "vocabulary digest mismatch; the file was modified "
                       "after export", 5)
    merges = doc["merges"]
    expected = BASE_TOKENS
    for i, m in enumerate(merges):
        if not isinstance(m, dict) or set(m.keys()) != \
                {"token", "left", "right", "round"}:
            raise BpeError("invalid_vocab", "malformed merge entry", 5)
        if any(isinstance(m[k], bool) or not isinstance(m[k], int)
               for k in ("token", "left", "right", "round")):
            raise BpeError("invalid_vocab",
                           "merge entries must use integers", 5)
        if m["token"] != expected + i or m["round"] != i + 1:
            raise BpeError("invalid_vocab",
                           "merge tokens/rounds must be consecutive", 5)
        if not (0 <= m["left"] < m["token"]) or not (0 <= m["right"]
                                                       < m["token"]):
            raise BpeError("invalid_vocab",
                           "merge parts must be earlier token ids", 5)
    doc["_max_token"] = expected + len(merges) - 1 if merges \
        else BASE_TOKENS - 1
    return doc


# ---------------------------------------------------------------------------
# Streaming encode / decode.
# ---------------------------------------------------------------------------
def _merge_order(vocab: dict):
    return [(m["left"], m["right"], m["token"]) for m in vocab["merges"]]


def encode_bytes(data: bytes, merges) -> list:
    """Apply learned merges in order; same non-overlapping rule as training."""
    seq = list(data)
    for left_id, right_id, token_id in merges:
        if not seq:
            break
        out = []
        i = 0
        n = len(seq)
        while i < n:
            if i + 1 < n and seq[i] == left_id and seq[i + 1] == right_id:
                out.append(token_id)
                i += 2
            else:
                out.append(seq[i])
                i += 1
        seq = out
    return seq


def _atomic_output(path: str):
    tmp = "%s.tmp.%d" % (os.path.abspath(path), os.getpid())
    return tmp


def encode_file(vocab_path: str, input_path: str, output_path: str) -> dict:
    vocab = load_vocab_file(vocab_path)
    info = validate_file(input_path, with_weight=False)
    merges = _merge_order(vocab)
    out_tmp = _atomic_output(output_path)
    records = 0
    try:
        with open(out_tmp, "w", encoding="utf-8", newline="\n") as out:
            out.write(json.dumps(
                {"format": ENCODED_FORMAT, "version": FORMAT_VERSION,
                 "vocab_digest": vocab["vocab_digest"]},
                ensure_ascii=False, separators=(",", ":")) + "\n")
            for line_no, raw in _iter_lines(input_path):
                doc_id, text = parse_encode_line(raw, line_no)
                tokens = encode_bytes(text.encode("utf-8"), merges)
                out.write(json.dumps({"id": doc_id, "tokens": tokens},
                                     ensure_ascii=False,
                                     separators=(",", ":")) + "\n")
                records += 1
        os.replace(out_tmp, output_path)
    except BaseException:
        try:
            os.remove(out_tmp)
        except FileNotFoundError:
            pass
        raise
    return {"status": "encoded", "records": records,
            "vocab_digest": vocab["vocab_digest"],
            "input_digest": info["corpus_digest"],
            "output": os.path.abspath(output_path)}


def _token_expansions(vocab: dict):
    """Exact byte expansion of every token; base tokens are single bytes."""
    expansions = [bytes((i,)) for i in range(BASE_TOKENS)]
    for m in vocab["merges"]:
        expansions.append(expansions[m["left"]] + expansions[m["right"]])
    return expansions


def decode_file(vocab_path: str, input_path: str, output_path: str) -> dict:
    vocab = load_vocab_file(vocab_path)
    if not os.path.isfile(input_path):
        raise BpeError("unknown_input",
                       "encoded file not found: " + input_path, 4)
    expansions = _token_expansions(vocab)
    max_token = vocab["_max_token"]
    out_tmp = _atomic_output(output_path)
    records = 0
    header_seen = False
    try:
        with open(out_tmp, "w", encoding="utf-8", newline="\n") as out:
            for line_no, raw in _iter_lines(input_path):
                obj = _parse_json_line(raw, line_no)
                if not header_seen:
                    if obj.get("format") != ENCODED_FORMAT \
                            or obj.get("version") != FORMAT_VERSION \
                            or not isinstance(obj.get("vocab_digest"), str):
                        raise BpeError(
                            "invalid_encoded_file",
                            "line %d: missing or malformed encoded-file "
                            "header" % line_no, 5)
                    if obj["vocab_digest"] != vocab["vocab_digest"]:
                        raise BpeError(
                            "vocab_mismatch",
                            "the encoded file is bound to vocabulary %s but "
                            "the given vocabulary is %s; refusing to decode "
                            "into a different text"
                            % (obj["vocab_digest"], vocab["vocab_digest"]), 5)
                    header_seen = True
                    continue
                if set(obj.keys()) != {"id", "tokens"}:
                    raise BpeError(
                        "invalid_encoded_file",
                        "line %d: records must have exactly id and tokens"
                        % line_no, 5)
                doc_id = _check_id(obj, line_no)
                tokens = obj["tokens"]
                if not isinstance(tokens, list) or any(
                        isinstance(t, bool) or not isinstance(t, int)
                        for t in tokens):
                    raise BpeError(
                        "invalid_token",
                        "line %d: tokens must be a list of integers"
                        % line_no, 5)
                if any(t < 0 or t > max_token for t in tokens):
                    raise BpeError(
                        "unknown_token",
                        "line %d (id=%r): token id outside this vocabulary"
                        % (line_no, doc_id), 5)
                data = b"".join(expansions[t] for t in tokens)
                try:
            # Exact UTF-8 round trip; invalid byte sequences are refused.
                    text = data.decode("utf-8")
                except UnicodeDecodeError as e:
                    raise BpeError(
                        "invalid_utf8",
                        "line %d (id=%r): decoded bytes are not valid UTF-8: "
                        "%s" % (line_no, doc_id, e), 5)
                out.write(json.dumps({"id": doc_id, "text": text},
                                     ensure_ascii=False,
                                     separators=(",", ":")) + "\n")
                records += 1
        if not header_seen:
            raise BpeError("invalid_encoded_file",
                           "encoded file contains no header", 5)
        os.replace(out_tmp, output_path)
    except BaseException:
        try:
            os.remove(out_tmp)
        except FileNotFoundError:
            pass
        raise
    return {"status": "decoded", "records": records,
            "vocab_digest": vocab["vocab_digest"],
            "output": os.path.abspath(output_path)}
