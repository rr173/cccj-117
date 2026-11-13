"""Streaming JSONL batch intake.

Properties enforced here:
  - a batch_tag is an atomic delivery credential: the whole batch commits or
    nothing does; corrupt lines are reported with their positions and never
    excused by other readable lines
  - input may exceed available memory: the stream is spooled to a scratch
    file, then applied line by line inside one SQLite transaction; receipts
    are streamed to a file as well
  - re-delivering the same batch_tag is idempotent: identical content yields
    a 'duplicate' receipt whose per-item dispositions reflect the CURRENT
    withdrawal registry (not the historical "accepted"); different content
    under a used batch_tag is rejected
  - a re-used edition number whose content differs from what was stored is
    rejected, even if that edition was superseded long ago
  - persons under a registered withdrawal barrier are suppressed (payload
    never reaches the database) while other participants in the same batch
    are accepted normally; the receipt reports the two groups separately

Error messages and receipts carry identifiers only - never display_name,
measure or memo values.
"""
from __future__ import annotations

import hashlib
import json
import os

from .store import reconcile
from .util import now_iso, safe_name, sha256_hex

FIELDS = ("study", "person", "observation", "edition", "display_name", "measure", "memo")
_MAX_ERRORS = 10000


def _validate(obj) -> str | None:
    if not isinstance(obj, dict):
        return "line is not a JSON object"
    keys = set(obj.keys())
    missing = [f for f in FIELDS if f not in keys]
    if missing:
        return "missing fields: " + ",".join(sorted(missing))
    extra = sorted(keys - set(FIELDS))
    if extra:
        return "unexpected fields: " + ",".join(extra)
    for f in ("study", "person", "observation"):
        if not isinstance(obj[f], str) or not obj[f]:
            return "field %s must be a non-empty string" % f
    if not isinstance(obj["display_name"], str):
        return "field display_name must be a string"
    if not isinstance(obj["memo"], str):
        return "field memo must be a string"
    ed = obj["edition"]
    if isinstance(ed, bool) or not isinstance(ed, int) or ed < 1:
        return "field edition must be a positive integer"
    m = obj["measure"]
    if isinstance(m, bool) or not isinstance(m, int):
        return "field measure must be an integer"
    return None


def _receipt_path(store, batch_tag: str) -> str:
    name = "%s-%s.jsonl" % (safe_name(batch_tag), sha256_hex(batch_tag.encode("utf-8"))[:8])
    return os.path.join(store.receipts_dir, name)


def _add_error(errors: list, entry: dict) -> None:
    if len(errors) < _MAX_ERRORS:
        errors.append(entry)
    elif len(errors) == _MAX_ERRORS:
        errors.append({"reason": "further errors truncated"})


def _process_line(conn, batch_tag: str, line_no: int, raw: bytes, errors: list):
    """Validate and apply one line inside the batch transaction.

    Returns (disposition, receipt_item) or None when the line raised an error.
    """
    try:
        obj = json.loads(raw.decode("utf-8"))
    except Exception:
        _add_error(errors, {"line": line_no, "reason": "invalid JSON"})
        return None
    reason = _validate(obj)
    if reason:
        _add_error(errors, {"line": line_no, "reason": reason})
        return None
    study, person, obs, ed = obj["study"], obj["person"], obj["observation"], obj["edition"]

    owner = conn.execute(
        "SELECT person FROM observation_owner WHERE study=? AND observation=?",
        (study, obs)).fetchone()
    if owner is None:
        # Record ownership even for suppressed lines: the identity tombstone
        # is what keeps a withdrawn person's observation ids from being
        # re-populated later (by anyone).
        conn.execute("INSERT INTO observation_owner VALUES (?,?,?)", (study, obs, person))
    elif owner["person"] != person:
        _add_error(errors, {"line": line_no, "reason": "ownership_conflict",
                            "study": study, "observation": obs, "person": person})
        return None

    if conn.execute("SELECT 1 FROM barrier WHERE study=? AND person=?",
                    (study, person)).fetchone():
        disposition = "withdrawn_suppressed"
    else:
        existing = conn.execute(
            "SELECT person, display_name, measure, memo FROM editions "
            "WHERE study=? AND observation=? AND edition=?",
            (study, obs, ed)).fetchone()
        if existing is None:
            conn.execute(
                "INSERT INTO editions VALUES (?,?,?,?,?,?,?,?)",
                (study, obs, ed, person, obj["display_name"], obj["measure"],
                 obj["memo"], batch_tag))
            disposition = "accepted"
        elif (existing["person"], existing["display_name"], existing["measure"],
              existing["memo"]) == (person, obj["display_name"], obj["measure"], obj["memo"]):
            disposition = "accepted_existing"
        else:
            # Same edition number, different payload: reject the whole batch,
            # however long ago this edition was superseded.
            _add_error(errors, {"line": line_no, "reason": "edition_conflict",
                                "study": study, "observation": obs, "edition": ed})
            return None

    conn.execute("INSERT INTO batch_items VALUES (?,?,?,?,?,?,?)",
                 (batch_tag, line_no, study, obs, person, ed, disposition))
    item = {"line": line_no, "study": study, "observation": obs,
            "person": person, "edition": ed, "disposition": disposition}
    return disposition, item


def _duplicate_receipt(store, conn, batch_tag: str):
    """Receipt for a re-delivered batch: no re-counting, dispositions are
    re-evaluated against the current withdrawal registry."""
    items = conn.execute(
        "SELECT * FROM batch_items WHERE batch_tag=? ORDER BY line_no", (batch_tag,)).fetchall()
    counts = {"accepted": 0, "accepted_existing": 0, "withdrawn_suppressed": 0}
    receipt_tmp = os.path.join(store.tmp_dir, "receipt.%d.jsonl" % os.getpid())
    with open(receipt_tmp, "w", encoding="utf-8") as rcpt:
        for it in items:
            disp = it["disposition"]
            if disp != "withdrawn_suppressed" and conn.execute(
                    "SELECT 1 FROM barrier WHERE study=? AND person=?",
                    (it["study"], it["person"])).fetchone():
                disp = "withdrawn_suppressed"
            counts[disp] += 1
            rcpt.write(json.dumps({"line": it["line_no"], "study": it["study"],
                                   "observation": it["observation"], "person": it["person"],
                                   "edition": it["edition"], "disposition": disp},
                                  ensure_ascii=False) + "\n")
    path = _receipt_path(store, batch_tag)
    os.replace(receipt_tmp, path)
    return {"batch_tag": batch_tag, "status": "duplicate",
            "note": "batch previously received; dispositions reflect the current "
                    "withdrawal registry",
            "accepted": counts["accepted"], "accepted_existing": counts["accepted_existing"],
            "withdrawn_suppressed": counts["withdrawn_suppressed"],
            "receipt": os.path.relpath(path, store.root)}


def import_batch(store, batch_tag: str, src):
    """Spool `src` (binary file-like), then apply it atomically.

    Returns (exit_code, summary_dict).
    """
    store.ensure()
    reconcile(store)
    digest = hashlib.sha256()
    spool = os.path.join(store.tmp_dir, "intake.%d.spool" % os.getpid())
    with open(spool, "wb") as out:
        while True:
            chunk = src.read(1 << 20)
            if not chunk:
                break
            digest.update(chunk)
            out.write(chunk)
    dg = digest.hexdigest()
    conn = store.connect()
    try:
        row = conn.execute("SELECT digest FROM batches WHERE batch_tag=?",
                           (batch_tag,)).fetchone()
        if row is not None:
            if row["digest"] != dg:
                return 2, {"batch_tag": batch_tag, "status": "rejected",
                           "errors": [{"reason": "batch_tag already used with different content"}]}
            return 0, _duplicate_receipt(store, conn, batch_tag)

        errors = []
        counts = {"accepted": 0, "accepted_existing": 0, "withdrawn_suppressed": 0}
        receipt_tmp = os.path.join(store.tmp_dir, "receipt.%d.jsonl" % os.getpid())
        conn.execute("BEGIN IMMEDIATE")
        try:
            with open(spool, "rb") as f, open(receipt_tmp, "w", encoding="utf-8") as rcpt:
                for line_no, raw in enumerate(f, 1):
                    result = _process_line(conn, batch_tag, line_no, raw, errors)
                    if result is not None:
                        disposition, item = result
                        counts[disposition] += 1
                        rcpt.write(json.dumps(item, ensure_ascii=False) + "\n")
            if errors:
                conn.execute("ROLLBACK")
            else:
                conn.execute(
                    "INSERT INTO batches VALUES (?,?,?,?,?)",
                    (batch_tag, now_iso(), dg,
                     counts["accepted"] + counts["accepted_existing"],
                     counts["withdrawn_suppressed"]))
                conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            raise
        if errors:
            try:
                os.remove(receipt_tmp)
            except FileNotFoundError:
                pass
            return 2, {"batch_tag": batch_tag, "status": "rejected", "errors": errors}
        path = _receipt_path(store, batch_tag)
        os.replace(receipt_tmp, path)
        return 0, {"batch_tag": batch_tag, "status": "accepted",
                   "accepted": counts["accepted"],
                   "accepted_existing": counts["accepted_existing"],
                   "withdrawn_suppressed": counts["withdrawn_suppressed"],
                   "receipt": os.path.relpath(path, store.root)}
    finally:
        conn.close()
        try:
            os.remove(spool)
        except FileNotFoundError:
            pass
