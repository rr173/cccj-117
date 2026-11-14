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

Two input protocols
-------------------
Without contract selectors, import uses the ORIGINAL fixed-field protocol:
exactly the seven canonical fields, original validation, unchanged receipts
and notifications.  This is the behaviour every existing caller keeps.

With `--provider/--material-type/--contract-version`, every line is first
checked against exactly that published contract version (never silently the
latest one).  Unknown fields, missing required fields, type mismatches,
illegal nulls and contract violations reject the WHOLE batch.  Diagnostics
carry line number, field name and error category only - never display_name,
measure or memo values.  A legal line has its optional contract defaults
filled and is then handed to the SAME apply path as the legacy protocol, so
observation values and notification behaviour are identical.  The batch's
protocol and contract version are recorded in `ct_batches` in the same
transaction, so a batch credential can never silently switch protocol or
contract version on re-delivery.

Error messages and receipts carry identifiers only - never display_name,
measure or memo values.
"""
from __future__ import annotations

import hashlib
import json
import os

from . import changefeed
from . import contracts as contracts_mod
from .syncpoints import sync_point
from .store import reconcile
from .util import now_iso, safe_name, sha256_hex

FIELDS = ("study", "person", "observation", "edition", "display_name", "measure", "memo")
_MAX_ERRORS = 10000


def _validate(obj, numeric_measure: bool = False) -> str | None:
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
    if numeric_measure:
        # A contract may widen measure to `number`; the value is stored
        # exactly as delivered.  The original integer rule is unchanged for
        # the fixed-field protocol.
        if isinstance(m, bool) or not isinstance(m, (int, float)):
            return "field measure must be a number"
    elif isinstance(m, bool) or not isinstance(m, int):
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


def _process_line(conn, batch_tag: str, line_no: int, raw: bytes, errors: list,
                  contract=None, tracker=None):
    """Validate and apply one line inside the batch transaction.

    Returns (disposition, receipt_item) or None when the line raised an error.
    With `contract`, the line is checked against exactly that version first
    and the resulting canonical object goes through the very same apply path.
    `tracker` observes the (study, observation) of the SEMANTIC object, so
    change-feed behaviour is identical under both protocols.
    """
    try:
        obj = json.loads(raw.decode("utf-8"))
    except Exception:
        _add_error(errors, {"line": line_no, "reason": "invalid JSON"})
        return None
    if contract is None:
        reason = _validate(obj)
        if reason:
            _add_error(errors, {"line": line_no, "reason": reason})
            return None
        canonical = obj
    else:
        line_errors = []
        canonical = contracts_mod.validate_line(obj, contract, line_errors)
        if line_errors:
            for e in line_errors:
                entry = {"line": line_no}
                entry.update(e)
                _add_error(errors, entry)
            return None
        # Second gate: the contract already enforced types, non-empty
        # identifiers and nullability, so only generic rules are reachable
        # here, but no input path may bypass the original invariants.
        numeric = any(s["target"] == "measure" and s["type"] == "number"
                      for s in contract["fields"])
        reason = _validate(canonical, numeric_measure=numeric)
        if reason:
            _add_error(errors, {"line": line_no, "reason": reason})
            return None
    if tracker is not None:
        tracker.begin(canonical["study"], canonical["observation"])
    result = _apply_obj(conn, batch_tag, line_no, canonical, errors)
    if tracker is not None:
        tracker.end()
    return result


def _apply_obj(conn, batch_tag: str, line_no: int, obj: dict, errors: list):
    """Apply one already-validated canonical object (shared by both
    protocols: values are stored exactly as delivered)."""
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


def import_batch(store, batch_tag: str, src, contract_selection=None):
    """Spool `src` (binary file-like), then apply it atomically.

    `contract_selection` is None (legacy fixed-field protocol) or a tuple
    (provider, material_type, contract_version).  When given, the version is
    resolved up front: an unknown version rejects the batch before anything
    is spooled or written.  Returns (exit_code, summary_dict).
    """
    store.ensure()
    reconcile(store)
    contract = None
    protocol = "legacy"
    provider = material_type = version = None
    if contract_selection is not None:
        provider, material_type, version = contract_selection
        protocol = "contract"
        sync_point("contract.import.before_resolve")
        try:
            contract = contracts_mod.load(store, provider, material_type, version)
        except contracts_mod.ContractError as e:
            return e.exit_code, {"batch_tag": batch_tag, "status": "rejected",
                                 "protocol": protocol, "contract": {
                                     "provider": provider,
                                     "material_type": material_type,
                                     "version": version},
                                 "errors": [e.as_dict()]}
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
            prior = conn.execute("SELECT * FROM ct_batches WHERE batch_tag=?",
                                 (batch_tag,)).fetchone()
            prior_protocol = prior["protocol"] if prior is not None else "legacy"
            prior_contract = None
            if prior is not None and prior_protocol == "contract":
                prior_contract = {"provider": prior["provider"],
                                  "material_type": prior["material_type"],
                                  "version": prior["contract_version"]}
            requested = None
            if contract_selection is not None:
                requested = {"provider": provider, "material_type": material_type,
                             "version": version}
            if prior_protocol != protocol or prior_contract != requested:
                return 2, {"batch_tag": batch_tag, "status": "rejected",
                           "errors": [{
                               "reason": "batch_tag already used under a "
                                         "different input protocol or contract "
                                         "version",
                               "recorded_protocol": prior_protocol,
                               "recorded_contract": prior_contract,
                               "requested_protocol": protocol,
                               "requested_contract": requested}]}
            return 0, _duplicate_receipt(store, conn, batch_tag)

        errors = []
        counts = {"accepted": 0, "accepted_existing": 0, "withdrawn_suppressed": 0}
        receipt_tmp = os.path.join(store.tmp_dir, "receipt.%d.jsonl" % os.getpid())
        feed = changefeed.EventCollector()
        tracker = changefeed.LineTracker(conn, feed)
        conn.execute("BEGIN IMMEDIATE")
        try:
            with open(spool, "rb") as f, open(receipt_tmp, "w", encoding="utf-8") as rcpt:
                for line_no, raw in enumerate(f, 1):
                    # Feed intent is derived from the write itself (see
                    # LineTracker): only a newly stored row whose edition is
                    # the new maximum becomes a refresh.  Duplicate re-delivery
                    # and legal older editions insert no row, so no event.
                    # The tracker is scoped to the SEMANTIC (study,
                    # observation) of the line, so contract-mapped field names
                    # behave exactly like the legacy fixed fields.
                    result = _process_line(conn, batch_tag, line_no, raw, errors,
                                           contract=contract, tracker=tracker)
                    if result is not None:
                        disposition, item = result
                        counts[disposition] += 1
                        rcpt.write(json.dumps(item, ensure_ascii=False) + "\n")
            if errors:
                conn.execute("ROLLBACK")
            else:
                # Same transaction as the business rows: a rejected batch
                # never leaves a consumable event behind.
                frame_id = feed.commit(conn, "intake:" + batch_tag)
                conn.execute(
                    "INSERT INTO batches VALUES (?,?,?,?,?)",
                    (batch_tag, now_iso(), dg,
                     counts["accepted"] + counts["accepted_existing"],
                     counts["withdrawn_suppressed"]))
                conn.execute(
                    "INSERT INTO ct_batches VALUES (?,?,?,?,?)",
                    (batch_tag, provider, material_type, version, protocol))
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
            result = {"batch_tag": batch_tag, "status": "rejected",
                      "errors": errors}
            if contract_selection is not None:
                result["protocol"] = protocol
                result["contract"] = {"provider": provider,
                                      "material_type": material_type,
                                      "version": version}
            return 2, result
        path = _receipt_path(store, batch_tag)
        os.replace(receipt_tmp, path)
        result = {"batch_tag": batch_tag, "status": "accepted",
                  "accepted": counts["accepted"],
                  "accepted_existing": counts["accepted_existing"],
                  "withdrawn_suppressed": counts["withdrawn_suppressed"],
                  "feed_events": frame_id is not None,
                  "receipt": os.path.relpath(path, store.root)}
        if contract_selection is not None:
            result["protocol"] = protocol
            result["contract"] = {"provider": provider,
                                  "material_type": material_type,
                                  "version": version}
        return 0, result
    finally:
        conn.close()
        try:
            os.remove(spool)
        except FileNotFoundError:
            pass
