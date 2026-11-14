"""Versioned upstream input contracts, change review and contract-checked intake.

What this subsystem manages
---------------------------
What an upstream provider PROMISES to deliver: a named, versioned field shape
for one material type.  A contract version is
`(provider, material_type, version)` plus field definitions and an explicit
compatibility declaration.  It says nothing about withdrawing material,
notification indexes or processing lineage, and it never rewrites an incoming
line - it only validates it (and may fill optional defaults) before the
existing intake path stores it.

Immutability
------------
A published version can never be rewritten.  Re-sending the exact same
version (same provider, material type, version, predecessor, compatibility and
field definitions) is an idempotent `duplicate`; sending different content for
an already published version is rejected.  All checks and the insert share one
`BEGIN IMMEDIATE` transaction, so two processes racing for the same successor
produce exactly one published version and one explainable loser.

Version graph
-------------
The first version of a contract line must be version 1 and has no
predecessor.  Every later version must be exactly `max + 1` (no skipped
numbers) and must name an existing predecessor of the SAME (provider,
material_type).  The predecessor pointer graph is walked with a visited set;
a revisit (including a self-reference) is rejected as a cycle.  All of this is
checked before anything is inserted, and the insert is one transaction, so a
rejected registration leaves no half a version behind.

Compatibility
-------------
The caller must choose `compatible` (compatible upgrade) or `branch` (new
branch) for a non-initial version.  A compatible upgrade may only:
  * add an OPTIONAL field that carries a non-null default,
  * relax nullability (nullable false -> true),
  * widen the accepted type (integer -> number; measure may also be declared
    `number` outright, and then stored exactly as delivered).
It may NOT delete a field, tighten a constraint, change required status, add or
change a target (meaning), or narrow a type.  Those changes require `branch`,
which may carry incompatible changes but still must name an existing
predecessor and may not be passed off as a compatible upgrade.  The per-field
change list is computed against the named predecessor and stored for review;
`compare` recomputes it independently.

Field definitions
-----------------
name + type (string|integer|number|boolean) + required + nullable + optional
non-null default + optional canonical target.  Field names and the provider id
follow the non-empty-string rule.  A contract used for intake must map each of
the seven canonical intake fields exactly once; those target types are fixed
by the existing protocol, target fields may not be nullable and must be
required or carry a default (so a contract-valid line is always complete).

Intake
------
`import` with all three contract selectors checks every line against exactly
the chosen version - never silently the latest.  Unknown fields, missing
required fields, type mismatches, illegal nulls and (by construction) bad
defaults reject the WHOLE batch; diagnostics carry line number, field name and
error category but never display_name / measure / memo values.  Legal lines
get their optional defaults filled and are then handed to the existing intake
logic, so stored values obey the original rules.  A rejection or an
idempotent duplicate confirmation leaves no batch, edition revision, receipt
or notification behind.  Without the selectors, import keeps the original
fixed-field protocol byte for byte.

Registry tables live in db.SCHEMA (`ct_*`); Store.ensure() creates them on
every command, so existing stores need no migration step.
"""
from __future__ import annotations

import json
import math

from .syncpoints import sync_point
from .util import now_iso, sha256_hex

TYPES = ("string", "integer", "number", "boolean")
FIELD_KEYS = {"name", "type", "required", "nullable", "default", "target"}
SPEC_KEYS = {"fields"}
# Allowed field types per canonical intake field.  measure may be declared
# integer OR number: a contract is allowed to widen the accepted type, and the
# value is then stored exactly as delivered (SQLite keeps the REAL).  The
# legacy fixed-field protocol still requires an integer measure.
CANONICAL_TYPES = {
    "study": ("string",),
    "person": ("string",),
    "observation": ("string",),
    "edition": ("integer",),
    "display_name": ("string",),
    "measure": ("integer", "number"),
    "memo": ("string",),
}
_INITIAL = "initial"
_KINDS = ("compatible", "branch")

# Types a compatible upgrade may widen FROM (old -> new).
_WIDENING = {("integer", "number")}


# --------------------------------------------------------------------------
# errors
# --------------------------------------------------------------------------
class ContractError(Exception):
    code = "contract_error"
    exit_code = 2

    def __init__(self, message: str, **fields):
        super().__init__(message)
        self.fields = fields

    def as_dict(self) -> dict:
        d = {"code": self.code, "error": str(self)}
        d.update(self.fields)
        return d


class InvalidContract(ContractError):
    code = "invalid_contract"


class UnknownPredecessor(ContractError):
    code = "unknown_predecessor"


class PredecessorRequired(ContractError):
    code = "predecessor_required"


class VersionGap(ContractError):
    code = "version_gap"


class ContentConflict(ContractError):
    code = "content_conflict"


class IncompatibleChange(ContractError):
    code = "incompatible_change"


class VersionCycle(ContractError):
    code = "cycle_detected"


class UnknownContract(ContractError):
    code = "unknown_contract_version"
    exit_code = 4


class InvalidSelection(ContractError):
    code = "invalid_contract_selection"


# --------------------------------------------------------------------------
# field specs
# --------------------------------------------------------------------------
def _nonempty_str(v) -> bool:
    return isinstance(v, str) and bool(v)


def type_ok(value, type_name: str) -> bool:
    if type_name == "string":
        return isinstance(value, str)
    if type_name == "boolean":
        return isinstance(value, bool)
    if isinstance(value, bool):
        return False
    if type_name == "integer":
        return isinstance(value, int)
    if type_name == "number":
        if isinstance(value, int):
            return True
        return isinstance(value, float) and math.isfinite(value)
    return False


def _parse_default(spec, where: str):
    if "default" not in spec:
        return None, False
    value = spec["default"]
    if value is None:
        raise InvalidContract("field %s: default must be non-null" % where,
                              field=spec.get("name"))
    if not type_ok(value, spec["type"]):
        raise InvalidContract(
            "field %s: default does not match type %s" % (where, spec["type"]),
            field=spec.get("name"))
    return value, True


def normalize_field(raw, where: str) -> dict:
    """Validate one field definition and return its canonical form."""
    if not isinstance(raw, dict):
        raise InvalidContract("%s must be a JSON object" % where)
    extra = sorted(set(raw) - FIELD_KEYS)
    if extra:
        raise InvalidContract("%s has unknown keys: %s" % (where, ",".join(extra)))
    name = raw.get("name")
    if not _nonempty_str(name):
        raise InvalidContract("%s.name must be a non-empty string" % where)
    type_name = raw.get("type")
    if type_name not in TYPES:
        raise InvalidContract("%s.type must be one of %s" % (where, "|".join(TYPES)),
                              field=name)
    for key in ("required", "nullable"):
        if not isinstance(raw.get(key), bool):
            raise InvalidContract("%s.%s must be a boolean" % (where, key),
                                  field=name)
    required, nullable = raw["required"], raw["nullable"]
    default, has_default = _parse_default(raw, where)
    if required and has_default:
        raise InvalidContract(
            "field %s: a required field cannot also carry a default" % name,
            field=name)
    target = raw.get("target")
    if target is not None:
        if not isinstance(target, str) or target not in CANONICAL_TYPES:
            raise InvalidContract(
                "field %s: target must be one of %s" % (name, ",".join(CANONICAL_TYPES)),
                field=name)
        if type_name not in CANONICAL_TYPES[target]:
            raise InvalidContract(
                "field %s: target %s requires type %s"
                % (name, target, "|".join(CANONICAL_TYPES[target])), field=name)
        if not required and not has_default:
            raise InvalidContract(
                "field %s: a field mapped to canonical target %s must be "
                "required or carry a default" % (name, target), field=name)
        if nullable and not has_default:
            raise InvalidContract(
                "field %s: a nullable field mapped to canonical target %s needs "
                "a default (a legal null has to resolve to a concrete value; "
                "the stored value is never rewritten)" % (name, target),
                field=name)
    return {"name": name, "type": type_name, "required": required,
            "nullable": nullable, "has_default": has_default,
            "default": default, "target": target}


def parse_spec(document) -> list:
    """Validate a whole fields document; returns canonical specs in order."""
    if isinstance(document, list):
        document = {"fields": document}
    if not isinstance(document, dict):
        raise InvalidContract("fields file must be a JSON object with a 'fields' list")
    extra = sorted(set(document) - SPEC_KEYS)
    if extra:
        raise InvalidContract("fields file has unknown keys: %s" % ",".join(extra))
    raw_fields = document.get("fields")
    if not isinstance(raw_fields, list) or not raw_fields:
        raise InvalidContract("fields must be a non-empty list")
    specs, seen, targets = [], set(), {}
    for i, raw in enumerate(raw_fields):
        spec = normalize_field(raw, "fields[%d]" % i)
        if spec["name"] in seen:
            raise InvalidContract("duplicate field name %r" % spec["name"],
                                  field=spec["name"])
        seen.add(spec["name"])
        if spec["target"] is not None:
            if spec["target"] in targets:
                raise InvalidContract(
                    "canonical target %s mapped more than once (%s and %s)"
                    % (spec["target"], targets[spec["target"]], spec["name"]),
                    field=spec["name"])
            targets[spec["target"]] = spec["name"]
        specs.append(spec)
    missing = [t for t in CANONICAL_TYPES if t not in targets]
    if missing:
        raise InvalidContract(
            "contract must map every canonical intake field; missing: %s"
            % ",".join(missing))
    return specs


# --------------------------------------------------------------------------
# change computation (against a named predecessor)
# --------------------------------------------------------------------------
def _detail(spec: dict) -> dict:
    return {"type": spec["type"], "required": spec["required"],
            "nullable": spec["nullable"], "has_default": spec["has_default"],
            "default": spec["default"], "target": spec["target"]}


def _default_equal(a, b) -> bool:
    if a["has_default"] != b["has_default"]:
        return False
    if not a["has_default"]:
        return True
    return a["default"] == b["default"] and type(a["default"]) is type(b["default"])


def compute_changes(old_specs, new_specs) -> list:
    """Per-field change list.  Deterministic, sorted by field name then kind."""
    old = {s["name"]: s for s in old_specs}
    new = {s["name"]: s for s in new_specs}
    entries = []

    def add(field, kind, compatible, detail):
        entries.append({"field": field, "kind": kind,
                        "compatible": compatible, "detail": detail})

    for name in sorted(set(old) | set(new)):
        a, b = old.get(name), new.get(name)
        if a is None:
            # Only an optional field with a non-null default can be added
            # compatibly.
            ok = (not b["required"]) and b["has_default"]
            add(name, "field_added", ok, _detail(b))
            continue
        if b is None:
            add(name, "field_removed", False, _detail(a))
            continue
        if a["type"] != b["type"]:
            if (a["type"], b["type"]) in _WIDENING:
                add(name, "type_widened", True,
                    {"from": a["type"], "to": b["type"]})
            elif (b["type"], a["type"]) in _WIDENING:
                add(name, "type_narrowed", False,
                    {"from": a["type"], "to": b["type"]})
            else:
                add(name, "type_changed", False,
                    {"from": a["type"], "to": b["type"]})
        if a["required"] != b["required"]:
            add(name, "required_changed", False,
                {"from": a["required"], "to": b["required"]})
        if a["nullable"] != b["nullable"]:
            relaxed = (not a["nullable"]) and b["nullable"]
            add(name, "nullable_relaxed" if relaxed else "nullable_tightened",
                relaxed, {"from": a["nullable"], "to": b["nullable"]})
        if not _default_equal(a, b):
            if not a["has_default"] and b["has_default"]:
                kind = "default_added"
            elif a["has_default"] and not b["has_default"]:
                kind = "default_removed"
            else:
                kind = "default_changed"
            add(name, kind, False,
                {"from": a["default"] if a["has_default"] else None,
                 "to": b["default"] if b["has_default"] else None})
        if a["target"] != b["target"]:
            add(name, "target_changed", False,
                {"from": a["target"], "to": b["target"]})
    entries.sort(key=lambda e: (e["field"], e["kind"]))
    return entries


def classify(entries: list) -> str:
    if not entries:
        return "no_change"
    return "compatible" if all(e["compatible"] for e in entries) else "branch_required"


# --------------------------------------------------------------------------
# digests / persistence helpers
# --------------------------------------------------------------------------
def _digest(provider, material_type, version, predecessor, compatibility,
            specs) -> str:
    payload = {
        "provider": provider,
        "material_type": material_type,
        "version": version,
        "predecessor": predecessor,
        "compatibility": compatibility,
        "fields": sorted(
            ({"name": s["name"], "type": s["type"],
              "required": s["required"], "nullable": s["nullable"],
              "has_default": s["has_default"],
              "default": s["default"] if s["has_default"] else None,
              "target": s["target"]} for s in specs),
            key=lambda f: f["name"]),
    }
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False)
    return sha256_hex(blob.encode("utf-8"))


def _default_json(spec):
    if not spec["has_default"]:
        return None
    return json.dumps(spec["default"], ensure_ascii=False, sort_keys=True)


def _row_to_spec(row) -> dict:
    raw = row["default_json"]
    return {"name": row["name"], "type": row["type"],
            "required": bool(row["required"]), "nullable": bool(row["nullable"]),
            "has_default": raw is not None,
            "default": json.loads(raw) if raw is not None else None,
            "target": row["target"]}


def _fetch_version(conn, provider, material_type, version):
    return conn.execute(
        "SELECT * FROM ct_contracts WHERE provider=? AND material_type=? AND version=?",
        (provider, material_type, version)).fetchone()


def _fetch_fields(conn, provider, material_type, version) -> list:
    rows = conn.execute(
        "SELECT * FROM ct_fields WHERE provider=? AND material_type=? AND version=? "
        "ORDER BY position", (provider, material_type, version)).fetchall()
    return [_row_to_spec(r) for r in rows]


def _max_version(conn, provider, material_type) -> int:
    row = conn.execute(
        "SELECT MAX(version) AS m FROM ct_contracts WHERE provider=? AND material_type=?",
        (provider, material_type)).fetchone()
    return 0 if row is None or row["m"] is None else int(row["m"])


def _require_selectors(provider, material_type, version) -> None:
    if not _nonempty_str(provider):
        raise InvalidSelection("provider must be a non-empty string")
    if not _nonempty_str(material_type):
        raise InvalidSelection("material type must be a non-empty string")
    if isinstance(version, bool) or not isinstance(version, int) or version < 1:
        raise InvalidSelection("contract version must be a positive integer")


def _rollback(conn) -> None:
    try:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
    except Exception:
        pass


# --------------------------------------------------------------------------
# registration
# --------------------------------------------------------------------------
def _cycle_check(conn, provider, material_type, version, predecessor) -> None:
    """Walk the predecessor chain from `predecessor`.  A revisit, or reaching
    the version being registered, is a cycle."""
    seen = set()
    cur = predecessor
    while cur is not None:
        if cur == version or cur in seen:
            raise VersionCycle(
                "predecessor graph for this contract would contain a cycle",
                provider=provider, material_type=material_type, version=version,
                predecessor=predecessor,
                cycle_at=cur, visited=sorted(seen))
        seen.add(cur)
        row = conn.execute(
            "SELECT predecessor FROM ct_contracts WHERE provider=? AND "
            "material_type=? AND version=?", (provider, material_type, cur)).fetchone()
        if row is None:
            return  # validated separately as unknown_predecessor
        cur = row["predecessor"]


def register(store, provider: str, material_type: str, version: int,
             specs: list, predecessor, compatibility):
    """Publish one immutable contract version.

    `specs` is already validated by parse_spec().  Returns (exit_code, dict)
    where status is `recorded` or `duplicate`.
    """
    _require_selectors(provider, material_type, version)
    if compatibility is None:
        compatibility = _INITIAL if predecessor is None else None
    if compatibility not in (_INITIAL,) + _KINDS:
        raise InvalidSelection(
            "compatibility must be 'compatible' or 'branch' (first version: "
            "'initial')")
    digest = _digest(provider, material_type, version, predecessor,
                     compatibility, specs)

    conn = store.connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        # Immutable re-send: identical content is an idempotent confirmation,
        # anything else is rejected.  (Checked before numbering rules so a
        # plain re-send never looks like a gap.)
        existing = _fetch_version(conn, provider, material_type, version)
        if existing is not None:
            if existing["content_digest"] == digest:
                conn.execute("COMMIT")
                summary = json.loads(existing["change_summary"])
                return 0, {
                    "provider": provider, "material_type": material_type,
                    "version": version, "predecessor": existing["predecessor"],
                    "compatibility": existing["compatibility"],
                    "status": "duplicate",
                    "note": "identical contract version already published",
                    "content_digest": digest,
                    "changes": summary["entries"],
                    "classification": summary["classification"]}
            conn.execute("ROLLBACK")
            raise ContentConflict(
                "version already published with different content",
                provider=provider, material_type=material_type, version=version,
                recorded_predecessor=existing["predecessor"],
                recorded_compatibility=existing["compatibility"],
                requested_predecessor=predecessor,
                requested_compatibility=compatibility)

        max_version = _max_version(conn, provider, material_type)
        if max_version == 0:
            if predecessor is not None:
                raise UnknownPredecessor(
                    "the first version of a contract has no predecessor",
                    provider=provider, material_type=material_type,
                    version=version, predecessor=predecessor)
            if compatibility != _INITIAL:
                raise InvalidSelection(
                    "the first version must declare compatibility 'initial'",
                    provider=provider, material_type=material_type, version=version)
            if version != 1:
                raise VersionGap(
                    "the first version must be 1 (skipped number 1)",
                    provider=provider, material_type=material_type, version=version)
            classification = "initial"
            entries = [{"field": s["name"], "kind": "field_defined",
                        "compatible": True, "detail": _detail(s)}
                       for s in sorted(specs, key=lambda x: x["name"])]
        else:
            if predecessor is None:
                raise PredecessorRequired(
                    "a new version must name the predecessor it was compared with",
                    provider=provider, material_type=material_type, version=version)
            if isinstance(predecessor, bool) or not isinstance(predecessor, int):
                raise InvalidSelection("predecessor must be a version number")
            if compatibility not in _KINDS:
                raise InvalidSelection(
                    "a new version must declare compatibility 'compatible' or "
                    "'branch'", provider=provider, material_type=material_type,
                    version=version)
            # A version may never be its own predecessor; the general walk
            # below also catches a loop introduced into the graph by any
            # other means (defensive against corrupted/imported stores).
            if predecessor == version:
                raise VersionCycle(
                    "a contract version cannot be its own predecessor",
                    provider=provider, material_type=material_type,
                    version=version, predecessor=predecessor,
                    cycle_at=predecessor)
            _cycle_check(conn, provider, material_type, version, predecessor)
            if _fetch_version(conn, provider, material_type, predecessor) is None:
                raise UnknownPredecessor(
                    "predecessor version does not exist for this provider and "
                    "material type", provider=provider, material_type=material_type,
                    version=version, predecessor=predecessor)
            if version != max_version + 1:
                raise VersionGap(
                    "a new version must be exactly max+1: versions may not "
                    "skip or be reused", provider=provider,
                    material_type=material_type, version=version,
                    expected=max_version + 1)
            old_specs = _fetch_fields(conn, provider, material_type, predecessor)
            entries = compute_changes(old_specs, specs)
            classification = classify(entries)
            if compatibility == "compatible" and classification == "branch_required":
                raise IncompatibleChange(
                    "the change list contains incompatible entries; declare "
                    "compatibility 'branch' to publish them",
                    provider=provider, material_type=material_type,
                    version=version, predecessor=predecessor,
                    classification=classification, changes=entries)
            if compatibility == _INITIAL:
                raise InvalidSelection(
                    "only the first version may declare compatibility 'initial'",
                    provider=provider, material_type=material_type, version=version)

        summary = {"classification": classification, "entries": entries}
        conn.execute(
            "INSERT INTO ct_contracts VALUES (?,?,?,?,?,?,?,?)",
            (provider, material_type, version, predecessor, compatibility,
             digest, json.dumps(summary, ensure_ascii=False, sort_keys=True),
             now_iso()))
        conn.executemany(
            "INSERT INTO ct_fields VALUES (?,?,?,?,?,?,?,?,?,?)",
            [(provider, material_type, version, i, s["name"], s["type"],
              int(s["required"]), int(s["nullable"]), _default_json(s),
              s["target"]) for i, s in enumerate(specs)])
        # Interleaving hook: validation is complete, the version rows are
        # written, and the write transaction still holds SQLite's write lock.
        sync_point("contract.register.before_commit")
        conn.execute("COMMIT")
        return 0, {"provider": provider, "material_type": material_type,
                   "version": version, "predecessor": predecessor,
                   "compatibility": compatibility, "status": "recorded",
                   "content_digest": digest,
                   "classification": classification,
                   "changes": entries,
                   "field_count": len(specs)}
    except Exception:
        _rollback(conn)
        raise
    finally:
        conn.close()


# --------------------------------------------------------------------------
# comparison / viewing
# --------------------------------------------------------------------------
def compare(store, provider: str, material_type: str, against: int,
            specs: list) -> dict:
    """Compare a candidate field document against one published version.
    Stores nothing."""
    _require_selectors(provider, material_type, against)
    conn = store.connect()
    try:
        row = _fetch_version(conn, provider, material_type, against)
        if row is None:
            raise UnknownContract(
                "unknown contract version", provider=provider,
                material_type=material_type, version=against)
        old_specs = _fetch_fields(conn, provider, material_type, against)
    finally:
        conn.close()
    entries = compute_changes(old_specs, specs)
    return {"provider": provider, "material_type": material_type,
            "against": against, "classification": classify(entries),
            "candidate_fields": len(specs), "changes": entries}


def load(store, provider: str, material_type: str, version: int) -> dict:
    """Load one published version in the form intake needs.  Raises
    UnknownContract."""
    _require_selectors(provider, material_type, version)
    conn = store.connect()
    try:
        row = _fetch_version(conn, provider, material_type, version)
        if row is None:
            raise UnknownContract(
                "unknown contract version", provider=provider,
                material_type=material_type, version=version,
                latest=_max_version(conn, provider, material_type) or None)
        specs = _fetch_fields(conn, provider, material_type, version)
    finally:
        conn.close()
    return {"provider": provider, "material_type": material_type,
            "version": version, "predecessor": row["predecessor"],
            "compatibility": row["compatibility"],
            "content_digest": row["content_digest"],
            "fields": specs,
            "by_name": {s["name"]: s for s in specs},
            "order": [s["name"] for s in specs]}


def show(store, provider: str, material_type: str, version: int) -> dict:
    data = load(store, provider, material_type, version)
    conn = store.connect()
    try:
        row = _fetch_version(conn, provider, material_type, version)
    finally:
        conn.close()
    summary = json.loads(row["change_summary"])
    return {"provider": provider, "material_type": material_type,
            "version": version, "predecessor": row["predecessor"],
            "compatibility": row["compatibility"],
            "content_digest": row["content_digest"],
            "published_at": row["published_at"],
            "classification": summary["classification"],
            "changes": summary["entries"],
            "fields": [{k: v for k, v in s.items() if k != "has_default"}
                       | {"has_default": s["has_default"]} for s in data["fields"]]}


def list_versions(store, provider: str | None = None,
                  material_type: str | None = None) -> list:
    conn = store.connect()
    try:
        sql = ("SELECT provider, material_type, version, predecessor, "
               "compatibility, content_digest, published_at FROM ct_contracts")
        clauses, params = [], []
        if provider is not None:
            clauses.append("provider=?")
            params.append(provider)
        if material_type is not None:
            clauses.append("material_type=?")
            params.append(material_type)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY provider, material_type, version"
        return [dict(r) for r in conn.execute(sql, params)]
    finally:
        conn.close()


# --------------------------------------------------------------------------
# intake-side validation
# --------------------------------------------------------------------------
def _canonical_domain_ok(value, target: str, declared_type: str) -> bool:
    """Domain rules of the original intake protocol for canonical targets:
    identifiers are non-empty strings, edition is a positive integer; measure
    follows its declared type (the original protocol's integer rule applies
    only when the contract declares integer)."""
    if target in ("study", "person", "observation"):
        return isinstance(value, str) and bool(value)
    if target == "edition":
        return isinstance(value, int) and not isinstance(value, bool) and value >= 1
    if target == "measure":
        if declared_type == "integer":
            return isinstance(value, int) and not isinstance(value, bool)
        return type_ok(value, "number")
    return True


def validate_line(obj, contract: dict, errors: list) -> dict | None:
    """Validate one parsed line against the chosen version.

    Diagnostics are appended to `errors`.  Returns the canonical field dict
    (defaults applied) or None.  Diagnostics carry the line-level field name
    and category only - never a display_name / measure / memo value.
    """
    def add_error(entry):
        errors.append(entry)

    if not isinstance(obj, dict):
        add_error({"reason": "line_not_object"})
        return None
    by_name = contract["by_name"]
    unknown = sorted(k for k in obj if k not in by_name)
    for name in unknown:
        add_error({"field": name, "reason": "unknown_field"})
    canonical = {}
    for name in contract["order"]:
        spec = by_name[name]
        if name in obj:
            value = obj[name]
            if value is None:
                if not spec["nullable"]:
                    add_error({"field": name, "reason": "null_not_allowed"})
                    continue
                # Legal null.  A contract-mapped field must still resolve to
                # a concrete stored value, so its declared default stands in;
                # the delivered value is never rewritten.
                if not spec["has_default"]:
                    continue
                value = spec["default"]
                if not _canonical_domain_ok(value, spec["target"], spec["type"]):
                    # A stored default that violates the contract is a schema
                    # problem, not an input problem; refuse the row rather
                    # than store an out-of-domain value.
                    add_error({"field": name, "reason": "invalid_default"})
                    continue
            elif not type_ok(value, spec["type"]):
                add_error({"field": name, "reason": "type_mismatch",
                           "expected": spec["type"]})
                continue
        else:
            if spec["required"]:
                add_error({"field": name, "reason": "missing_required_field"})
                continue
            if spec["has_default"]:
                value = spec["default"]
                if spec["target"] is not None and not _canonical_domain_ok(
                        value, spec["target"], spec["type"]):
                    add_error({"field": name, "reason": "invalid_default"})
                    continue
            else:
                continue
        if spec["target"] is not None:
            # Canonical targets carry the original protocol's domain rules,
            # so a nice category is reported here instead of the generic
            # second gate.
            if not _canonical_domain_ok(value, spec["target"], spec["type"]):
                add_error({"field": name, "reason": "value_out_of_range",
                           "target": spec["target"]})
                continue
            canonical[spec["target"]] = value
    if unknown:
        return None
    missing = [t for t in CANONICAL_TYPES if t not in canonical]
    if missing and not errors:
        # Defensive: parse_spec() guarantees every canonical target is either
        # required or defaulted, so this cannot normally happen.  It is only
        # reported when no more specific diagnostic already explains the line.
        add_error({"reason": "contract_incomplete", "missing": missing})
    if missing:
        return None
    return canonical
