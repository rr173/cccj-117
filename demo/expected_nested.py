#!/usr/bin/env python3
"""Hand-written expectations for the nested study-table demo.

Nothing here calls the production implementation.  All row identities are
recomputed here from the documented identity rule (sha256 of the JSON
encoding of [source_digest, root_index, array_path]), where source_digest is
sha256 of the exact bytes written to the fixture file.  All projected
values, presence markers, parent links and source coordinates are literals
derived by hand from the fixtures below.

The demo writes the fixtures with byte-exact content and compares the real
output database against these constants with its own SQL.
"""
from __future__ import annotations

import hashlib
import json

# ---------------------------------------------------------------------------
# plan used by the whole demo (also written verbatim to samples/plan.json)
# ---------------------------------------------------------------------------

def _plan_digest(plan):
    import hashlib
    raw = json.dumps(plan, ensure_ascii=False, sort_keys=True,
                     separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


PLAN = {
    "version": 1,
    "root": {
        "name": "participants",
        "columns": [
            {"name": "person", "path": ["person"], "type": "string"},
            {"name": "study", "path": ["study"], "type": "string"},
            {"name": "display.label", "path": ["display.label"], "type": "string"},
            {"name": "ratio", "path": ["ratio"], "type": "decimal"},
            {"name": "visits_after", "path": ["visits_after"], "type": "integer"},
            {"name": "memo", "path": ["memo"], "type": "string"},
        ],
    },
    "tables": [
        {"name": "visits", "parent": "participants", "path": ["visits"],
         "columns": [
             {"name": "visit_label", "path": ["label"], "type": "string"},
             {"name": "visit_note", "path": ["note"], "type": "string"}]},
        {"name": "readings", "parent": "visits", "path": ["readings"],
         "columns": [
             {"name": "code", "path": ["code"], "type": "string"},
             {"name": "value", "path": ["value"], "type": "integer"}]},
        {"name": "consents", "parent": "participants", "path": ["consents"],
         "columns": [
             {"name": "kind", "path": ["kind"], "type": "string"},
             {"name": "granted", "path": ["granted"], "type": "boolean"}]},
        {"name": "tags", "parent": "participants", "path": ["meta", "tags"],
         "columns": [{"name": "tag", "path": ["name"], "type": "string"}]},
        {"name": "subs", "parent": "visits", "path": ["subs"],
         "columns": [{"name": "sval", "path": ["s"], "type": "decimal"}]},
    ],
}

PLAN_DIGEST = _plan_digest(PLAN)

# ---------------------------------------------------------------------------
# fixtures (strings; written byte-exact by the demo)
# ---------------------------------------------------------------------------

# The example from the task, verbatim.
F_EXAMPLE = (
    '[\n'
    '{"person":"P1","study":"S1","visits":[{"label":"V1","readings":'
    '[{"code":"A","value":0},{"code":"A","value":2}]},{"label":"V2",'
    '"readings":[]}],"consents":[{"kind":"lab"},{"kind":"contact"},'
    '{"kind":"reuse"}]}\n'
    ']\n'
)

# Same participant, but person/study (and a trailing scalar) are written
# AFTER the huge child arrays.  Every projected value must be identical
# except the source digest; row identities differ because the file bytes
# differ (documented identity includes the source digest).
F_LATE = (
    '[\n'
    '{"visits":[{"label":"V1","readings":[{"code":"A","value":0},'
    '{"code":"A","value":2}]},{"label":"V2","readings":[]}],'
    '"consents":[{"kind":"lab"},{"kind":"contact"},{"kind":"reuse"}],'
    '"person":"P1","study":"S1"}\n'
    ']\n'
)

# Empty arrays, missing fields, explicit null, false, 0, duplicate business
# values and keys containing a dot, slash and space.
F_VARIANTS = (
    '[\n'
    '{"person":"P1","study":"S1","visits":[],'
    '"consents":[{"kind":"x","granted":false},{"kind":"x","granted":false}],'
    '"meta":{"tags":[{"name":"a/b.c"},{"name":"x y"}]},'
    '"ratio":0,"visits_after":0,"memo":"","display.label":"D"},\n'
    '{"person":"P2","visits":[{"label":"V","readings":[{"code":"K","value":0},'
    '{"code":"K","value":0}]}],"memo":null,"ratio":null}\n'
    ']\n'
)

# A bad root in the middle: the second root repeats the key "person".
F_BAD_MIDDLE = (
    '[\n'
    '{"person":"R0","study":"S"},\n'
    '{"person":"R1","person":"R1b","study":"S"},\n'
    '{"person":"R2","study":"S"}\n'
    ']\n'
)

# A bad root in the middle: the second root has a type error (person given
# as a number although the plan declares string).
F_BAD_TYPE = (
    '[\n'
    '{"person":"R0","study":"S"},\n'
    '{"person":7,"study":"S"},\n'
    '{"person":"R2","study":"S"}\n'
    ']\n'
)

# Syntax damage at the end: the second root is truncated mid-object.
F_TRUNCATED = (
    '[\n'
    '{"person":"T0","study":"S"},\n'
    '{"person":"T1","study":"S",'
)

# Same render as F_EXAMPLE but with a small chunk size so the retry path is
# exercised; content identical to F_EXAMPLE.
F_SMALL_CHUNK = F_EXAMPLE

# ---------------------------------------------------------------------------
# helpers used to write expectations by hand
# ---------------------------------------------------------------------------

def sha_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def rowid(digest: str, root_index: int, array_path) -> str:
    """The documented identity rule, recomputed here independently."""
    raw = json.dumps([digest, root_index, array_path], separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


D_EXAMPLE = sha_text(F_EXAMPLE)
D_LATE = sha_text(F_LATE)
D_VARIANTS = sha_text(F_VARIANTS)
D_BAD_MIDDLE = sha_text(F_BAD_MIDDLE)
D_BAD_TYPE = sha_text(F_BAD_TYPE)
D_TRUNCATED = sha_text(F_TRUNCATED)


def v(value, presence):
    return {"value": value, "presence": presence}


# ---------------------------------------------------------------------------
# expectations for F_EXAMPLE
#
# root:   participants P1/S1, index_path [], array_path [], ordinal 0
# visits: [0] V1 (ordinal 0), [1] V2 (ordinal 1)   parent = root row
# readings: [0,0] A/0, [0,1] A/2                   parent = visits[0]
#           V2 has no readings, so no row exists - but V2 itself is kept
# consents: [0] lab, [1] contact, [2] reuse         parent = root row
# ---------------------------------------------------------------------------

E_EXAMPLE = {
    "participants": [
        {"row_id": rowid(D_EXAMPLE, 0, []),
         "parent_row_id": None,
         "root_row_id": rowid(D_EXAMPLE, 0, []),
         "root_index": 0,
         "index_path": [],
         "array_path": [],
         "ordinal": 0,
         "values": {
             "person": v("P1", 1),
             "study": v("S1", 1),
             "display.label": v(None, 0),
             "ratio": v(None, 0),
             "visits_after": v(None, 0),
             "memo": v(None, 0),
         }},
    ],
    "visits": [
        {"row_id": rowid(D_EXAMPLE, 0, [["visits", 0]]),
         "parent_row_id": rowid(D_EXAMPLE, 0, []),
         "root_row_id": rowid(D_EXAMPLE, 0, []),
         "root_index": 0, "index_path": [0],
         "array_path": [["visits", 0]], "ordinal": 0,
         "values": {"visit_label": v("V1", 1),
                    "visit_note": v(None, 0)}},
        {"row_id": rowid(D_EXAMPLE, 0, [["visits", 1]]),
         "parent_row_id": rowid(D_EXAMPLE, 0, []),
         "root_row_id": rowid(D_EXAMPLE, 0, []),
         "root_index": 0, "index_path": [1],
         "array_path": [["visits", 1]], "ordinal": 1,
         "values": {"visit_label": v("V2", 1),
                    "visit_note": v(None, 0)}},
    ],
    "readings": [
        {"row_id": rowid(D_EXAMPLE, 0, [["visits", 0], ["readings", 0]]),
         "parent_row_id": rowid(D_EXAMPLE, 0, [["visits", 0]]),
         "root_row_id": rowid(D_EXAMPLE, 0, []),
         "root_index": 0, "index_path": [0, 0],
         "array_path": [["visits", 0], ["readings", 0]], "ordinal": 0,
         "values": {"code": v("A", 1), "value": v("0", 1)}},
        {"row_id": rowid(D_EXAMPLE, 0, [["visits", 0], ["readings", 1]]),
         "parent_row_id": rowid(D_EXAMPLE, 0, [["visits", 0]]),
         "root_row_id": rowid(D_EXAMPLE, 0, []),
         "root_index": 0, "index_path": [0, 1],
         "array_path": [["visits", 0], ["readings", 1]], "ordinal": 1,
         "values": {"code": v("A", 1), "value": v("2", 1)}},
    ],
    "consents": [
        {"row_id": rowid(D_EXAMPLE, 0, [["consents", 0]]),
         "parent_row_id": rowid(D_EXAMPLE, 0, []),
         "root_row_id": rowid(D_EXAMPLE, 0, []),
         "root_index": 0, "index_path": [0],
         "array_path": [["consents", 0]], "ordinal": 0,
         "values": {"kind": v("lab", 1), "granted": v(None, 0)}},
        {"row_id": rowid(D_EXAMPLE, 0, [["consents", 1]]),
         "parent_row_id": rowid(D_EXAMPLE, 0, []),
         "root_row_id": rowid(D_EXAMPLE, 0, []),
         "root_index": 0, "index_path": [1],
         "array_path": [["consents", 1]], "ordinal": 1,
         "values": {"kind": v("contact", 1), "granted": v(None, 0)}},
        {"row_id": rowid(D_EXAMPLE, 0, [["consents", 2]]),
         "parent_row_id": rowid(D_EXAMPLE, 0, []),
         "root_row_id": rowid(D_EXAMPLE, 0, []),
         "root_index": 0, "index_path": [2],
         "array_path": [["consents", 2]], "ordinal": 2,
         "values": {"kind": v("reuse", 1), "granted": v(None, 0)}},
    ],
    "tags": [],
    "subs": [],
}

E_EXAMPLE_COUNTS = {"participants": 1, "visits": 2, "readings": 2,
                    "consents": 3, "tags": 0, "subs": 0}
# the per-root record only lists tables that actually produced rows in it
E_EXAMPLE_ROOT_COUNTS = {"participants": 1, "visits": 2, "readings": 2,
                         "consents": 3}

# ---------------------------------------------------------------------------
# expectations for F_LATE: same projected values, same path coordinates,
# different digest / row identities.
# ---------------------------------------------------------------------------

E_LATE = {
    "participants": [
        {"row_id": rowid(D_LATE, 0, []),
         "parent_row_id": None,
         "root_row_id": rowid(D_LATE, 0, []),
         "root_index": 0, "index_path": [], "array_path": [], "ordinal": 0,
         "values": {
             "person": v("P1", 1), "study": v("S1", 1),
             "display.label": v(None, 0), "ratio": v(None, 0),
             "visits_after": v(None, 0), "memo": v(None, 0),
         }},
    ],
    "visits": [
        {"row_id": rowid(D_LATE, 0, [["visits", 0]]),
         "parent_row_id": rowid(D_LATE, 0, []),
         "root_row_id": rowid(D_LATE, 0, []),
         "root_index": 0, "index_path": [0],
         "array_path": [["visits", 0]], "ordinal": 0,
         "values": {"visit_label": v("V1", 1),
                    "visit_note": v(None, 0)}},
        {"row_id": rowid(D_LATE, 0, [["visits", 1]]),
         "parent_row_id": rowid(D_LATE, 0, []),
         "root_row_id": rowid(D_LATE, 0, []),
         "root_index": 0, "index_path": [1],
         "array_path": [["visits", 1]], "ordinal": 1,
         "values": {"visit_label": v("V2", 1),
                    "visit_note": v(None, 0)}},
    ],
    "readings": [
        {"row_id": rowid(D_LATE, 0, [["visits", 0], ["readings", 0]]),
         "parent_row_id": rowid(D_LATE, 0, [["visits", 0]]),
         "root_row_id": rowid(D_LATE, 0, []),
         "root_index": 0, "index_path": [0, 0],
         "array_path": [["visits", 0], ["readings", 0]], "ordinal": 0,
         "values": {"code": v("A", 1), "value": v("0", 1)}},
        {"row_id": rowid(D_LATE, 0, [["visits", 0], ["readings", 1]]),
         "parent_row_id": rowid(D_LATE, 0, [["visits", 0]]),
         "root_row_id": rowid(D_LATE, 0, []),
         "root_index": 0, "index_path": [0, 1],
         "array_path": [["visits", 0], ["readings", 1]], "ordinal": 1,
         "values": {"code": v("A", 1), "value": v("2", 1)}},
    ],
    "consents": [
        {"row_id": rowid(D_LATE, 0, [["consents", 0]]),
         "parent_row_id": rowid(D_LATE, 0, []),
         "root_row_id": rowid(D_LATE, 0, []),
         "root_index": 0, "index_path": [0],
         "array_path": [["consents", 0]], "ordinal": 0,
         "values": {"kind": v("lab", 1), "granted": v(None, 0)}},
        {"row_id": rowid(D_LATE, 0, [["consents", 1]]),
         "parent_row_id": rowid(D_LATE, 0, []),
         "root_row_id": rowid(D_LATE, 0, []),
         "root_index": 0, "index_path": [1],
         "array_path": [["consents", 1]], "ordinal": 1,
         "values": {"kind": v("contact", 1), "granted": v(None, 0)}},
        {"row_id": rowid(D_LATE, 0, [["consents", 2]]),
         "parent_row_id": rowid(D_LATE, 0, []),
         "root_row_id": rowid(D_LATE, 0, []),
         "root_index": 0, "index_path": [2],
         "array_path": [["consents", 2]], "ordinal": 2,
         "values": {"kind": v("reuse", 1), "granted": v(None, 0)}},
    ],
    "tags": [],
    "subs": [],
}

# ---------------------------------------------------------------------------
# expectations for F_VARIANTS
#
# root 0: empty visits (no visit rows), two *identical* consents (two rows),
#         two tags whose keys contain "/", "." and " ", ratio 0 (present),
#         visits_after 0 (present), memo "" (present), display.label present.
# root 1: visits with two identical readings (two rows), memo explicit null,
#         ratio explicit null, study MISSING, display.label MISSING.
# ---------------------------------------------------------------------------

E_VARIANTS = {
    "participants": [
        {"row_id": rowid(D_VARIANTS, 0, []),
         "parent_row_id": None,
         "root_row_id": rowid(D_VARIANTS, 0, []),
         "root_index": 0, "index_path": [], "array_path": [], "ordinal": 0,
         "values": {
             "person": v("P1", 1),
             "study": v("S1", 1),
             "display.label": v("D", 1),
             "ratio": v("0", 1),
             "visits_after": v("0", 1),
             "memo": v("", 1),
         }},
        {"row_id": rowid(D_VARIANTS, 1, []),
         "parent_row_id": None,
         "root_row_id": rowid(D_VARIANTS, 1, []),
         "root_index": 1, "index_path": [], "array_path": [], "ordinal": 0,
         "values": {
             "person": v("P2", 1),
             "study": v(None, 0),
             "display.label": v(None, 0),
             "ratio": v(None, 2),
             "visits_after": v(None, 0),
             "memo": v(None, 2),
         }},
    ],
    "visits": [
        {"row_id": rowid(D_VARIANTS, 1, [["visits", 0]]),
         "parent_row_id": rowid(D_VARIANTS, 1, []),
         "root_row_id": rowid(D_VARIANTS, 1, []),
         "root_index": 1, "index_path": [0],
         "array_path": [["visits", 0]], "ordinal": 0,
         "values": {"visit_label": v("V", 1),
                    "visit_note": v(None, 0)}},
    ],
    "readings": [
        {"row_id": rowid(D_VARIANTS, 1, [["visits", 0], ["readings", 0]]),
         "parent_row_id": rowid(D_VARIANTS, 1, [["visits", 0]]),
         "root_row_id": rowid(D_VARIANTS, 1, []),
         "root_index": 1, "index_path": [0, 0],
         "array_path": [["visits", 0], ["readings", 0]], "ordinal": 0,
         "values": {"code": v("K", 1), "value": v("0", 1)}},
        {"row_id": rowid(D_VARIANTS, 1, [["visits", 0], ["readings", 1]]),
         "parent_row_id": rowid(D_VARIANTS, 1, [["visits", 0]]),
         "root_row_id": rowid(D_VARIANTS, 1, []),
         "root_index": 1, "index_path": [0, 1],
         "array_path": [["visits", 0], ["readings", 1]], "ordinal": 1,
         "values": {"code": v("K", 1), "value": v("0", 1)}},
    ],
    "consents": [
        {"row_id": rowid(D_VARIANTS, 0, [["consents", 0]]),
         "parent_row_id": rowid(D_VARIANTS, 0, []),
         "root_row_id": rowid(D_VARIANTS, 0, []),
         "root_index": 0, "index_path": [0],
         "array_path": [["consents", 0]], "ordinal": 0,
         "values": {"kind": v("x", 1), "granted": v(0, 1)}},
        {"row_id": rowid(D_VARIANTS, 0, [["consents", 1]]),
         "parent_row_id": rowid(D_VARIANTS, 0, []),
         "root_row_id": rowid(D_VARIANTS, 0, []),
         "root_index": 0, "index_path": [1],
         "array_path": [["consents", 1]], "ordinal": 1,
         "values": {"kind": v("x", 1), "granted": v(0, 1)}},
    ],
    "tags": [
        {"row_id": rowid(D_VARIANTS, 0, [["tags", 0]]),
         "parent_row_id": rowid(D_VARIANTS, 0, []),
         "root_row_id": rowid(D_VARIANTS, 0, []),
         "root_index": 0, "index_path": [0],
         "array_path": [["tags", 0]], "ordinal": 0,
         "values": {"tag": v("a/b.c", 1)}},
        {"row_id": rowid(D_VARIANTS, 0, [["tags", 1]]),
         "parent_row_id": rowid(D_VARIANTS, 0, []),
         "root_row_id": rowid(D_VARIANTS, 0, []),
         "root_index": 0, "index_path": [1],
         "array_path": [["tags", 1]], "ordinal": 1,
         "values": {"tag": v("x y", 1)}},
    ],
    "subs": [],
}

E_VARIANTS_COUNTS = {"participants": 2, "visits": 1, "readings": 2,
                     "consents": 2, "tags": 2, "subs": 0}

# ---------------------------------------------------------------------------
# rejection scenarios
# ---------------------------------------------------------------------------

E_BAD_MIDDLE_COUNTS = {"participants": 2, "visits": 0, "readings": 0,
                       "consents": 0, "tags": 0, "subs": 0}
E_BAD_MIDDLE_ROOTS = [
    {"root_index": 0, "status": "committed", "category": None},
    {"root_index": 1, "status": "rejected", "category": "duplicate_key"},
    {"root_index": 2, "status": "committed", "category": None},
]
E_BAD_MIDDLE_PERSONS = ["R0", "R2"]

E_BAD_TYPE_COUNTS = {"participants": 2, "visits": 0, "readings": 0,
                     "consents": 0, "tags": 0, "subs": 0}
E_BAD_TYPE_ROOTS = [
    {"root_index": 0, "status": "committed", "category": None},
    {"root_index": 1, "status": "rejected", "category": "type_mismatch"},
    {"root_index": 2, "status": "committed", "category": None},
]
E_BAD_TYPE_PERSONS = ["R0", "R2"]

# Truncated file: root 0 committed, root 1 never committed (its half tree
# does not exist), run aborted at a syntax position.
E_TRUNCATED_COUNTS = {"participants": 1, "visits": 0, "readings": 0,
                      "consents": 0, "tags": 0, "subs": 0}
E_TRUNCATED_PERSONS = ["T0"]
E_TRUNCATED_ROOTS = [{"root_index": 0, "status": "committed",
                      "category": None}]
E_TRUNCATED_POSITION = len(F_TRUNCATED)     # premature EOF reports the end

# ---------------------------------------------------------------------------
# plan rejection cases (never read a byte of input: compile only)
# ---------------------------------------------------------------------------

PLAN_REJECTIONS = [
    ("unknown_parent", {
        "version": 1,
        "root": {"name": "r", "columns": [
            {"name": "a", "path": ["a"], "type": "string"}]},
        "tables": [{"name": "c", "parent": "nope", "path": ["c"],
                    "columns": [{"name": "b", "path": ["b"],
                                 "type": "string"}]}]}),
    ("parent_cycle", {
        "version": 1,
        "root": {"name": "r", "columns": [
            {"name": "a", "path": ["a"], "type": "string"}]},
        "tables": [
            {"name": "c1", "parent": "c2", "path": ["c1"],
             "columns": [{"name": "b", "path": ["b"], "type": "string"}]},
            {"name": "c2", "parent": "c1", "path": ["c2"],
             "columns": [{"name": "d", "path": ["d"], "type": "string"}]}]}),
    ("duplicate_table", {
        "version": 1,
        "root": {"name": "r", "columns": [
            {"name": "a", "path": ["a"], "type": "string"}]},
        "tables": [
            {"name": "c", "parent": "r", "path": ["c"],
             "columns": [{"name": "b", "path": ["b"], "type": "string"}]},
            {"name": "c", "parent": "r", "path": ["d"],
             "columns": [{"name": "e", "path": ["e"], "type": "string"}]}]}),
    ("duplicate_column", {
        "version": 1,
        "root": {"name": "r", "columns": [
            {"name": "a", "path": ["a"], "type": "string"},
            {"name": "a", "path": ["b"], "type": "string"}]},
        "tables": []}),
    ("conflicting_definition", {
        "version": 1,
        "root": {"name": "r", "columns": [
            {"name": "a", "path": ["x"], "type": "string"}]},
        "tables": [{"name": "c", "parent": "r", "path": ["x"],
                    "columns": [{"name": "b", "path": ["b"],
                                 "type": "string"}]}]}),
    ("conflicting_definition", {
        "version": 1,
        "root": {"name": "r", "columns": [
            {"name": "a", "path": ["m", "a"], "type": "string"}]},
        "tables": [{"name": "c", "parent": "r", "path": ["m"],
                    "columns": [{"name": "b", "path": ["b"],
                                 "type": "string"}]}]}),
    ("duplicate_table_target", {
        "version": 1,
        "root": {"name": "r", "columns": [
            {"name": "a", "path": ["a"], "type": "string"}]},
        "tables": [
            {"name": "c1", "parent": "r", "path": ["c"],
             "columns": [{"name": "b", "path": ["b"], "type": "string"}]},
            {"name": "c2", "parent": "r", "path": ["c"],
             "columns": [{"name": "d", "path": ["d"], "type": "string"}]}]}),
    ("reserved_column_name", {
        "version": 1,
        "root": {"name": "r", "columns": [
            {"name": "index_path", "path": ["a"], "type": "string"}]},
        "tables": []}),
    ("invalid_column_type", {
        "version": 1,
        "root": {"name": "r", "columns": [
            {"name": "a", "path": ["a"], "type": "number"}]},
        "tables": []}),
    ("reserved_table_name", {
        "version": 1,
        "root": {"name": "sqlite_x", "columns": [
            {"name": "a", "path": ["a"], "type": "string"}]},
        "tables": []}),
    ("reserved_table_name", {
        "version": 1,
        "root": {"name": "meta", "columns": [
            {"name": "a", "path": ["a"], "type": "string"}]},
        "tables": []}),
    ("invalid_path", {
        "version": 1,
        "root": {"name": "r", "columns": [
            {"name": "a", "path": "a", "type": "string"}]},
        "tables": []}),
]


# ---------------------------------------------------------------------------
# resume scenario: three roots, the first is committed before a forced exit
# (SIGKILL at nested.root_before_commit), then the run is resumed.  The
# resulting database must contain exactly the same three roots, once each.
# ---------------------------------------------------------------------------

F_RESUME = (
    '[' +
    ','.join(
        '{"person":"G%d","study":"S","visits":[{"label":"V","readings":'
        '[{"code":"C","value":%d}]}],"consents":[{"kind":"k"}]}' % (i, i)
        for i in range(3)) +
    ']'
)

D_RESUME = sha_text(F_RESUME)


def _resume_root(i):
    return rowid(D_RESUME, i, [])


def _resume_visit(i):
    return rowid(D_RESUME, i, [["visits", 0]])


def _resume_reading(i):
    return rowid(D_RESUME, i, [["visits", 0], ["readings", 0]])


def _resume_consent(i):
    return rowid(D_RESUME, i, [["consents", 0]])


E_RESUME = {
    "participants": [
        {"row_id": _resume_root(i), "parent_row_id": None,
         "root_row_id": _resume_root(i), "root_index": i,
         "index_path": [], "array_path": [], "ordinal": 0,
         "values": {"person": v("G%d" % i, 1), "study": v("S", 1),
                    "display.label": v(None, 0), "ratio": v(None, 0),
                    "visits_after": v(None, 0), "memo": v(None, 0)}}
        for i in range(3)],
    "visits": [
        {"row_id": _resume_visit(i), "parent_row_id": _resume_root(i),
         "root_row_id": _resume_root(i), "root_index": i, "index_path": [0],
         "array_path": [["visits", 0]], "ordinal": 0,
         "values": {"visit_label": v("V", 1), "visit_note": v(None, 0)}}
        for i in range(3)],
    "readings": [
        {"row_id": _resume_reading(i), "parent_row_id": _resume_visit(i),
         "root_row_id": _resume_root(i), "root_index": i,
         "index_path": [0, 0],
         "array_path": [["visits", 0], ["readings", 0]], "ordinal": 0,
         "values": {"code": v("C", 1), "value": v(str(i), 1)}}
        for i in range(3)],
    "consents": [
        {"row_id": _resume_consent(i), "parent_row_id": _resume_root(i),
         "root_row_id": _resume_root(i), "root_index": i, "index_path": [0],
         "array_path": [["consents", 0]], "ordinal": 0,
         "values": {"kind": v("k", 1), "granted": v(None, 0)}}
        for i in range(3)],
    "tags": [],
    "subs": [],
}
E_RESUME_COUNTS = {"participants": 3, "visits": 3, "readings": 3,
                   "consents": 3, "tags": 0, "subs": 0}
E_RESUME_ROOTS = [{"root_index": i, "status": "committed", "category": None}
                  for i in range(3)]

# ---------------------------------------------------------------------------
# large single-root fixture: a few roots, one root with many descendants.
# Fixture builder information is duplicated here by hand: 3 roots; root 1
# has ROOTS_BIG_VISITS visits of ROOTS_BIG_READINGS readings each plus one
# consent.  Values are deterministic (reading i has code "C%05d" and value
# i), the last reading of the last visit is the final one written.
# ---------------------------------------------------------------------------

BIG_ROOTS = 3
BIG_VISITS = 2
BIG_READINGS = 60000

BIG_EXPECT = {
    "counts": {
        "participants": 3,
        "visits": BIG_VISITS + 2,        # root 1's visits; roots 0/2 have 1
        "readings": BIG_VISITS * BIG_READINGS + 2,
        "consents": 3,
        "tags": 0,
        "subs": 0,
    },
    # codes are printed modulo 100000, so the last code wraps
    "last_reading": ("C%05d" % ((BIG_VISITS * BIG_READINGS - 1) % 100000),
                     str(BIG_VISITS * BIG_READINGS - 1), 1),
}


# ---------------------------------------------------------------------------
# the minimal four-table plan documented in README 13.1.1 (the task example)
# ---------------------------------------------------------------------------

MINIMAL_PLAN = {
    "version": 1,
    "root": {
        "name": "participants",
        "columns": [
            {"name": "person", "path": ["person"], "type": "string"},
            {"name": "study", "path": ["study"], "type": "string"},
        ],
    },
    "tables": [
        {"name": "visits", "parent": "participants", "path": ["visits"],
         "columns": [
             {"name": "visit_label", "path": ["label"], "type": "string"},
         ]},
        {"name": "readings", "parent": "visits", "path": ["readings"],
         "columns": [
             {"name": "code", "path": ["code"], "type": "string"},
             {"name": "value", "path": ["value"], "type": "integer"},
         ]},
        {"name": "consents", "parent": "participants", "path": ["consents"],
         "columns": [
             {"name": "kind", "path": ["kind"], "type": "string"},
         ]},
    ],
}

MINIMAL_COUNTS = {"participants": 1, "visits": 2, "readings": 2,
                  "consents": 3}
