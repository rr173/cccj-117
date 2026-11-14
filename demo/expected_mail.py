#!/usr/bin/env python3
"""Hand-written expectations for the mail-channel demo.

Every payload is a literal here; the demo assembles real MIME bytes from
these literals and verify_mail.py compares the store against them with its
own SQL.  Digests are derived from the literals with hashlib (sha256 is
deterministic), never from the implementation under test.
"""
from __future__ import annotations

import hashlib
import json


def jl(rows, sep, trailing):
    """Encode rows as JSONL bytes (UTF-8, unescaped non-ASCII)."""
    body = sep.join(json.dumps(r, ensure_ascii=False) for r in rows)
    if trailing:
        body += sep
    return body.encode("utf-8")


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# --------------------------------------------------------------------------
# mail 1: nested related image + two differently encoded data parts + manifest
# --------------------------------------------------------------------------
# batch mail-b1: base64, CRLF line endings, NO trailing newline, non-ASCII
B1_ROWS = [
    {"study": "mx1", "person": "p-ann", "observation": "site-alpha",
     "edition": 1, "display_name": "Ann Müller", "measure": 10,
     "memo": "first café"},
    {"study": "mx1", "person": "p-bob", "observation": "site-beta",
     "edition": 1, "display_name": "Bob 林", "measure": 20, "memo": "naïve"},
    {"study": "mx1", "person": "p-ann", "observation": "site-alpha",
     "edition": 2, "display_name": "Ann Müller", "measure": 11,
     "memo": "revised ✓"},
]
B1_PAYLOAD = jl(B1_ROWS, "\r\n", trailing=False)
B1_SHA = sha(B1_PAYLOAD)

# batch mail-b2: quoted-printable, LF line endings, trailing newline, a long
# non-ASCII memo that forces several soft line breaks
LONG_MEMO = ("tést☃ " * 40).strip() + " 🌡️"
B2_ROWS = [
    {"study": "mx1", "person": "p-cid", "observation": "site-gamma",
     "edition": 1, "display_name": "Cid 王", "measure": 30, "memo": LONG_MEMO},
    {"study": "mx1", "person": "p-dan", "observation": "site-delta",
     "edition": 1, "display_name": "Dan", "measure": 40, "memo": ""},
]
B2_PAYLOAD = jl(B2_ROWS, "\n", trailing=True)
B2_SHA = sha(B2_PAYLOAD)

M1_BATCHES = [("mail-b1", "data-1@rvault", B1_SHA),
              ("mail-b2", "data-2@rvault", B2_SHA)]

# expected editions rows: (study, observation, edition, person, display_name,
# measure, memo, batch_tag)
M1_EDITIONS = [
    ("mx1", "site-alpha", 1, "p-ann", "Ann Müller", 10, "first café", "mail-b1"),
    ("mx1", "site-alpha", 2, "p-ann", "Ann Müller", 11, "revised ✓", "mail-b1"),
    ("mx1", "site-beta", 1, "p-bob", "Bob 林", 20, "naïve", "mail-b1"),
    ("mx1", "site-delta", 1, "p-dan", "Dan", 40, "", "mail-b2"),
    ("mx1", "site-gamma", 1, "p-cid", "Cid 王", 30, LONG_MEMO, "mail-b2"),
]
# expected change-feed events: (seq, frame_id, op, study, observation)
M1_EVENTS = [
    (1, 1, "refresh", "mx1", "site-alpha"),
    (2, 1, "refresh", "mx1", "site-beta"),
    (3, 2, "refresh", "mx1", "site-delta"),
    (4, 2, "refresh", "mx1", "site-gamma"),
]
M1_MANIFEST_PART = "5"
M1_PART_COUNT = 7

# --------------------------------------------------------------------------
# error mails (all must be rejected in preflight; nothing is imported)
# --------------------------------------------------------------------------
# name -> expected ordered list of error categories
ERR_SECRET = "DO-NOT-LEAK"
ERROR_CASES = {
    "e1_digest_mismatch": ["digest_mismatch"],
    "e2_duplicate_cid": ["content_id_duplicate", "reference_ambiguous"],
    "e3_unterminated": ["unterminated_multipart"],
    "e4_unsupported_cte": ["unsupported_transfer_encoding"],
    "e5_invalid_encoding": ["invalid_transfer_encoding",
                            "invalid_transfer_encoding"],
    "e7_rfc822": ["manifest_missing"],
    "e8_manifest_duplicate": ["manifest_duplicate"],
    "e9_manifest_invalid": ["manifest_invalid"],
    "e10_batch_tag_duplicate": ["batch_tag_duplicate"],
    "e11_reference_unresolved": ["reference_unresolved"],
}
ERROR_PARTS = {  # expected "part" of the first error, where defined
    "e1_digest_mismatch": "1",
    "e3_unterminated": "0",
    "e4_unsupported_cte": "1",
    "e5_invalid_encoding": "1",
}

# --------------------------------------------------------------------------
# partial mail: first batch legal, second has a bad line, third legal
# --------------------------------------------------------------------------
P_SECRET_NAME = "SECRETNAME"
P_SECRET_MEMO = "SECRETMEMO"
PB1_ROWS = [
    {"study": "mp", "person": "pp-1", "observation": "po1", "edition": 1,
     "display_name": "P One", "measure": 1, "memo": ""},
    {"study": "mp", "person": "pp-2", "observation": "po2", "edition": 1,
     "display_name": "P Two", "measure": 2, "memo": ""},
]
PB2_ROWS = [
    {"study": "mp", "person": "pp-3", "observation": "po3", "edition": 1,
     "display_name": "P Three", "measure": 3, "memo": ""},
    {"study": "mp", "person": "pp-4", "observation": "po4", "edition": 0,
     "display_name": P_SECRET_NAME, "measure": 4, "memo": P_SECRET_MEMO},
    {"study": "mp", "person": "pp-5", "observation": "po5", "edition": 1,
     "display_name": "P Five", "measure": 5, "memo": ""},
]
PB3_ROWS = [
    {"study": "mp", "person": "pp-6", "observation": "po6", "edition": 1,
     "display_name": "P Six", "measure": 6, "memo": ""},
    {"study": "mp", "person": "pp-7", "observation": "po7", "edition": 1,
     "display_name": "P Seven", "measure": 7, "memo": ""},
]
PB1_PAYLOAD = jl(PB1_ROWS, "\n", trailing=True)
PB2_PAYLOAD = jl(PB2_ROWS, "\n", trailing=True)
PB3_PAYLOAD = jl(PB3_ROWS, "\n", trailing=True)
PARTIAL_BATCHES = [("pb1", "pd-1@rvault", sha(PB1_PAYLOAD)),
                   ("pb2", "pd-2@rvault", sha(PB2_PAYLOAD)),
                   ("pb3", "pd-3@rvault", sha(PB3_PAYLOAD))]
PARTIAL_EDITIONS = [
    ("mp", "po1", 1, "pp-1", "P One", 1, "", "pb1"),
    ("mp", "po2", 1, "pp-2", "P Two", 2, "", "pb1"),
    ("mp", "po6", 1, "pp-6", "P Six", 6, "", "pb3"),
    ("mp", "po7", 1, "pp-7", "P Seven", 7, "", "pb3"),
]
PARTIAL_EVENTS = [
    (1, 1, "refresh", "mp", "po1"),
    (2, 1, "refresh", "mp", "po2"),
    (3, 2, "refresh", "mp", "po6"),
    (4, 2, "refresh", "mp", "po7"),
]
PARTIAL_STATUSES = ["accepted", "rejected", "accepted"]

# --------------------------------------------------------------------------
# crash mail: killed after batch c1 commits but before its record update
# --------------------------------------------------------------------------
C1_ROWS = [{"study": "mc", "person": "pc-1", "observation": "k1",
            "edition": 1, "display_name": "K One", "measure": 11, "memo": ""}]
C2_ROWS = [{"study": "mc", "person": "pc-2", "observation": "k2",
            "edition": 1, "display_name": "K Two", "measure": 22, "memo": ""}]
C3_ROWS = [{"study": "mc", "person": "pc-3", "observation": "k3",
            "edition": 1, "display_name": "K Three", "measure": 33, "memo": ""}]
C1_PAYLOAD = jl(C1_ROWS, "\n", trailing=True)
C2_PAYLOAD = jl(C2_ROWS, "\n", trailing=True)
C3_PAYLOAD = jl(C3_ROWS, "\n", trailing=True)
CRASH_BATCHES = [("c1", "cd-1@rvault", sha(C1_PAYLOAD)),
                 ("c2", "cd-2@rvault", sha(C2_PAYLOAD)),
                 ("c3", "cd-3@rvault", sha(C3_PAYLOAD))]
CRASH_EDITIONS = [
    ("mc", "k1", 1, "pc-1", "K One", 11, "", "c1"),
    ("mc", "k2", 1, "pc-2", "K Two", 22, "", "c2"),
    ("mc", "k3", 1, "pc-3", "K Three", 33, "", "c3"),
]
CRASH_EVENTS = [
    (1, 1, "refresh", "mc", "k1"),
    (2, 2, "refresh", "mc", "k2"),
    (3, 3, "refresh", "mc", "k3"),
]
# after the resume, batch c1 is re-confirmed through its credential and is
# reported as a duplicate; c2/c3 are accepted
RESUMED_STATUSES = ["duplicate", "accepted", "accepted"]

# --------------------------------------------------------------------------
# traversal-filename mail (must process normally; nothing escapes tmp/)
# --------------------------------------------------------------------------
SAFE_ROWS = [{"study": "ms", "person": "ps-1", "observation": "safe1",
              "edition": 1, "display_name": "Safe", "measure": 1, "memo": ""}]
SAFE_PAYLOAD = jl(SAFE_ROWS, "\n", trailing=True)
SAFE_BATCHES = [("safe-b1", "sd-1@rvault", sha(SAFE_PAYLOAD))]

# --------------------------------------------------------------------------
# big mail: one large base64 attachment
# --------------------------------------------------------------------------
BIG_N = 120000
BIG_STUDY = "big"
