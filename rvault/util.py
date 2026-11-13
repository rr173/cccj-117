"""Small shared helpers."""
from __future__ import annotations

import datetime
import hashlib
import json


def now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def canonical_bytes(obj) -> bytes:
    """Deterministic JSON encoding used for bundle files."""
    return (json.dumps(obj, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode("utf-8")


def safe_name(s: str) -> str:
    """Filesystem-safe rendering of an external identifier."""
    out = []
    for ch in s:
        out.append(ch if (ch.isalnum() or ch in "._-") else "_")
    return "".join(out) or "_"
