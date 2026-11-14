"""Offline .eml intake channel (mail adapter).

What this subsystem does
------------------------
The data-receiving group gets raw exported mail files.  A message may carry
prose, inline images, one batch manifest and several transfer-encoded
research-data attachments.  This module reads an .eml file OFFLINE (no
mailbox connection, no sending, no mail UI), finds the attachments the
manifest actually points to, decodes them byte-exactly and feeds them to the
EXISTING batch intake (`intake.import_batch`).  It never opens a second
write path around the intake, batch-credential or withdrawal-barrier rules.

Message conventions
-------------------
* The manifest is a single leaf part of type
  `application/vnd.rvault-batches+json`; the whole message carries exactly
  one.  It lists, per batch: batch_tag, content_id and sha256 - the digest
  of the TRANSFER-DECODED attachment bytes.
* Data attachments are leaf parts of type `application/x-ndjson` whose
  decoded body follows the original seven-field observation protocol.
* Manifest and attachments are linked by Content-ID only - never by file
  name or part order.  Nested multipart/mixed and multipart/related are
  transparent; prose and images the manifest does not reference are
  ignored; a message/rfc822 part is opaque - the nested message is never
  scanned for another message's batches.  A JSON example inside a body
  text part is prose, not a manifest.
* Transfer encodings: base64, quoted-printable, 7bit, 8bit, binary.
  Decoding is byte-faithful (CRLF, non-ASCII text, trailing newline or
  not); the message is never rewritten as text first.  Boundary lines may
  be split across read chunks and body lines may share a prefix with the
  boundary without being delimiters.  File names are metadata only: a name
  like ../../data.jsonl never decides a host write path.

Two phases, crash-safe
----------------------
Preflight covers the WHOLE message before any business import: structure,
references, encodings and digests are all verified first, so a corrupt last
attachment cannot leave earlier ones already imported.  Decoded attachments
are spooled to the store's tmp/ directory (nowhere else) and digested while
streaming; memory stays flat for messages larger than RAM.  Temp files are
removed on normal completion and on determined rejection; files orphaned
by a crash are reclaimed by the next run's reconcile() - they are never
evidence of receipt.

A durable processing record keyed by the sha256 of the raw message
(ml_messages / ml_batches) tracks the manifest position, the real
batch_tags and the per-batch outcomes.  Import then proceeds in manifest
order, one existing intake transaction per batch (no cross-batch
transaction): a batch with bad lines is rejected whole, earlier batches
stay, later batches continue, and every batch's outcome is reported
explicitly.  Receiving the same message again is idempotent: finalized
batches are not re-imported, pending ones are.  If the process dies after
a batch committed but before its record update, the next run re-confirms
that batch through the original batch_tag credential (intake answers
'duplicate') instead of counting it again.

Diagnostics carry the message digest, part positions, batch_tags and error
categories only - never body text, display names, measurements or memos.

Part numbering: the message itself is "0"; parts of the top-level entity
are "1", "2", ...; parts of a nested multipart are dotted ("2.1", "2.2").
"""
from __future__ import annotations

import hashlib
import json
import os
import re

from . import intake as intake_mod
from .store import reconcile
from .syncpoints import sync_point
from .util import now_iso

MANIFEST_TYPE = "application/vnd.rvault-batches+json"
DATA_TYPE = "application/x-ndjson"
DEFAULT_CHUNK = 65536

_MAX_HEADERS = 64 * 1024          # one part's header block
_MAX_PADDING = 4096               # transport padding after a boundary
_MAX_MANIFEST = 16 * 1024 * 1024  # decoded manifest size
_MAX_DEPTH = 64                   # multipart nesting depth
_MAX_PARTS = 100000               # parts per message

_IDENTITY_CTE = ("7bit", "8bit", "binary")
_ALLOWED_CTE = _IDENTITY_CTE + ("base64", "quoted-printable")
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_MAX_ERRORS = 1000


# --------------------------------------------------------------------------
# streams: bounded-memory layered readers
# --------------------------------------------------------------------------
class _HashingStream:
    """Wraps the top-level file; every byte read is hashed exactly once."""

    def __init__(self, raw, digest):
        self.raw = raw
        self.digest = digest

    def read(self, n):
        data = self.raw.read(n)
        if data:
            self.digest.update(data)
        return data


class _ChainStream:
    """read(n) over a bytes prefix followed by an underlying stream."""

    def __init__(self, prefix, raw):
        self.prefix = prefix
        self.raw = raw

    def read(self, n):
        if self.prefix:
            out = self.prefix[:n]
            self.prefix = self.prefix[n:]
            if len(out) == n:
                return out
            rest = self.raw.read(n - len(out))
            return out + (rest or b"")
        return self.raw.read(n)


def _drain(stream, chunk):
    while stream.read(chunk):
        pass


# --------------------------------------------------------------------------
# boundary handling
# --------------------------------------------------------------------------
def _classify_delim(buf, j, eof):
    """Classify what follows '--boundary' (buf[j:] is right after it).

    Returns (kind, line_end) with kind 'open' | 'close' for a real delimiter
    (line_end = index just past the delimiter line), ("need_more", None)
    when more bytes are required to decide, or ("not_delim", None) when the
    bytes after the boundary text prove this is ordinary content.
    """
    n = len(buf)
    kind = "open"
    k = j
    if k < n and buf[k] == 0x2D:  # '-'
        if k + 1 < n:
            if buf[k + 1] == 0x2D:
                kind = "close"
                k += 2
            else:
                return ("not_delim", None)
        elif eof:
            return ("not_delim", None)  # '--boundary-' at end of data
        else:
            return ("need_more", None)
    # transport padding: spaces and tabs only, bounded
    pad = 0
    while k < n and buf[k] in (0x20, 0x09):
        k += 1
        pad += 1
        if pad > _MAX_PADDING:
            return ("not_delim", None)
    if k >= n:
        if eof:
            return (kind, n)  # delimiter line ends at end of data
        return ("need_more", None)
    c = buf[k]
    if c == 0x0A:
        return (kind, k + 1)
    if c == 0x0D:
        if k + 1 < n:
            if buf[k + 1] == 0x0A:
                return (kind, k + 2)
            return ("not_delim", None)
        if eof:
            return ("not_delim", None)  # bare CR at end of data
        return ("need_more", None)
    return ("not_delim", None)


class _BoundaryStream:
    """Delivers exactly one part body from a multipart stream.

    Reads ahead from the underlying stream in bounded chunks; bytes past
    the delimiter line stay buffered and are handed back to the caller
    (they belong to the parent level).  The CRLF immediately preceding a
    delimiter belongs to the delimiter, never to the payload.  Content
    lines that merely share a prefix with the boundary are passed through
    as payload.
    """

    def __init__(self, raw, boundary, chunk):
        self.raw = raw
        self.pat = b"\n--" + boundary
        self.chunk = chunk
        self.buf = bytearray()
        self.eof = False
        self.at_delim = False
        self.delim_kind = None  # 'open' | 'close' once a delimiter was found

    def _fill(self):
        if self.eof:
            return
        data = self.raw.read(self.chunk)
        if not data:
            self.eof = True
        else:
            self.buf += data

    def _scan(self):
        """Return (safe_len, delim).  safe_len = bytes releasable as payload
        now; delim = (kind, line_end) when a delimiter was found."""
        buf = self.buf
        search = 0
        while True:
            i = buf.find(self.pat, search)
            if i < 0:
                if self.eof:
                    return (len(buf), None)
                safe = len(buf) - (len(self.pat) - 1)
                if safe < 0:
                    safe = 0
                # never release a trailing CR before EOF: it may belong to
                # the CRLF that introduces a delimiter
                if safe > 0 and buf[safe - 1] == 0x0D:
                    safe -= 1
                return (safe, None)
            cls = _classify_delim(buf, i + len(self.pat), self.eof)
            if cls[0] == "need_more":
                safe = i
                if safe > 0 and buf[safe - 1] == 0x0D:
                    safe -= 1
                return (safe, None)
            if cls[0] == "not_delim":
                # no delimiter can start inside the matched needle (a
                # boundary never contains CR/LF), so continue after it
                search = i + len(self.pat)
                continue
            return (i, cls)

    def read(self, n):
        while True:
            if self.at_delim:
                return b""
            safe, delim = self._scan()
            if delim is not None:
                kind, line_end = delim
                end = safe
                if end > 0 and self.buf[end - 1] == 0x0D:
                    end -= 1
                out = bytes(self.buf[:end])
                del self.buf[:line_end]
                self.at_delim = True
                self.delim_kind = kind
                return out
            if safe > 0:
                out = bytes(self.buf[:safe]) if safe <= n else bytes(self.buf[:n])
                del self.buf[:len(out)]
                return out
            if self.eof:
                # end of data without a delimiter: the caller reports the
                # unterminated multipart (delim_kind stays None)
                self.at_delim = True
                return b""
            self._fill()


class _Unterminated(Exception):
    pass


def _first_boundary(stream, boundary, chunk):
    """Skip the preamble up to and including the first delimiter line.

    Returns (stream_after_delimiter, kind).  The first delimiter may sit at
    offset 0 of the body (no preceding CRLF).  Raises _Unterminated when no
    delimiter appears before the end of the data.
    """
    pat0 = b"--" + boundary
    pat = b"\n" + pat0
    buf = bytearray()
    eof = False
    state0 = "unknown"  # position-0 delimiter: unknown | no
    while True:
        if state0 == "unknown":
            if len(buf) < len(pat0) and not eof:
                pass  # need more bytes to compare
            elif bytes(buf[:len(pat0)]) != pat0:
                state0 = "no"
            else:
                cls = _classify_delim(buf, len(pat0), eof)
                if cls[0] == "need_more":
                    pass
                elif cls[0] == "not_delim":
                    state0 = "no"
                else:
                    kind, line_end = cls
                    del buf[:line_end]
                    return _ChainStream(bytes(buf), stream), kind
        if state0 == "no":
            i = buf.find(pat)
            if i >= 0:
                cls = _classify_delim(buf, i + len(pat), eof)
                if cls[0] == "need_more":
                    pass
                elif cls[0] == "not_delim":
                    del buf[:i + len(pat)]  # preamble is discarded anyway
                else:
                    kind, line_end = cls
                    del buf[:line_end]
                    return _ChainStream(bytes(buf), stream), kind
            else:
                if eof:
                    raise _Unterminated()
                keep = len(pat) - 1
                if len(buf) > keep:
                    del buf[:len(buf) - keep]
        if eof:
            raise _Unterminated()
        data = stream.read(chunk)
        if not data:
            eof = True
        else:
            buf += data


# --------------------------------------------------------------------------
# headers
# --------------------------------------------------------------------------
class _WalkAbort(Exception):
    """A structural error that makes the rest of the message unparsable."""

    def __init__(self, error):
        super().__init__(error.get("category", "walk_abort"))
        self.error = error


def _find_header_sep(buf):
    """Locate the blank line ending a header block.  Returns (index, seplen)
    of the separator or None.  Handles CRLF and LF line endings."""
    if buf[:2] == b"\r\n":
        return (0, 2)
    if buf[:1] == b"\n":
        return (0, 1)
    i2 = buf.find(b"\n\n")
    i3 = buf.find(b"\n\r\n")
    cands = [(i, l) for i, l in ((i2, 2), (i3, 3)) if i >= 0]
    if not cands:
        return None
    return min(cands)


def _parse_headers(block):
    """Parse a header block (latin-1, byte-preserving) into a list of
    (lowercased name, unfolded value).  Malformed lines are ignored."""
    headers = []
    cur = None
    for ln in block.decode("latin-1").split("\n"):
        ln = ln.rstrip("\r")
        if not ln:
            continue
        if ln[0] in " \t":
            if cur is not None:
                cur = (cur[0], cur[1] + ln)
                headers[-1] = cur
            continue
        if ":" in ln:
            name, val = ln.split(":", 1)
            cur = (name.strip().lower(), val.strip())
            headers.append(cur)
        else:
            cur = None
    return headers


def _read_headers(stream, chunk, pos):
    """Read one header block; returns (headers, stream_positioned_at_body)."""
    buf = bytearray()
    while True:
        sep = _find_header_sep(buf)
        if sep is not None:
            idx, seplen = sep
            headers = _parse_headers(bytes(buf[:idx]))
            rest = bytes(buf[idx + seplen:])
            return headers, _ChainStream(rest, stream)
        if len(buf) > _MAX_HEADERS:
            raise _WalkAbort({"category": "headers_too_large", "part": pos,
                              "reason": "header block exceeds %d bytes" % _MAX_HEADERS})
        data = stream.read(chunk)
        if not data:
            # end of part before a blank line: treat what we have as
            # headers with an empty body (lenient, like common parsers)
            return _parse_headers(bytes(buf)), _ChainStream(b"", stream)
        buf += data


def _header(headers, name):
    for k, v in headers:
        if k == name:
            return v
    return None


def _split_params(value):
    """Split a header value on ';' respecting quoted strings."""
    parts = []
    cur = []
    inq = False
    esc = False
    for ch in value:
        if esc:
            cur.append(ch)
            esc = False
        elif ch == "\\" and inq:
            esc = True
        elif ch == '"':
            inq = not inq
        elif ch == ";" and not inq:
            parts.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
    parts.append("".join(cur))
    return parts


def _content_type(headers):
    """Return (maintype, subtype, params); defaults to text/plain."""
    value = _header(headers, "content-type")
    if value is None:
        return ("text", "plain", {})
    segs = _split_params(value)
    maintype, _, subtype = segs[0].strip().lower().partition("/")
    params = {}
    for p in segs[1:]:
        k, eq, v = p.partition("=")
        if not eq:
            continue
        k = k.strip().lower()
        v = v.strip()
        if len(v) >= 2 and v[0] == '"' and v[-1] == '"':
            v = v[1:-1]
        if k:
            params[k] = v
    return (maintype, subtype, params)


def _cte(headers):
    value = _header(headers, "content-transfer-encoding")
    return "7bit" if value is None else value.strip().lower()


def _content_id(headers):
    value = _header(headers, "content-id")
    if value is None:
        return None
    value = value.strip()
    if len(value) >= 2 and value[0] == "<" and value[-1] == ">":
        value = value[1:-1].strip()
    return value or None


def _filename(headers, ct_params):
    """File name as METADATA only - it never decides a host write path."""
    disp = _header(headers, "content-disposition")
    if disp is not None:
        for p in _split_params(disp)[1:]:
            k, eq, v = p.partition("=")
            if eq and k.strip().lower() == "filename":
                v = v.strip()
                if len(v) >= 2 and v[0] == '"' and v[-1] == '"':
                    v = v[1:-1]
                return v
    return ct_params.get("name")


def _valid_boundary(value):
    if value is None:
        return None
    try:
        b = value.encode("latin-1")
    except UnicodeEncodeError:
        return None
    if not b or len(b) > 200 or b"\r" in b or b"\n" in b:
        return None
    return b


# --------------------------------------------------------------------------
# transfer decoders (streaming; strict about encoding violations)
# --------------------------------------------------------------------------
class _DecodeError(Exception):
    pass


class _IdentityDecoder:
    def feed(self, data):
        return data

    def finish(self):
        return b""


_B64_BAD = re.compile(rb"[^A-Za-z0-9+/= \t\r\n]")
_B64_WS = re.compile(rb"[ \t\r\n]+")
_B64_FINAL = re.compile(rb"[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=")


class _Base64Decoder:
    """Strict base64: whitespace ignored, anything else outside the alphabet
    is an error; padding must be canonical.  Quads may straddle chunks."""

    def __init__(self):
        import base64
        self._b64 = base64
        self.carry = bytearray()
        self.tail = None  # final quantum (from the first '=') once seen

    def feed(self, data):
        bad = _B64_BAD.search(data)
        if bad:
            raise _DecodeError("invalid base64 character 0x%02x" % bad.group()[0])
        cleaned = _B64_WS.sub(b"", data)
        if self.tail is not None:
            if cleaned:
                self.tail += cleaned
                if len(self.tail) > 4:
                    raise _DecodeError("data after base64 padding")
            return b""
        self.carry += cleaned
        eq = self.carry.find(b"=")
        if eq >= 0:
            gs = (eq // 4) * 4
            head = bytes(self.carry[:gs])
            self.tail = bytes(self.carry[gs:])
            if len(self.tail) > 4:
                raise _DecodeError("data after base64 padding")
            self.carry = bytearray()
            return self._b64.b64decode(head) if head else b""
        n = len(self.carry) // 4 * 4
        if n == 0:
            return b""
        out = self._b64.b64decode(bytes(self.carry[:n]))
        del self.carry[:n]
        return out

    def finish(self):
        if self.tail is None:
            if self.carry:
                raise _DecodeError("base64 data length is not a multiple of 4")
            return b""
        if len(self.tail) != 4 or not _B64_FINAL.match(self.tail):
            raise _DecodeError("invalid base64 final quantum")
        return self._b64.b64decode(self.tail)


_HEX = b"0123456789abcdefABCDEF"


class _QPDecoder:
    """Strict quoted-printable: =XX escapes and soft line breaks only; a
    bare '=' or a bad escape is an error.  Escapes may straddle chunks."""

    def __init__(self):
        self.carry = b""

    def feed(self, data):
        if self.carry:
            data = self.carry + data
            self.carry = b""
        out = bytearray()
        n = len(data)
        i = 0
        while i < n:
            j = data.find(b"=", i)
            if j < 0:
                out += data[i:]
                break
            out += data[i:j]
            if j + 1 >= n:
                self.carry = data[j:]
                break
            c1 = data[j + 1]
            if c1 == 0x0A:  # soft break: '=\n'
                i = j + 2
                continue
            if c1 == 0x0D:  # soft break: '=\r\n'
                if j + 2 >= n:
                    self.carry = data[j:]
                    break
                if data[j + 2] == 0x0A:
                    i = j + 3
                    continue
                raise _DecodeError("quoted-printable: '=' CR not followed by LF")
            if j + 2 >= n:
                self.carry = data[j:]
                break
            c2 = data[j + 2]
            if c1 in _HEX and c2 in _HEX:
                out.append(int(chr(c1) + chr(c2), 16))
                i = j + 3
                continue
            raise _DecodeError("quoted-printable: invalid '=' escape")
        return bytes(out)

    def finish(self):
        if self.carry:
            raise _DecodeError("quoted-printable: truncated escape at end of data")
        return b""


def _make_decoder(cte):
    if cte in _IDENTITY_CTE:
        return _IdentityDecoder()
    if cte == "base64":
        return _Base64Decoder()
    if cte == "quoted-printable":
        return _QPDecoder()
    raise AssertionError(cte)


# --------------------------------------------------------------------------
# preflight: streaming parse + decode + digest of the whole message
# --------------------------------------------------------------------------
class _Preflight:
    def __init__(self, store, chunk):
        self.store = store
        self.chunk = chunk
        self.parts = []      # every part (containers and leaves), in walk order
        self.errors = []
        self.decoded = {}    # pos -> {ok, path, digest, size} for manifest/data leaves
        self.tmp_files = []
        self.counter = 0
        self.manifest_part = None
        self.walk_aborted = False

    # -- parts ---------------------------------------------------------
    def add_part(self, pos, ctype, cte, cid, filename, container):
        if len(self.parts) >= _MAX_PARTS:
            raise _WalkAbort({"category": "too_many_parts",
                              "reason": "more than %d parts" % _MAX_PARTS})
        if container:
            role = "container"
        elif ctype == MANIFEST_TYPE:
            role = "manifest"
        elif ctype == DATA_TYPE:
            role = "data"
        else:
            role = "other"
        self.parts.append({"pos": pos, "ctype": ctype, "cte": cte, "cid": cid,
                           "filename": filename, "container": container,
                           "role": role})

    def handle_leaf(self, stream, pos):
        """Decode a manifest/data leaf to a managed temp file while
        digesting; drain anything else (prose, images, message/rfc822)."""
        part = self.parts[-1]
        if part["role"] == "other":
            _drain(stream, self.chunk)
            return
        cte = part["cte"]
        if cte not in _ALLOWED_CTE:
            self.errors.append({"category": "unsupported_transfer_encoding",
                                "part": pos, "declared": cte})
            self.decoded[pos] = {"ok": False, "path": None, "digest": None, "size": 0}
            _drain(stream, self.chunk)
            return
        dec = _make_decoder(cte)
        self.counter += 1
        tmp = os.path.join(self.store.tmp_dir,
                           "mail.%d.%d.part" % (os.getpid(), self.counter))
        self.tmp_files.append(tmp)
        digest = hashlib.sha256()
        size = 0
        ok = True
        try:
            with open(tmp, "wb") as out:
                while True:
                    data = stream.read(self.chunk)
                    if not data:
                        break
                    piece = dec.feed(data)
                    if piece:
                        out.write(piece)
                        digest.update(piece)
                        size += len(piece)
                tail = dec.finish()
                if tail:
                    out.write(tail)
                    digest.update(tail)
                    size += len(tail)
        except _DecodeError as e:
            ok = False
            self.errors.append({"category": "invalid_transfer_encoding",
                                "part": pos, "reason": str(e)})
            _drain(stream, self.chunk)
        self.decoded[pos] = {"ok": ok, "path": tmp,
                             "digest": digest.hexdigest() if ok else None,
                             "size": size}

    # -- whole-message walk ---------------------------------------------
    def run(self, path):
        """Parse and decode the message; returns the raw-message digest.
        Every byte of the file is hashed exactly once, even when the walk
        aborts early."""
        digest = hashlib.sha256()
        with open(path, "rb") as f:
            hashing = _HashingStream(f, digest)
            try:
                headers, stream = _read_headers(hashing, self.chunk, "0")
                after = _handle_entity(stream, headers, "", self, 0)
                _drain(after, self.chunk)
            except _WalkAbort as e:
                self.errors.append(e.error)
                self.walk_aborted = True
            _drain(hashing, self.chunk)
        return digest.hexdigest()

    # -- manifest + structural checks ------------------------------------
    def _parse_manifest(self, mpart):
        d = self.decoded.get(mpart["pos"])
        if d is None or not d["ok"]:
            return None  # the decode error is already recorded
        if d["size"] > _MAX_MANIFEST:
            self.errors.append({"category": "manifest_too_large",
                                "part": mpart["pos"],
                                "reason": "decoded manifest exceeds %d bytes"
                                          % _MAX_MANIFEST})
            return None
        with open(d["path"], "rb") as f:
            raw = f.read()
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            self.errors.append({"category": "manifest_invalid",
                                "part": mpart["pos"],
                                "reason": "manifest is not valid UTF-8"})
            return None
        try:
            doc = json.loads(text)
        except json.JSONDecodeError as e:
            self.errors.append({"category": "manifest_invalid",
                                "part": mpart["pos"],
                                "reason": "manifest is not valid JSON: %s "
                                          "(line %d column %d)"
                                          % (e.msg, e.lineno, e.colno)})
            return None
        if not isinstance(doc, dict) or not isinstance(doc.get("batches"), list):
            self.errors.append({"category": "manifest_invalid",
                                "part": mpart["pos"],
                                "reason": "manifest must be an object with a "
                                          "'batches' list"})
            return None
        entries = []
        for i, item in enumerate(doc["batches"], 1):
            bad = None
            if not isinstance(item, dict) or set(item.keys()) != {
                    "batch_tag", "content_id", "sha256"}:
                bad = "entry %d must be an object with exactly batch_tag, " \
                      "content_id and sha256" % i
            elif not isinstance(item["batch_tag"], str) or not item["batch_tag"]:
                bad = "entry %d: batch_tag must be a non-empty string" % i
            elif not isinstance(item["content_id"], str) or not item["content_id"]:
                bad = "entry %d: content_id must be a non-empty string" % i
            elif not isinstance(item["sha256"], str) \
                    or not _SHA256_RE.match(item["sha256"]):
                bad = "entry %d: sha256 must be 64 lowercase hex characters" % i
            if bad is not None:
                self.errors.append({"category": "manifest_invalid",
                                    "part": mpart["pos"], "reason": bad})
                return None
            entries.append({"batch_tag": item["batch_tag"],
                            "content_id": item["content_id"],
                            "sha256": item["sha256"]})
        return entries

    def checks(self):
        """Structural, reference and digest checks over the walked message.
        Returns the resolved manifest entries (valid only when no errors)."""
        bycid = {}
        for p in self.parts:
            if p["cid"] is not None:
                bycid.setdefault(p["cid"], []).append(p["pos"])
        for cid in sorted(bycid):
            if len(bycid[cid]) > 1:
                self.errors.append({"category": "content_id_duplicate",
                                    "content_id": cid, "parts": bycid[cid]})
        mans = [p for p in self.parts if p["role"] == "manifest"]
        entries = None
        if not mans:
            self.errors.append({"category": "manifest_missing",
                                "reason": "no part of type " + MANIFEST_TYPE})
        elif len(mans) > 1:
            self.errors.append({"category": "manifest_duplicate",
                                "parts": [m["pos"] for m in mans]})
        else:
            self.manifest_part = mans[0]["pos"]
            entries = self._parse_manifest(mans[0])
        resolved = []
        if entries is not None:
            seen = {}
            for i, e in enumerate(entries, 1):
                tag = e["batch_tag"]
                if tag in seen:
                    self.errors.append({"category": "batch_tag_duplicate",
                                        "batch_tag": tag,
                                        "entries": [seen[tag], i]})
                else:
                    seen[tag] = i
            for i, e in enumerate(entries, 1):
                cid = e["content_id"]
                matches = [p for p in self.parts if p["cid"] == cid]
                if not matches:
                    self.errors.append({"category": "reference_unresolved",
                                        "seq": i, "batch_tag": e["batch_tag"],
                                        "content_id": cid})
                    continue
                if len(matches) > 1:
                    self.errors.append({"category": "reference_ambiguous",
                                        "seq": i, "batch_tag": e["batch_tag"],
                                        "content_id": cid,
                                        "parts": [m["pos"] for m in matches]})
                    continue
                p = matches[0]
                if p["ctype"] != DATA_TYPE:
                    self.errors.append({"category": "referenced_part_not_data",
                                        "seq": i, "batch_tag": e["batch_tag"],
                                        "part": p["pos"],
                                        "content_type": p["ctype"]})
                    continue
                d = self.decoded.get(p["pos"])
                if d is None or not d["ok"]:
                    continue  # the decode error is already recorded
                if d["digest"] != e["sha256"]:
                    self.errors.append({"category": "digest_mismatch",
                                        "seq": i, "batch_tag": e["batch_tag"],
                                        "part": p["pos"],
                                        "declared": e["sha256"],
                                        "actual": d["digest"]})
                    continue
                resolved.append({"seq": i, "batch_tag": e["batch_tag"],
                                 "content_id": cid, "sha256": e["sha256"],
                                 "part": p["pos"], "path": d["path"]})
        return resolved

    def cleanup(self):
        for p in self.tmp_files:
            try:
                os.remove(p)
            except FileNotFoundError:
                pass
        self.tmp_files = []


def _handle_entity(stream, headers, pos, pf, depth):
    """Handle one entity (leaf or multipart); returns the stream positioned
    after it.  pos '' means the message root."""
    if depth > _MAX_DEPTH:
        raise _WalkAbort({"category": "structure_too_deep", "part": pos or "0"})
    maintype, subtype, params = _content_type(headers)
    ctype = "%s/%s" % (maintype, subtype)
    cid = _content_id(headers)
    if maintype == "multipart":
        if pos:
            pf.add_part(pos, ctype, None, cid, None, container=True)
        boundary = _valid_boundary(params.get("boundary"))
        if boundary is None:
            raise _WalkAbort({"category": "invalid_multipart_boundary",
                              "part": pos or "0",
                              "reason": "multipart part without a usable "
                                        "boundary parameter"})
        return _parse_multipart(stream, boundary, pos, pf, depth)
    pf.add_part(pos or "1", ctype, _cte(headers), cid,
                _filename(headers, params), container=False)
    pf.handle_leaf(stream, pos or "1")
    return stream


def _parse_multipart(stream, boundary, pos, pf, depth):
    """Parse one multipart body; returns the stream positioned after the
    closing delimiter line (the parent continues from there)."""
    try:
        stream, kind = _first_boundary(stream, boundary, pf.chunk)
    except _Unterminated:
        raise _WalkAbort({"category": "unterminated_multipart",
                          "part": pos or "0",
                          "reason": "no boundary delimiter found"})
    index = 0
    while kind == "open":
        index += 1
        child = "%s.%d" % (pos, index) if pos else str(index)
        bs = _BoundaryStream(stream, boundary, pf.chunk)
        headers, body = _read_headers(bs, pf.chunk, child)
        after = _handle_entity(body, headers, child, pf, depth + 1)
        _drain(after, pf.chunk)
        if bs.delim_kind is None:
            raise _WalkAbort({"category": "unterminated_multipart",
                              "part": pos or "0",
                              "reason": "end of data before the closing "
                                        "boundary"})
        kind = bs.delim_kind
        stream = _ChainStream(bytes(bs.buf), stream)
    return stream


# --------------------------------------------------------------------------
# durable processing record
# --------------------------------------------------------------------------
def _get_message(conn, digest):
    return conn.execute("SELECT * FROM ml_messages WHERE message_digest=?",
                        (digest,)).fetchone()


def _record_rejected(store, digest, pf, batch_count):
    conn = store.connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO ml_messages(message_digest, state, received_at, "
            "updated_at, manifest_part, part_count, batch_count, errors_json) "
            "VALUES (?,?,?,?,?,?,?,?) "
            "ON CONFLICT(message_digest) DO UPDATE SET state=excluded.state, "
            "updated_at=excluded.updated_at, "
            "manifest_part=excluded.manifest_part, "
            "part_count=excluded.part_count, "
            "batch_count=excluded.batch_count, "
            "errors_json=excluded.errors_json",
            (digest, "rejected", now_iso(), now_iso(), pf.manifest_part,
             len(pf.parts), batch_count,
             json.dumps(pf.errors, ensure_ascii=False)))
        conn.execute("DELETE FROM ml_batches WHERE message_digest=?", (digest,))
        conn.execute("COMMIT")
    finally:
        conn.close()


def _record_importing(store, digest, pf, entries):
    conn = store.connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO ml_messages(message_digest, state, received_at, "
            "updated_at, manifest_part, part_count, batch_count, errors_json) "
            "VALUES (?,?,?,?,?,?,?,NULL) "
            "ON CONFLICT(message_digest) DO UPDATE SET state=excluded.state, "
            "updated_at=excluded.updated_at, "
            "manifest_part=excluded.manifest_part, "
            "part_count=excluded.part_count, "
            "batch_count=excluded.batch_count, errors_json=NULL",
            (digest, "importing", now_iso(), now_iso(), pf.manifest_part,
             len(pf.parts), len(entries)))
        for e in entries:
            # Metadata is refreshed; an existing per-batch outcome (from an
            # interrupted run) is preserved, never reset.
            conn.execute(
                "INSERT INTO ml_batches(message_digest, seq, batch_tag, "
                "content_id, part, sha256, status, updated_at) "
                "VALUES (?,?,?,?,?,?,'pending',?) "
                "ON CONFLICT(message_digest, seq) DO UPDATE SET "
                "batch_tag=excluded.batch_tag, content_id=excluded.content_id, "
                "part=excluded.part, sha256=excluded.sha256",
                (digest, e["seq"], e["batch_tag"], e["content_id"], e["part"],
                 e["sha256"], now_iso()))
        conn.execute("COMMIT")
    finally:
        conn.close()


def _record_batch(store, digest, seq, status, detail):
    conn = store.connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "UPDATE ml_batches SET status=?, detail_json=?, updated_at=? "
            "WHERE message_digest=? AND seq=?",
            (status, json.dumps(detail, ensure_ascii=False, sort_keys=True),
             now_iso(), digest, seq))
        conn.execute("COMMIT")
    finally:
        conn.close()


def _record_done(store, digest):
    conn = store.connect()
    try:
        counts = {"accepted": 0, "duplicate": 0, "rejected": 0, "pending": 0}
        for r in conn.execute(
                "SELECT status, COUNT(*) AS c FROM ml_batches "
                "WHERE message_digest=? GROUP BY status", (digest,)):
            counts[r["status"]] = int(r["c"])
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "UPDATE ml_messages SET state='done', accepted_count=?, "
            "duplicate_count=?, rejected_count=?, updated_at=? "
            "WHERE message_digest=?",
            (counts["accepted"], counts["duplicate"], counts["rejected"],
             now_iso(), digest))
        conn.execute("COMMIT")
    finally:
        conn.close()


def _message_summary(store, digest, replayed):
    """Build the operator summary from the durable record; the exit code is
    2 whenever any batch was rejected, 0 otherwise."""
    conn = store.connect()
    try:
        msg = _get_message(conn, digest)
        batches = []
        any_rejected = False
        for r in conn.execute(
                "SELECT * FROM ml_batches WHERE message_digest=? "
                "ORDER BY seq", (digest,)):
            entry = {"seq": int(r["seq"]), "batch_tag": r["batch_tag"],
                     "content_id": r["content_id"], "part": r["part"],
                     "sha256": r["sha256"], "status": r["status"]}
            if r["detail_json"]:
                detail = json.loads(r["detail_json"])
                for k in ("accepted", "accepted_existing",
                          "withdrawn_suppressed", "errors", "receipt"):
                    if k in detail:
                        entry[k] = detail[k]
            if r["status"] == "rejected":
                any_rejected = True
            batches.append(entry)
        errors = json.loads(msg["errors_json"]) if msg["errors_json"] else []
    finally:
        conn.close()
    out = {"message_digest": digest,
           "state": msg["state"],
           "status": ("rejected" if msg["state"] == "rejected"
                      else "partial" if any_rejected else "received"),
           "manifest_part": msg["manifest_part"],
           "part_count": int(msg["part_count"]),
           "batches": batches}
    if errors:
        out["errors"] = errors
    if replayed:
        out["note"] = "message already fully processed; recorded outcome " \
                      "replayed without re-importing"
    code = 0 if out["status"] == "received" else 2
    return code, out


# --------------------------------------------------------------------------
# public entry points
# --------------------------------------------------------------------------
def _hash_file(path, chunk):
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            data = f.read(max(chunk, 1 << 20))
            if not data:
                break
            digest.update(data)
    return digest.hexdigest()


def receive(store, path, chunk_size=DEFAULT_CHUNK, require_existing=False):
    """Receive one .eml file: preflight the whole message, then import the
    manifest's batches in order through the existing intake.  Idempotent
    and resumable via the durable record keyed by the raw-message digest.
    Returns (exit_code, summary)."""
    store.ensure()
    reconcile(store)
    if chunk_size < 1:
        return 2, {"status": "rejected",
                   "errors": [{"category": "invalid_argument",
                               "reason": "chunk-size must be >= 1"}]}
    try:
        digest = _hash_file(path, chunk_size)
    except OSError as e:
        return 4, {"error": "message file not readable: %s" % e.strerror,
                   "input": path}
    conn = store.connect()
    try:
        rec = _get_message(conn, digest)
    finally:
        conn.close()
    if rec is None and require_existing:
        return 4, {"error": "unknown message (no processing record); "
                            "use 'mail receive' for a first pass",
                   "message_digest": digest}
    if rec is not None and rec["state"] == "done":
        return _message_summary(store, digest, replayed=True)

    pf = _Preflight(store, chunk_size)
    try:
        parsed_digest = pf.run(path)
    except OSError as e:
        pf.cleanup()
        return 4, {"error": "message file not readable: %s" % e.strerror,
                   "input": path}
    if parsed_digest != digest:
        pf.cleanup()
        return 2, {"message_digest": digest, "status": "rejected",
                   "errors": [{"category": "message_changed",
                               "reason": "the file changed while being "
                                         "processed"}]}
    entries = [] if pf.walk_aborted else pf.checks()
    if pf.errors:
        _record_rejected(store, digest, pf, len(entries))
        errors = list(pf.errors)
        pf.cleanup()
        return 2, {"message_digest": digest, "status": "rejected",
                   "manifest_part": pf.manifest_part,
                   "part_count": len(pf.parts),
                   "errors": errors}

    _record_importing(store, digest, pf, entries)
    conn = store.connect()
    try:
        for e in entries:
            row = conn.execute(
                "SELECT status FROM ml_batches WHERE message_digest=? "
                "AND seq=?", (digest, e["seq"])).fetchone()
            if row is not None and row["status"] != "pending":
                continue  # finalized by an earlier (interrupted) run
            with open(e["path"], "rb") as f:
                code, res = intake_mod.import_batch(store, e["batch_tag"], f)
            # Crash window exercised by the demo: the batch is committed
            # (or not) by the existing intake here, but the processing
            # record has not been updated yet.
            sync_point("mail.after_batch_import.%d" % e["seq"])
            status = res.get("status", "rejected")
            if status not in ("accepted", "duplicate", "rejected"):
                status = "rejected"
            _record_batch(store, digest, e["seq"], status, res)
        _record_done(store, digest)
    finally:
        conn.close()
        pf.cleanup()
    return _message_summary(store, digest, replayed=False)


def status(store, message_digest=None):
    """Processing records: one message, or every message when no digest is
    given.  Identifiers and categories only - never payload."""
    conn = store.connect()
    try:
        if message_digest is not None:
            msg = _get_message(conn, message_digest)
            if msg is None:
                return None
            batches = []
            for r in conn.execute(
                    "SELECT * FROM ml_batches WHERE message_digest=? "
                    "ORDER BY seq", (message_digest,)):
                entry = {"seq": int(r["seq"]), "batch_tag": r["batch_tag"],
                         "content_id": r["content_id"], "part": r["part"],
                         "sha256": r["sha256"], "status": r["status"]}
                if r["detail_json"]:
                    detail = json.loads(r["detail_json"])
                    for k in ("accepted", "accepted_existing",
                              "withdrawn_suppressed", "errors", "receipt"):
                        if k in detail:
                            entry[k] = detail[k]
                batches.append(entry)
            return {"message_digest": message_digest,
                    "state": msg["state"],
                    "manifest_part": msg["manifest_part"],
                    "part_count": int(msg["part_count"]),
                    "batch_count": int(msg["batch_count"]),
                    "received_at": msg["received_at"],
                    "updated_at": msg["updated_at"],
                    "batches": batches,
                    "errors": (json.loads(msg["errors_json"])
                               if msg["errors_json"] else [])}
        messages = []
        for r in conn.execute(
                "SELECT * FROM ml_messages ORDER BY received_at, message_digest"):
            messages.append({"message_digest": r["message_digest"],
                             "state": r["state"],
                             "batch_count": int(r["batch_count"]),
                             "accepted": int(r["accepted_count"]),
                             "duplicate": int(r["duplicate_count"]),
                             "rejected": int(r["rejected_count"]),
                             "updated_at": r["updated_at"]})
        return {"messages": messages}
    finally:
        conn.close()
