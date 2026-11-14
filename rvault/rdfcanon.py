"""Offline RDF dataset canonicalization and dataset-identity checking.

Implements W3C RDF Dataset Canonicalization 1.0 (RDFC-1.0) with SHA-256, a
strict RDF 1.1 N-Quads parser, and a byte-level dataset comparison.  This
module is independent of the vault: it never opens the store, never touches
revision lineage, and offers no reasoning or query service.

Algorithm map against https://www.w3.org/TR/rdf-canon/:

  §4.4.3  Canonicalization Algorithm      Canonicalizer.canonicalize()
  §4.5.2  Issue Identifier Algorithm      _IdentifierIssuer.issue()
  §4.6.3  Hash First Degree Quads         Canonicalizer._hash_first_degree_quads()
  §4.7.3  Hash Related Blank Node         Canonicalizer._hash_related()
  §4.8.3  Hash N-Degree Quads             Canonicalizer._hash_n_degree_quads()
  §5      Serialization                   _serialize_quad() + code point sort + LF
  App. A  Canonical N-Quads form          _escape_literal(), _escape_iri()

The resource budget (``max_work``) is the denial-of-service mitigation
suggested by §7.1: one unit is charged whenever 4.8 Hash N-Degree Quads is
entered, and whenever a candidate permutation of step 5.4 begins to be
considered (including permutations later pruned).  A counter is never reset on
recursion or when moving to another starting node, and the same counter can be
shared by both sides of a two-file comparison.
"""
from __future__ import annotations

import hashlib
import itertools
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

DEFAULT_MAX_WORK = 100000

_HEX = set("0123456789abcdefABCDEF")
_WS = " \t"
_ECHAR = {"t": "\t", "b": "\b", "n": "\n", "r": "\r", "f": "\f",
          '"': '"', "'": "'", "\\": "\\"}
# Characters that may never appear raw inside IRIREF (N-Quads §4, production
# [10]): controls through U+0020 plus <>"{}|^` and backslash.
_IRI_FORBIDDEN = set('<>"{}|^`\\') | {chr(c) for c in range(0x21)}


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------

class CanonError(Exception):
    """Base class; every error carries a machine-readable ``code``."""

    code = "canon_error"
    status = 3  # never 0 (success) or 1 (different)

    def __init__(self, message, **details):
        super().__init__(message)
        self.message = message
        self.details = details

    def as_dict(self) -> dict:
        d = {"code": self.code, "error": self.message}
        d.update(self.details)
        return d


class NQuadsSyntaxError(CanonError):
    """Malformed N-Quads; always reports 1-based line and column."""

    code = "syntax_error"
    status = 2          # input rejected (matches the intake exit-code family)

    def __init__(self, message, line, column, line_text=None):
        super().__init__(message, line=line, column=column, text=line_text)
        self.line = line
        self.column = column


class ResourceLimitError(CanonError):
    """The max_work budget ran out before canonicalization could complete."""

    code = "resource_limit"

    def __init__(self, max_work, used, phase="hash_n_degree_quads"):
        super().__init__(
            "work budget exhausted: the next step would exceed max_work=%d"
            % max_work, max_work=max_work, used=used, phase=phase)
        self.max_work = max_work
        self.used = used


class WriteError(CanonError):
    code = "output_error"


# ---------------------------------------------------------------------------
# Work budget
# ---------------------------------------------------------------------------

class WorkBudget:
    """Monotone counter shared by every canonicalization in one invocation."""

    __slots__ = ("max_work", "used")

    def __init__(self, max_work: int = DEFAULT_MAX_WORK):
        if isinstance(max_work, bool) or not isinstance(max_work, int) \
                or max_work <= 0:
            raise ValueError("max_work must be a positive integer")
        self.max_work = max_work
        self.used = 0

    def charge(self, phase: str) -> None:
        if self.used >= self.max_work:
            raise ResourceLimitError(self.max_work, self.used, phase)
        self.used += 1


# ---------------------------------------------------------------------------
# N-Quads terms
#
# A term is an immutable tuple tagged by kind.  Terms are kept exactly as
# read: no Unicode normalization, case folding, IRI resolution or datatype
# value coercion.  The language tag alone is lowercased at read time (RDF 1.1
# language tags are case-insensitive).  xsd:string typed literals are folded
# into simple literals because RDF 1.1 defines them as the same term and
# canonical N-Quads forbids the redundant datatype IRI.
# ---------------------------------------------------------------------------

TAG_IRI = "I"
TAG_BNODE = "B"
TAG_LITERAL = "L"

LIT_PLAIN = "plain"        # xsd:string, serialized without ^^
LIT_LANG = "lang"          # rdf:langString with a language tag
LIT_TYPED = "typed"        # explicit datatype IRI

XSD_STRING = "http://www.w3.org/2001/XMLSchema#string"

GlobalTerm = tuple
Quad = Tuple[tuple, tuple, tuple, Optional[tuple]]


def iri(value: str) -> tuple:
    return (TAG_IRI, value)


def bnode(label: str) -> tuple:
    return (TAG_BNODE, label)


def literal(lexical: str, kind: str = LIT_PLAIN, value: str = "") -> tuple:
    return (TAG_LITERAL, kind, lexical, value)


def is_bnode(term) -> bool:
    return term is not None and term[0] == TAG_BNODE


# ---------------------------------------------------------------------------
# Character classes from N-Quads §4 productions [157s], [158s], [160s]
# ---------------------------------------------------------------------------

def _pn_chars_base(ch: str) -> bool:
    o = ord(ch)
    return (ch.isascii() and ch.isalpha()) or \
        0x00C0 <= o <= 0x00D6 or 0x00D8 <= o <= 0x00F6 or \
        0x00F8 <= o <= 0x02FF or 0x0370 <= o <= 0x037D or \
        0x037F <= o <= 0x1FFF or 0x200C <= o <= 0x200D or \
        0x2070 <= o <= 0x218F or 0x2C00 <= o <= 0x2FEF or \
        0x3001 <= o <= 0xD7FF or 0xF900 <= o <= 0xFDCF or \
        0xFDF0 <= o <= 0xFFFD or 0x10000 <= o <= 0xEFFFF


def _pn_chars_u(ch: str) -> bool:
    return _pn_chars_base(ch) or ch == "_" or ch == ":"


def _pn_chars(ch: str) -> bool:
    o = ord(ch)
    return (_pn_chars_u(ch) or ch == "-" or ("0" <= ch <= "9") or o == 0x00B7
            or 0x0300 <= o <= 0x036F or 0x203F <= o <= 0x2040)


def _is_surrogate(o: int) -> bool:
    return 0xD800 <= o <= 0xDFFF


def _check_codepoint(value: int, line: int, column: int) -> str:
    """Map a numeric escape to a character, rejecting invalid scalar values."""
    if value > 0x10FFFF:
        raise NQuadsSyntaxError(
            "escape sequence U+%04X is outside the Unicode range "
            "U+0000..U+10FFFF" % value, line, column)
    if _is_surrogate(value):
        raise NQuadsSyntaxError(
            "escape sequence U+%04X is a lone surrogate, not a Unicode "
            "scalar value" % value, line, column)
    return chr(value)


# ---------------------------------------------------------------------------
# Strict N-Quads parser
# ---------------------------------------------------------------------------

@dataclass
class ParsedDataset:
    quads: List[Quad]             # deduplicated (set semantics), input order
    blank_labels: List[str]       # original labels in first-seen order
    occurrences: Dict[Quad, int]  # occurrences per unique quad
    raw_occurrences: int

    @property
    def duplicate_occurrences(self) -> int:
        return self.raw_occurrences - len(self.quads)


class _Parser:
    """Line-oriented strict N-Quads 1.1 parser.

    * Comments start with '#' outside IRIREF/STRING_LITERAL_QUOTE and run to
      (but not including) the line break; they count as white space.
    * Statements may not span lines.  Blank or comment-only lines between
      statements are accepted.
    * Escapes follow Turtle §6.4: numeric \\uXXXX / \\UXXXXXXXX in IRIs and
      strings, ECHAR string escapes in strings only.
    * After the final '.' only white space, a comment or a line break may
      follow; anything else is trailing garbage.
    """

    def __init__(self, text: str, source: str = "<input>"):
        self.text = text
        self.pos = 0
        self.line = 1
        self.line_start = 0
        self.n = len(text)
        self.source = source
        self.bnode_labels: set = set()
        self.bnode_order: List[str] = []
        self.quads: Dict[Quad, int] = {}
        self.order: List[Quad] = []

    # --- diagnostics ----------------------------------------------------

    def _line_text(self, line=None) -> str:
        line = self.line if line is None else line
        start = self.line_start
        end = self.text.find("\n", start)
        if end == -1:
            end = self.n
        return self.text[start:end].rstrip("\r")

    def _err(self, msg, line=None, column=None):
        line = self.line if line is None else line
        if column is None:
            column = self.pos - self.line_start + 1
        raise NQuadsSyntaxError(msg, line, column, self._line_text(line))

    def _pos(self):
        return self.line, self.pos - self.line_start + 1

    # --- whitespace / comments / EOL ------------------------------------

    def _peek(self) -> str:
        return self.text[self.pos] if self.pos < self.n else ""

    def _eof(self) -> bool:
        return self.pos >= self.n

    def _skip_ws(self) -> bool:
        start = self.pos
        while self.pos < self.n and self.text[self.pos] in _WS:
            self.pos += 1
        return self.pos != start

    def _skip_comment(self) -> bool:
        if self._peek() != "#":
            return False
        while self.pos < self.n and self.text[self.pos] not in "\r\n":
            self.pos += 1
        return True

    def _skip_ws_comments(self) -> bool:
        moved = False
        while True:
            step = self._skip_ws()
            if self._skip_comment():
                step = True
            if not step:
                return moved
            moved = True

    def _consume_eol(self) -> bool:
        """Consume a CR/LF run, updating the line counter."""
        if self.pos >= self.n or self.text[self.pos] not in "\r\n":
            return False
        while self.pos < self.n and self.text[self.pos] in "\r\n":
            if self.text[self.pos] == "\n":
                self.line += 1
                self.line_start = self.pos + 1
            self.pos += 1
        return True

    def _skip_separators(self) -> None:
        while True:
            self._skip_ws_comments()
            if not self._consume_eol():
                return

    # --- lexical helpers ------------------------------------------------

    def _take_hex(self, count: int, esc_line: int, esc_col: int) -> str:
        digits = self.text[self.pos:self.pos + count]
        if len(digits) != count or any(c not in _HEX for c in digits):
            raise NQuadsSyntaxError(
                "malformed escape: expected %d hexadecimal digits" % count,
                esc_line, esc_col, self._line_text(esc_line))
        self.pos += count
        return digits

    def _raw_char_ok(self, ch: str, line: int, col: int) -> None:
        if _is_surrogate(ord(ch)):
            raise NQuadsSyntaxError(
                "lone surrogate U+%04X is not a Unicode scalar value"
                % ord(ch), line, col, self._line_text(line))

    # --- productions ----------------------------------------------------

    def _term(self):
        c = self._peek()
        if c == "<":
            return self._iri()
        if c == '"':
            return self._literal()
        if c == "_" and self.text.startswith("_:", self.pos):
            return self._bnode()
        self._err("expected an IRI, blank node label or literal")

    def _iri(self) -> tuple:
        start_line, start_col = self._pos()
        self.pos += 1  # '<'
        out: List[str] = []
        while True:
            if self._eof():
                self._err("unterminated IRI", start_line, start_col)
            c = self.text[self.pos]
            if c == ">":
                self.pos += 1
                break
            if c == "\\":
                esc_line, esc_col = self._pos()
                if self.pos + 1 >= self.n:
                    self._err("trailing backslash in IRI", esc_line, esc_col)
                kind = self.text[self.pos + 1]
                if kind == "u":
                    self.pos += 2
                    out.append(_check_codepoint(
                        int(self._take_hex(4, esc_line, esc_col), 16),
                        esc_line, esc_col))
                elif kind == "U":
                    self.pos += 2
                    out.append(_check_codepoint(
                        int(self._take_hex(8, esc_line, esc_col), 16),
                        esc_line, esc_col))
                else:
                    raise NQuadsSyntaxError(
                        "only numeric escapes are allowed inside an IRI; "
                        "'\\%s' is not legal" % kind,
                        esc_line, esc_col, self._line_text(esc_line))
                continue
            if c in "\r\n":
                self._err("raw line break inside an IRI", start_line, start_col)
            if c in _IRI_FORBIDDEN:
                self._err("character %r is not allowed in an IRIREF" % c)
            self._raw_char_ok(c, *self._pos())
            out.append(c)
            self.pos += 1
        value = "".join(out)
        self._require_absolute_iri(value, start_line, start_col)
        return (TAG_IRI, value)

    def _require_absolute_iri(self, value: str, line: int, col: int) -> None:
        scheme, sep, _rest = value.partition(":")
        ok = bool(sep) and bool(scheme) and scheme[0].isascii() \
            and scheme[0].isalpha() and all(
                ch.isascii() and (ch.isalnum() or ch in "+-.")
                for ch in scheme)
        if not ok:
            raise NQuadsSyntaxError(
                "IRI <%s> is not absolute (N-Quads requires a scheme; "
                "relative IRIs are not resolved)" % value,
                line, col, self._line_text(line))

    def _bnode(self) -> tuple:
        start_line, start_col = self._pos()
        self.pos += 2  # '_:'
        if self._eof():
            self._err("blank node label ends after '_:'", start_line, start_col)
        c = self._peek()
        if not (c.isdigit() or _pn_chars_u(c)):
            self._err("blank node label must start with PN_CHARS_U or a digit")
        out = [c]
        self.pos += 1
        while not self._eof():
            c = self.text[self.pos]
            if _pn_chars(c):
                out.append(c)
                self.pos += 1
            elif c == ".":
                # '.' is allowed only when followed by another label char;
                # a final '.' is the statement terminator.
                nxt = self.text[self.pos + 1] if self.pos + 1 < self.n else ""
                if nxt and (_pn_chars(nxt) or nxt == "."):
                    out.append(c)
                    self.pos += 1
                else:
                    break
            else:
                break
        label = "".join(out)
        if label.endswith("."):
            self._err("blank node label may not end with '.'",
                      start_line, start_col)
        if label not in self.bnode_labels:
            self.bnode_labels.add(label)
            self.bnode_order.append(label)
        return (TAG_BNODE, label)

    def _literal(self) -> tuple:
        start_line, start_col = self._pos()
        self.pos += 1  # opening quote
        out: List[str] = []
        while True:
            if self._eof():
                self._err("unterminated string literal", start_line, start_col)
            c = self.text[self.pos]
            if c == '"':
                self.pos += 1
                break
            if c in "\r\n":
                self._err("raw line break inside a string literal "
                          "(use \\n or \\r)", start_line, start_col)
            if c == "\\":
                esc_line, esc_col = self._pos()
                if self.pos + 1 >= self.n:
                    self._err("trailing backslash in string literal",
                              esc_line, esc_col)
                kind = self.text[self.pos + 1]
                if kind in _ECHAR:
                    out.append(_ECHAR[kind])
                    self.pos += 2
                elif kind == "u":
                    self.pos += 2
                    out.append(_check_codepoint(
                        int(self._take_hex(4, esc_line, esc_col), 16),
                        esc_line, esc_col))
                elif kind == "U":
                    self.pos += 2
                    out.append(_check_codepoint(
                        int(self._take_hex(8, esc_line, esc_col), 16),
                        esc_line, esc_col))
                else:
                    raise NQuadsSyntaxError(
                        "invalid escape '\\%s' in string literal" % kind,
                        esc_line, esc_col, self._line_text(esc_line))
                continue
            self._raw_char_ok(c, *self._pos())
            out.append(c)
            self.pos += 1
        lexical = "".join(out)
        if self._peek() == "@":
            self.pos += 1
            sub_line, sub_col = self._pos()
            tag_start = self.pos
            while not self._eof() and self.text[self.pos].isascii() \
                    and self.text[self.pos].isalpha():
                self.pos += 1
            if self.pos == tag_start:
                self._err("LANGTAG must start with an ASCII letter")
            while self._peek() == "-":
                self.pos += 1
                sub_start = self.pos
                while self.pos < self.n and self.text[self.pos].isascii() \
                        and self.text[self.pos].isalnum():
                    self.pos += 1
                if self.pos == sub_start:
                    raise NQuadsSyntaxError(
                        "LANGTAG subtag after '-' must be non-empty and "
                        "ASCII alphanumeric", sub_line, sub_col,
                        self._line_text(sub_line))
            tag = self.text[tag_start:self.pos].lower()
            return (TAG_LITERAL, LIT_LANG, lexical, tag)
        if self.text.startswith("^^", self.pos):
            self.pos += 2
            if self._peek() != "<":
                self._err("datatype after '^^' must be an IRI")
            datatype = self._iri()[1]
            if datatype == XSD_STRING:
                return (TAG_LITERAL, LIT_PLAIN, lexical, "")
            return (TAG_LITERAL, LIT_TYPED, lexical, datatype)
        return (TAG_LITERAL, LIT_PLAIN, lexical, "")

    # --- document --------------------------------------------------------

    def parse(self) -> ParsedDataset:
        self._skip_separators()
        while not self._eof():
            stmt_line = self.line
            subject = self._term()
            if subject[0] not in (TAG_IRI, TAG_BNODE):
                self._err("subject must be an IRI or a blank node",
                          stmt_line)
            if not self._skip_ws_comments():
                self._err("expected white space after the subject")
            predicate = self._term()
            if predicate[0] != TAG_IRI:
                self._err("predicate must be an IRI")
            if not self._skip_ws_comments():
                self._err("expected white space after the predicate")
            obj = self._term()
            if obj[0] not in (TAG_IRI, TAG_BNODE, TAG_LITERAL):
                self._err("object must be an IRI, a blank node or a literal")
            graph = None
            if self._skip_ws_comments() and self._peek() != ".":
                graph = self._term()
                if graph[0] not in (TAG_IRI, TAG_BNODE):
                    self._err("graph label must be an IRI or a blank node")
                self._skip_ws_comments()
            if self._peek() != ".":
                self._err("expected '.' at the end of the statement")
            self.pos += 1
            quad = (subject, predicate, obj, graph)
            self.quads[quad] = self.quads.get(quad, 0) + 1
            if self.quads[quad] == 1:
                self.order.append(quad)
            self._skip_ws_comments()
            if not self._eof():
                if not self._consume_eol():
                    self._err("trailing content after the end of the "
                              "statement (expected a line break)")
                self._skip_separators()
        return ParsedDataset(self.order, self.bnode_order,
                             dict(self.quads), sum(self.quads.values()))


def parse_nquads(text: str, source: str = "<input>") -> ParsedDataset:
    return _Parser(text, source).parse()


def parse_nquads_bytes(data: bytes, source: str = "<input>") -> ParsedDataset:
    """Decode strict UTF-8 then parse; errors always carry line and column."""
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as e:
        prefix = data[:e.start]
        line = prefix.count(b"\n") + 1
        column = e.start - (prefix.rfind(b"\n") + 1) + 1
        start = prefix.rfind(b"\n") + 1
        end = data.find(b"\n", e.start)
        if end == -1:
            end = len(data)
        text_line = data[start:end].decode("utf-8", "replace").rstrip("\r")
        raise NQuadsSyntaxError(
            "input is not valid UTF-8 near byte %d (%s)" % (e.start, source),
            line, column, text_line)
    return parse_nquads(text, source)


# ---------------------------------------------------------------------------
# Canonical N-Quads form (Appendix A) and §5 serialization
# ---------------------------------------------------------------------------

def _escape_literal(lexical: str) -> str:
    out = ['"']
    for ch in lexical:
        o = ord(ch)
        if ch == '"':
            out.append('\\"')
        elif ch == "\\":
            out.append("\\\\")
        elif ch == "\n":
            out.append("\\n")
        elif ch == "\r":
            out.append("\\r")
        elif o == 0x09:
            out.append("\\t")
        elif o == 0x08:
            out.append("\\b")
        elif o == 0x0C:
            out.append("\\f")
        elif o <= 0x07 or o == 0x0B or 0x0E <= o <= 0x1F or o == 0x7F \
                or o in (0xFFFE, 0xFFFF):
            out.append("\\u%04X" % o)
        else:
            out.append(ch)
    out.append('"')
    return "".join(out)


def _escape_iri(value: str) -> str:
    out = ["<"]
    for ch in value:
        o = ord(ch)
        if ch in _IRI_FORBIDDEN or o <= 0x20:
            if o <= 0xFFFF:
                out.append("\\u%04X" % o)
            else:
                out.append("\\U%08X" % o)
        else:
            out.append(ch)
    out.append(">")
    return "".join(out)


def _serialize_term(term, label_for_bnode=None) -> str:
    tag = term[0]
    if tag == TAG_IRI:
        return _escape_iri(term[1])
    if tag == TAG_BNODE:
        label = label_for_bnode(term[1]) if label_for_bnode else term[1]
        return "_:" + label
    kind, lexical, value = term[1], term[2], term[3]
    if kind == LIT_LANG:
        return _escape_literal(lexical) + "@" + value
    if kind == LIT_TYPED:
        return _escape_literal(lexical) + "^^" + _escape_iri(value)
    return _escape_literal(lexical)


def _serialize_quad(quad, label_for_bnode=None) -> str:
    s, p, o, g = quad
    parts = [_serialize_term(s, label_for_bnode),
             _serialize_term(p, label_for_bnode),
             _serialize_term(o, label_for_bnode)]
    if g is not None:
        parts.append(_serialize_term(g, label_for_bnode))
    return " ".join(parts) + " .\n"


# ---------------------------------------------------------------------------
# Identifier issuer (§4.3, §4.5.2)
# ---------------------------------------------------------------------------

class _IdentifierIssuer:
    __slots__ = ("prefix", "counter", "issued", "order")

    def __init__(self, prefix: str):
        self.prefix = prefix
        self.counter = 0
        self.issued: Dict[str, str] = {}
        self.order: List[str] = []   # existing identifiers in issuance order

    def clone(self) -> "_IdentifierIssuer":
        other = _IdentifierIssuer(self.prefix)
        other.counter = self.counter
        other.issued = dict(self.issued)
        other.order = list(self.order)
        return other

    def issue(self, existing: str) -> str:
        """Issue Identifier Algorithm (§4.5.2)."""
        got = self.issued.get(existing)
        if got is not None:
            return got
        new = self.prefix + str(self.counter)
        self.issued[existing] = new
        self.order.append(existing)
        self.counter += 1
        return new


# ---------------------------------------------------------------------------
# Canonicalization (§4.4.3)
# ---------------------------------------------------------------------------

@dataclass
class CanonicalResult:
    canonical_bytes: bytes
    quad_count: int
    raw_quad_count: int
    blank_node_count: int
    label_map: Dict[str, str]        # input label -> canonical label (no "_:")
    work_used: int                   # budget value after this run
    max_work: int
    digest: str = ""

    def as_dict(self) -> dict:
        return {
            "status": "canonicalized",
            "sha256": self.digest,
            "format": "canonical-n-quads",
            "algorithm": "RDFC-1.0",
            "hash_algorithm": "SHA-256",
            "quad_count": self.quad_count,
            "input_quad_occurrences": self.raw_quad_count,
            "duplicate_occurrences": self.raw_quad_count - self.quad_count,
            "blank_nodes": self.blank_node_count,
            "work": {"used": self.work_used, "max": self.max_work},
            "canonical_size": len(self.canonical_bytes),
            "blank_node_map": dict(sorted(self.label_map.items())),
        }


class Canonicalizer:
    def __init__(self, dataset: ParsedDataset,
                 max_work: int = DEFAULT_MAX_WORK,
                 budget: Optional[WorkBudget] = None):
        self.dataset = dataset
        self.budget = budget if budget is not None else WorkBudget(max_work)
        self.raw_quad_count = dataset.raw_occurrences
        self.quad_count = len(dataset.quads)
        self.input_quads = dataset.quads
        self.blank_labels = list(dataset.blank_labels)
        self.blank_node_count = len(dataset.blank_labels)

        # §4.4.3 step 2: blank node to quads map.  The blank node label is a
        # single node for the whole input dataset, wherever it occurs.  The
        # map value is the *set* of quads mentioning the node (§4.2), so a
        # quad in which the same node appears twice (e.g. object and graph
        # name) is recorded once for that node.
        self.bnode_to_quads: Dict[str, List[Quad]] = {
            label: [] for label in dataset.blank_labels}
        for quad in self.input_quads:
            seen = set()
            for term in quad:
                if is_bnode(term) and term[1] not in seen:
                    seen.add(term[1])
                    self.bnode_to_quads[term[1]].append(quad)

        self.canonical_issuer = _IdentifierIssuer("c14n")
        self.first_degree_cache: Dict[str, str] = {}

    # -- §4.6.3 -----------------------------------------------------------

    def _hash_first_degree_quads(self, reference: str) -> str:
        cached = self.first_degree_cache.get(reference)
        if cached is not None:
            return cached
        lines = [
            _serialize_quad(
                quad,
                lambda label, ref=reference: "a" if label == ref else "z")
            for quad in self.bnode_to_quads[reference]
        ]
        lines.sort()
        digest = hashlib.sha256(
            "".join(lines).encode("utf-8")).hexdigest()
        self.first_degree_cache[reference] = digest
        return digest

    # -- §4.7.3 -----------------------------------------------------------

    def _hash_related(self, related: str, quad: Quad,
                      issuer: _IdentifierIssuer, position: str) -> str:
        predicate = quad[1][1]
        buf = [position]
        if position != "g":
            buf.append("<")
            buf.append(predicate)
            buf.append(">")
        canonical = self.canonical_issuer.issued.get(related)
        if canonical is not None:
            buf.append("_:")
            buf.append(canonical)
        else:
            temporary = issuer.issued.get(related)
            if temporary is not None:
                buf.append("_:")
                buf.append(temporary)
            else:
                buf.append(self._hash_first_degree_quads(related))
        return hashlib.sha256("".join(buf).encode("utf-8")).hexdigest()

    # -- §4.8.3 -----------------------------------------------------------

    def _hash_n_degree_quads(self, identifier: str,
                             issuer: _IdentifierIssuer):
        self.budget.charge("hash_n_degree_quads")
        related_by_hash: Dict[str, List[str]] = {}
        for quad in self.bnode_to_quads[identifier]:
            s, _p, o, g = quad
            for position, component in (("s", s), ("o", o), ("g", g)):
                if component is None or component[0] != TAG_BNODE:
                    continue
                related = component[1]
                if related == identifier:
                    continue
                h = self._hash_related(related, quad, issuer, position)
                # §4.8.3 (3.2): append the identifier for this *mention*.
                # The list is a mention list, so the same node can appear
                # more than once (e.g. the same bnode in two quads sharing
                # predicate and position); those repetitions are part of the
                # candidate permutations and must not be collapsed.
                related_by_hash.setdefault(h, []).append(related)

        data_to_hash: List[str] = []
        for related_hash in sorted(related_by_hash):
            data_to_hash.append(related_hash)
            chosen_path: Optional[str] = None
            chosen_issuer: Optional[_IdentifierIssuer] = None
            for permutation in itertools.permutations(related_by_hash[related_hash]):
                # §4.8.3 (5.4): each candidate permutation is charged even if
                # it is pruned below.
                self.budget.charge("permutation")
                issuer_copy = issuer.clone()
                path = ""
                recursion_list: List[str] = []
                skip = False
                for related in permutation:
                    canonical = self.canonical_issuer.issued.get(related)
                    if canonical is not None:
                        path += "_:" + canonical
                    else:
                        if related not in issuer_copy.issued:
                            recursion_list.append(related)
                        path += "_:" + issuer_copy.issue(related)
                    if chosen_path and len(path) >= len(chosen_path) \
                            and path > chosen_path:
                        skip = True
                        break
                if skip:
                    continue
                for related in recursion_list:
                    result_hash, result_issuer = self._hash_n_degree_quads(
                        related, issuer_copy)
                    issuer_copy = result_issuer
                    path += "_:" + issuer_copy.issue(related)
                    path += "<" + result_hash + ">"
                    if chosen_path and len(path) >= len(chosen_path) \
                            and path > chosen_path:
                        skip = True
                        break
                if skip:
                    continue
                if chosen_path is None or path < chosen_path:
                    chosen_path = path
                    chosen_issuer = issuer_copy
            data_to_hash.append(chosen_path if chosen_path is not None else "")
            if chosen_issuer is not None:
                issuer = chosen_issuer

        digest = hashlib.sha256(
            "".join(data_to_hash).encode("utf-8")).hexdigest()
        return digest, issuer

    # -- §4.4.3 -----------------------------------------------------------

    def canonicalize(self) -> CanonicalResult:
        # §4.4.3 (3): first degree hash for every blank node.
        hash_to_bnodes: Dict[str, List[str]] = {}
        for label in self.blank_labels:
            h = self._hash_first_degree_quads(label)
            hash_to_bnodes.setdefault(h, []).append(label)

        # (4): unique hashes get canonical identifiers immediately.
        for h in sorted(hash_to_bnodes):
            bucket = hash_to_bnodes[h]
            if len(bucket) == 1:
                self.canonical_issuer.issue(bucket[0])
                del hash_to_bnodes[h]

        # (5): shared hashes use Hash N-Degree Quads, iterating in hash order.
        for h in sorted(hash_to_bnodes):
            identifier_list = hash_to_bnodes[h]
            hash_path_list = []
            for n in identifier_list:
                if n in self.canonical_issuer.issued:
                    continue
                temporary = _IdentifierIssuer("b")
                temporary.issue(n)
                digest, temporary = self._hash_n_degree_quads(n, temporary)
                hash_path_list.append((digest, temporary))
            hash_path_list.sort(key=lambda item: item[0])
            for _digest, temporary in hash_path_list:
                for existing in temporary.order:
                    if existing not in self.canonical_issuer.issued:
                        self.canonical_issuer.issue(existing)

        # §5: canonical labels, code point order, concatenated; every
        # statement already ends with a single LF.
        issued = self.canonical_issuer.issued
        lines = sorted(_serialize_quad(quad, lambda label: issued[label])
                       for quad in self.input_quads)
        payload = "".join(lines).encode("utf-8")
        label_map = {label: issued[label] for label in self.blank_labels}
        return CanonicalResult(
            canonical_bytes=payload, quad_count=self.quad_count,
            raw_quad_count=self.raw_quad_count,
            blank_node_count=self.blank_node_count, label_map=label_map,
            work_used=self.budget.used, max_work=self.budget.max_work,
            digest=hashlib.sha256(payload).hexdigest())


def canonicalize_dataset(dataset: ParsedDataset,
                         max_work: int = DEFAULT_MAX_WORK,
                         budget: Optional[WorkBudget] = None) -> CanonicalResult:
    return Canonicalizer(dataset, max_work, budget).canonicalize()


def canonicalize_bytes(data: bytes, max_work: int = DEFAULT_MAX_WORK,
                       source: str = "<input>",
                       budget: Optional[WorkBudget] = None) -> CanonicalResult:
    return canonicalize_dataset(parse_nquads_bytes(data, source),
                                max_work, budget)


# ---------------------------------------------------------------------------
# Dataset equality helpers
# ---------------------------------------------------------------------------

def first_difference(a: bytes, b: bytes):
    """Return (offset, kind) for the first byte difference, or (None, None).

    ``kind`` is ``byte_mismatch``, ``left_ended`` or ``right_ended``.
    Offsets are 0-based byte offsets into canonical N-Quads documents.
    """
    limit = min(len(a), len(b))
    for i in range(limit):
        if a[i] != b[i]:
            return i, "byte_mismatch"
    if len(a) != len(b):
        return limit, ("left_ended" if len(a) < len(b) else "right_ended")
    return None, None


def offset_location(data: bytes, offset: int) -> Tuple[int, int]:
    """1-based (line, column) for a byte offset in a UTF-8 canonical file."""
    prefix = data[:offset]
    line = prefix.count(b"\n") + 1
    column = len(prefix) - (prefix.rfind(b"\n") + 1) + 1
    return line, column


def substitute_bnodes(quads: List[Quad], mapping: Dict[str, str]) -> List[Quad]:
    """Replace every blank node label in quads according to mapping."""
    out = []
    for s, p, o, g in quads:
        ns = (TAG_BNODE, mapping[s[1]]) if s[0] == TAG_BNODE else s
        np_ = (TAG_BNODE, mapping[p[1]]) if p[0] == TAG_BNODE else p
        no = (TAG_BNODE, mapping[o[1]]) if o[0] == TAG_BNODE else o
        ng = None
        if g is not None:
            ng = (TAG_BNODE, mapping[g[1]]) if g[0] == TAG_BNODE else g
        out.append((ns, np_, no, ng))
    return out


def build_correspondence(left: CanonicalResult,
                         right: CanonicalResult) -> Dict[str, str]:
    """Compose original labels through the shared canonical labels.

    Equal canonical bytes guarantee a bijection between canonical labels, so
    this yields a one-to-one map from left input labels to right input labels.
    """
    right_by_canonical: Dict[str, List[str]] = {}
    for label, canonical in right.label_map.items():
        right_by_canonical.setdefault(canonical, []).append(label)
    result = {}
    for label, canonical in left.label_map.items():
        candidates = right_by_canonical.get(canonical, [])
        if len(candidates) != 1:
            raise CanonError(
                "canonical label %r is not uniquely matched on the right side"
                % canonical, canonical=canonical, candidates=candidates)
        result[label] = candidates[0]
    if len(set(result.values())) != len(result):
        raise CanonError("blank node correspondence is not one-to-one")
    return result
