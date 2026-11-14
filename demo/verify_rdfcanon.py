#!/usr/bin/env python3
"""Independent checks for the RDF canonicalization demo.

Nothing here imports ``rvault.rdfcanon``.  These helpers re-read N-Quads with
a deliberately simple parser written for this file, then verify the demo's
claims at the level an administrator cares about:

* the canonical output is the input dataset after applying the reported
  label map (every quad matches, no quad added or lost);
* the reported label map is one-to-one and covers exactly the blank nodes
  that appear in that dataset;
* a blank-node correspondence between two files maps every left quad onto a
  right quad and vice versa (no leftovers on either side);
* canonical output files really are sorted, LF-terminated UTF-8.

Usage from the demo script only; this module raises AssertionError on any
mismatch.
"""
from __future__ import annotations

import hashlib

_WS = " \t"


class Term:
    __slots__ = ("kind", "value", "lang", "datatype")

    def __init__(self, kind, value, lang=None, datatype=None):
        self.kind = kind          # 'iri' | 'bnode' | 'lit'
        self.value = value
        self.lang = lang
        self.datatype = datatype

    def key(self):
        if self.kind == "lit":
            return ("lit", self.value, self.lang or "", self.datatype or "")
        return (self.kind, self.value)

    def __repr__(self):
        return "Term(%r,%r,%r,%r)" % (self.kind, self.value, self.lang,
                                      self.datatype)


_ECHAR = {"t": "\t", "b": "\b", "n": "\n", "r": "\r", "f": "\f",
          '"': '"', "'": "'", "\\": "\\"}


def _iri(text, i):
    assert text[i] == "<", (i, text[i:i + 20])
    i += 1
    out = []
    while text[i] != ">":
        if text[i] == "\\":
            kind = text[i + 1]
            assert kind in "uU", text[i:i + 2]
            n = 4 if kind == "u" else 8
            out.append(chr(int(text[i + 2:i + 2 + n], 16)))
            i += 2 + n
        else:
            out.append(text[i])
            i += 1
    return Term("iri", "".join(out)), i + 1


def _literal(text, i):
    assert text[i] == '"'
    i += 1
    out = []
    while text[i] != '"':
        if text[i] == "\\":
            kind = text[i + 1]
            if kind in "uU":
                n = 4 if kind == "u" else 8
                out.append(chr(int(text[i + 2:i + 2 + n], 16)))
                i += 2 + n
            else:
                out.append(_ECHAR[kind])
                i += 2
        else:
            out.append(text[i])
            i += 1
    lexical = "".join(out)
    i += 1
    if i < len(text) and text[i] == "@":
        j = i + 1
        while j < len(text) and (text[j].isalnum() or text[j] == "-"):
            j += 1
        return Term("lit", lexical, lang=text[i + 1:j].lower()), j
    if text.startswith("^^", i):
        dt, i = _iri(text, i + 2)
        return Term("lit", lexical, datatype=dt.value), i
    return Term("lit", lexical), i


def _bnode(text, i):
    j = i + 2
    while j < len(text) and text[j] not in _WS and text[j] not in "\r\n" \
            and text[j] != ">":
        if text[j] == "." and (j + 1 >= len(text)
                               or text[j + 1] in _WS + "\r\n"):
            break
        j += 1
    return Term("bnode", text[i + 2:j]), j


def _term(text, i):
    if text[i] == "<":
        return _iri(text, i)
    if text[i] == '"':
        return _literal(text, i)
    assert text.startswith("_:", i), text[i:i + 20]
    return _bnode(text, i)


def parse(path):
    """Return a set of quads as tuples of Term.key(); comments understood.

    Line splitting uses only LF/CRLF: ``str.splitlines`` would also split on
    U+0085/U+2028, which are legal characters inside an N-Quads string.
    """
    with open(path, "rb") as f:
        text = f.read().decode("utf-8")
    quads = set()
    raw_lines = text.split("\n")
    if raw_lines and raw_lines[-1] == "":
        raw_lines.pop()
    # Strip comments only where they cannot be inside <...> or "...".
    cleaned_lines = []
    for raw in raw_lines:
        if raw.endswith("\r"):
            raw = raw[:-1]
        line = []
        i = 0
        in_iri = in_lit = False
        while i < len(raw):
            ch = raw[i]
            if in_iri:
                line.append(ch)
                if ch == ">":
                    in_iri = False
                i += 1
                continue
            if in_lit:
                line.append(ch)
                if ch == "\\":
                    line.append(raw[i + 1])
                    i += 2
                    continue
                if ch == '"':
                    in_lit = False
                i += 1
                continue
            if ch == "<":
                in_iri = True
            elif ch == '"':
                in_lit = True
            elif ch == "#":
                break
            line.append(ch)
            i += 1
        cleaned_lines.append("".join(line))
    for line in cleaned_lines:
        line = line.strip(" \t")
        if not line:
            continue
        assert line.endswith("."), line
        line = line[:-1]
        i = 0
        terms = []
        while i < len(line):
            while i < len(line) and line[i] in _WS:
                i += 1
            if i >= len(line):
                break
            term, i = _term(line, i)
            terms.append(term.key())
        assert len(terms) in (3, 4), (len(terms), line)
        quads.add(tuple(terms))
    return quads


def bnode_labels(quads):
    return {t[1] for q in quads for t in q if t[0] == "bnode"}


def apply_map(quads, mapping):
    """mapping: original label -> new label for quads in the canonical file."""
    out = set()
    for quad in quads:
        out.add(tuple(("bnode", mapping[t[1]]) if t[0] == "bnode" else t
                      for t in quad))
    return out


def assert_map_is_bijection(mapping, input_labels, output_labels):
    assert len(mapping) == len(input_labels), (len(mapping), len(input_labels))
    assert set(mapping) == set(input_labels), (
        set(mapping) ^ set(input_labels))
    assert len(set(mapping.values())) == len(mapping), "not one-to-one"
    assert set(mapping.values()) == set(output_labels), (
        set(mapping.values()) ^ set(output_labels))


def assert_topologically_equal(left_quads, right_quads):
    """There exists a bnode bijection mapping left dataset onto right."""
    left_labels = bnode_labels(left_quads)
    right_labels = bnode_labels(right_quads)
    # ground quads must match exactly
    ground_l = {q for q in left_quads
                if all(t[0] != "bnode" for t in q)}
    ground_r = {q for q in right_quads
                if all(t[0] != "bnode" for t in q)}
    assert ground_l == ground_r, "ground quads differ"
    if not left_labels and not right_labels:
        return
    # Greedy bijection search by degree signature is enough for the small
    # demo datasets; fall back to brute force for up to 8 nodes.
    import itertools
    if len(left_labels) != len(right_labels):
        raise AssertionError("blank node counts differ: %d vs %d"
                             % (len(left_labels), len(right_labels)))
    ll = sorted(left_labels)
    rl = sorted(right_labels)
    if len(ll) > 8:
        raise AssertionError("dataset too large for the independent "
                             "isomorphism check")
    for perm in itertools.permutations(rl):
        mapping = dict(zip(ll, perm))
        if apply_map(left_quads, mapping) == right_quads:
            return
    raise AssertionError("no blank node bijection maps left dataset onto "
                         "right dataset")


_ESCAPE_LITERAL = {'"': '\\"', "\\": "\\\\", "\n": "\\n", "\r": "\\r",
                   "\t": "\\t", "\b": "\\b", "\f": "\\f"}


def serialize_term_key(key):
    """Independent canonical N-Quads rendering of one parsed term key."""
    if key[0] == "iri":
        return "<" + key[1] + ">"
    if key[0] == "bnode":
        return "_:" + key[1]
    _kind, lexical, lang, datatype = key
    out = ['"']
    for ch in lexical:
        o = ord(ch)
        if ch in _ESCAPE_LITERAL:
            out.append(_ESCAPE_LITERAL[ch])
        elif o <= 0x07 or o == 0x0B or 0x0E <= o <= 0x1F or o == 0x7F:
            out.append("\\u%04X" % o)
        else:
            out.append(ch)
    out.append('"')
    if lang:
        out.append("@" + lang)
    elif datatype:
        out.append("^^<" + datatype + ">")
    return "".join(out)


def serialize_quad_key(quad):
    return " ".join(serialize_term_key(t) for t in quad) + " ."


def assert_canonical_form(path):
    """Structural checks any canonical N-Quads file must satisfy."""
    with open(path, "rb") as f:
        payload = f.read()
    payload.decode("utf-8")
    assert payload == b"" or payload.endswith(b"\n"), \
        "canonical file must end with one LF"
    if not payload:
        return
    lines = payload[:-1].split(b"\n")
    assert lines == sorted(lines), "canonical lines are not sorted"
    for line in lines:
        assert b"\r" not in line
        assert line.endswith(b" .")
        assert b"\t" not in line
        text = line.decode("utf-8")
        # Re-serialize independently; only a canonical layout survives.
        body = text[:-1].rstrip(" ")
        i = 0
        terms = []
        while i < len(body):
            while i < len(body) and body[i] == " ":
                i += 1
            term, i = _term(body, i)
            terms.append(term.key())
        assert serialize_quad_key(terms) == text, (
            "not in canonical form", text, serialize_quad_key(terms))


def sha256_file(path):
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


def has_duplicate_lines(path):
    with open(path, "rb") as f:
        lines = f.read().split(b"\n")
    lines = [l for l in lines if l]
    return len(lines) != len(set(lines))
