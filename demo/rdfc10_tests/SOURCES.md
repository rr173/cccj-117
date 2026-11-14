# RDFC-1.0 test fixtures — origin and provenance

## official/

Copied **verbatim** from the W3C RDF Dataset Canonicalization test suite:

* Repository: <https://github.com/w3c/rdf-canon>
* Test suite path: `tests/rdfc10/`
* Manifest: <https://w3c.github.io/rdf-canon/tests/manifest.ttl>
* Upstream snapshot: commit `5ffaaa13c0e19a2a2f9e60d0ecd9efaf3edab633`
  (2026-11-14), downloaded from
  <https://codeload.github.com/w3c/rdf-canon/tar.gz/refs/heads/main>
* Distribution: W3C Test Suite License and W3C 3-clause BSD License
  (see `LICENCE-official.md` in this directory; copied from the same snapshot).

Only the files needed by the positive SHA-256 evaluation tests and their
issued-identifier map tests are retained.  `*-in.nq` are the inputs,
`*-rdfc10.nq` the official expected canonical output, and `*-rdfc10map.json`
the official expected issued identifiers map.  **None of these expected files
was produced by this or any other implementation at test time**; they are the
upstream expected outputs checked into the W3C repository.

Tests retained (16 positive evaluation tests, all SHA-256):

| Test | Name | Shape covered |
|---|---|---|
| test020 | blank node - diamond | diamond, shared node |
| test022 | blank node - double circle of 2 | symmetric double circle |
| test023 | blank node - circle of 3 | cycle of 3 |
| test024 | blank node - double circle of 3 (0-1-2) | symmetric double circle |
| test025 | blank node - double circle of 3 (0-2-1) | symmetric double circle |
| test030 | blank node - point at circle of 3 | shared node + cycle |
| test033 | disjoint identical subgraphs (1) | two disconnected components |
| test043 | literal with language | literal language tag |
| test057 | unnamed graph | blank node as graph name |
| test058 | unnamed graph with blank node objects | bnode graph + bnode objects |
| test060 | n-quads escaping | escapes, control chars, non-ASCII |
| test061 | same literal value with multiple languages | language tag lowercase |
| test062 | same literal value with multiple datatypes | typed literals |
| test070 | dataset - isomorphic default and IRI named | graph boundary |
| test071 | dataset - isomorphic default and node named | blank graph name |
| test072 | dataset - shared blank nodes | cross-graph shared bnode |
| test073 | dataset - referencing graph name | bnode graph name also used as term |

`test020`/`test030` also carry an official map test.  `test060` (N-Quads
escaping) is the canonical-form check for literal escaping and non-ASCII.

## cases/

Small hand-written cases used by the demo to explain the administrator
scenarios; not from the W3C suite.  Each case directory contains
`left.nq`, `right.nq`, `expected_same` and a short `NOTE.md`.

## samples/

Inputs for the resource-limit demo.  `symmetric_ring.nq` is a hand-written
symmetric ring; `last_line_bad.nq` is a deliberately broken file.

## Permutation checks

`demo/run_rdfcanon_demo.py` re-derives, for every official test, several
random blank-label renamings and statement-order shuffles of the *input*
and compares each result byte-for-byte against the **same official expected
file**.  The generated permutations live only in the temporary run directory
and are recorded in `permutations.json` there.  The expected file is never
regenerated.
