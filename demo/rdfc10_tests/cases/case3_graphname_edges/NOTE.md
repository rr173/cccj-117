# Case 3 — blank graph names, self-loops, non-ASCII, escapes, language/datatype

One export where:

* `_:g` is used both as a **graph name** and as an ordinary term inside
  another graph (same node, one scope for the whole dataset);
* `_:s` has a self-loop;
* literals include non-ASCII text, an escaped quote, a language-tagged value
  with an uppercase tag, and `"01"^^xsd:integer` next to `"1"^^xsd:integer`
  (distinct lexical forms, never merged by numeric value);
* `"x"^^xsd:string` and `"x"` are the same RDF 1.1 term and must collapse.

Expected: the read-back canonical file keeps every graph boundary, keeps the
literal lexical forms distinct where the model says they are distinct, and the
round-trip through the parser reproduces exactly the reported quads.
