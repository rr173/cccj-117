# Case 1 — same catalogue, renamed blank labels, shuffled and duplicated

`left.nq` is a small catalogue export.  `right.nq` is the same dataset:
every blank node was renamed (`_:x` -> `_:node77`, `_:y` -> `_:zzz1`,
`_:g` -> `_:shapeA`), statements were reordered, comments were added, and
three statements were repeated verbatim.

Expected: canonical bytes identical, SHA-256 identical, and the reported
blank-node correspondence maps every left quad onto a right quad and back.
