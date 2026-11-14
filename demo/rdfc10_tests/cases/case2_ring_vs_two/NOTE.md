# Case 2 — one six-node cycle vs. two disconnected three-node cycles

Both exports have six blank nodes, each node has exactly one outgoing and one
incoming edge, and both contain exactly six quads.  Local degree sequences are
identical and the quad counts are identical, but the datasets are **not**
isomorphic: the left one is a single cycle, the right one is two cycles.

Expected: `different`, exit code 1, different SHA-256 on each side, and the
first differing byte reported (not a claim of "minimal business change").
