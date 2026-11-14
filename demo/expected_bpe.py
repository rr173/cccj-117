"""Hand-computed expected answers for the BPE demonstration.

Nothing here is produced by rvault.bpe. The numbers were worked out on paper
from the byte-pair rules; run_bpe_demo.py re-checks them both against the
slow reference (demo/bpe_slow_ref.py) and against the real on-disk training
database via its own SQL.

Byte values used below:
  a=97 b=98 c=99 (newline)=10
  'é' = UTF-8 bytes (195, 169)

========================================================================
1. The weighted aaaa example
========================================================================
One doc: id=x, weight=2, text="aaaa" -> bytes [97,97,97,97]

Round 1: overlapping adjacent positions in one text = 3
  (positions 0-1, 1-2, 2-3), all the pair (97,97).
  weighted count = 3 * weight 2 = 6.
  Left-to-right non-overlapping replacement consumes positions 0-1 and
  2-3: [97,97,97,97] -> [256,256]            (2 replacement positions)

Round 2: the only adjacent pair is now (256,256), count 1 * weight 2 = 2.
  [256,256] -> [257]                           (1 replacement position)

Then no pairs remain; training stops even if more merges were requested.
The two rounds' counts (6 and 2) must never be conflated.
"""

AAAA_ROUNDS = [
    {"round": 1, "token_id": 256, "left": 97, "right": 97,
     "weighted_count": 6, "docs_hit": 1, "docs_rewritten": 1,
     "positions_replaced": 2},
    {"round": 2, "token_id": 257, "left": 256, "right": 256,
     "weighted_count": 2, "docs_hit": 1, "docs_rewritten": 1,
     "positions_replaced": 1},
]
AAAA_FINAL = {"x": [257]}

# ---------------------------------------------------------------------------
# 2. Rich corpus: tie-break, same text/different id, false cross-text
#    adjacency, empty text, non-ASCII.
#
#   id  weight text        initial bytes
#   a   1      "abc"       [97,98,99]
#   b   2      "ba"        [98,97]
#   c   2      "abc"       [97,98,99]   (same text as a, different id:
#                                        each contributes its own weight)
#   d   1      ""          []           (legal, has no adjacent pair)
#   e   1      "é"         [195,169]
#   f   1      "\nb"       [10,98]
#
# Initial weighted pair multiset:
#   (97,98):  doc a 1*1 + doc c 2*1                = 3
#   (98,99):  doc a 1   + doc c 2                  = 3
#   (98,97):  doc b weight 2                       = 2
#   (195,169): doc e                               = 1
#   (10,98):  doc f                                = 1
#
# False adjacency that must never appear: (99,98) across the end of one
# text and the start of the next, and (169,10) across e->f: replacement
# never crosses a doc boundary.
# ---------------------------------------------------------------------------

RICH_FALSE_PAIRS = [(99, 98), (169, 10)]

# Round 1: (97,98) and (98,99) tie at weighted count 3; the
# (left, right) id ordering picks (97,98). Token 256. Docs a and c:
#   [97,98,99] -> [256,99]
# Each of a/c loses (97,98)*1 and (98,99)*1, gains (256,99)*1; weighted,
# the new pair (256,99) therefore has count 1+2 = 3.
RICH_ROUNDS = [
    {"round": 1, "token_id": 256, "left": 97, "right": 98,
     "weighted_count": 3, "docs_hit": 2, "docs_rewritten": 2,
     "positions_replaced": 2},
    # Counts now: (256,99)=3 (weighted over a and c), (98,97)=2,
    # (195,169)=1, (10,98)=1. The unique winner is (256,99); token 257;
    # a and c: [256,99] -> [257].
    {"round": 2, "token_id": 257, "left": 256, "right": 99,
     "weighted_count": 3, "docs_hit": 2, "docs_rewritten": 2,
     "positions_replaced": 2},
    # Remaining: (98,97)=2, then the two count-1 pairs (10,98) and
    # (195,169). (98,97) wins; token 258; doc b: [98,97] -> [258].
    {"round": 3, "token_id": 258, "left": 98, "right": 97,
     "weighted_count": 2, "docs_hit": 1, "docs_rewritten": 1,
     "positions_replaced": 1},
    # Now (10,98) and (195,169) tie at count 1: the (left,right)
    # tie-break picks (10,98). Token 259; doc f: [10,98] -> [259].
    {"round": 4, "token_id": 259, "left": 10, "right": 98,
     "weighted_count": 1, "docs_hit": 1, "docs_rewritten": 1,
     "positions_replaced": 1},
    # Only (195,169)=1 remains. Token 260; doc e: [195,169] -> [260].
    {"round": 5, "token_id": 260, "left": 195, "right": 169,
     "weighted_count": 1, "docs_hit": 1, "docs_rewritten": 1,
     "positions_replaced": 1},
]

RICH_FINAL = {
    "a": [257],
    "b": [258],
    "c": [257],
    "d": [],
    "e": [260],
    "f": [259],
}

# Texts not present during training still have to encode/decode losslessly.
RICH_ENCODE_CASES = [
    {"id": "new1", "text": "abcba"},
    {"id": "new2", "text": ""},
    {"id": "new3", "text": "é组合 😀\n\t "},
    {"id": "new4", "text": "a" * 10},
]
