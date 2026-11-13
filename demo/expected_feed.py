"""Hand-written expected results for the change-feed demo.

Everything below was worked out BY HAND from the fixture rows and the phase
script in demo/run_feed_demo.py - NOT by running the implementation.
verify_feed.py reads the real database with its own SQL and compares against
these literals; run_feed_demo.py compares each recorded pull line by line
against DELIVERIES.

Fixtures
--------
Main store (study "obs").  measure = base[person] + edition,
base: alice 10, bob 20, cara 30.

  obs_f1 : alice ob1 e1, alice ob2 e1, bob ob3 e5, cara ob4 e1
  obs_f2 : alice ob1 e2, bob ob3 e6, alice ob1 e1 (identical), cara ob4 e1 (identical)
  obs_f3 : bob ob3 e2 (legal OLDER edition), cara ob4 e1 (identical)
  obs_f4 : alice ob5 e1, ob5 e2, ob5 e3, bob ob3 e7, ob3 e8   (revisions in one batch)
  obs_bad: alice ob6 e1, then a truncated JSON line             (whole batch rejected)
  obs_g6 : alice ob6 e1
  obs_post: alice ob1 e3, alice ob6 e2, bob ob7 e1             (alice is barred)
  obs_f5 : cara ob4 e2, cara ob8 e1                            (crash-test traffic)

Why each frame exists (hand derivation, main store)
  f1  import obs_f1   -> 4 new observations                 refresh ob1 ob2 ob3 ob4
  f2  import obs_f2   -> ob1 and ob3 got a higher edition   refresh ob1 ob3
                         (ob1 e1 / ob4 e1 are byte-identical redeliveries: no row
                          is inserted -> no event)
  --  import obs_f3   -> ob3 e2 is older than stored e6; no INSERT happens, no
                         event.  obs_f3 as a whole therefore produces no frame.
  f3  import obs_f4   -> ob5 grows, ob3 grows; repeated revisions of the SAME
                         object inside one batch collapse to ONE refresh
  --  import obs_bad  -> rejected batch: rolled back business rows AND events
  f4  import obs_g6   -> new observation ob6
  f5  withdraw W1 {alice} -> removes for every observation alice owns
                         ob1, ob2, ob5, ob6 (barrier commit, not file cleanup)
  f6  import obs_post -> alice's two lines are suppressed by the barrier (no
                         events); bob's ob7 e1 is new -> refresh ob7
  f7  withdraw W2 {bob} -> objects bob owns: ob3, ob7
  --  withdraw W3 {alice}: alice owns nothing new; every object is already in
                         cf_removed_objects -> no frame (never re-announced)
  f8  withdraw W4 {cara} -> remove ob4

seq allocation (frames are numbered consecutively, ordinals sorted by observation)
  f1 seq 1-4, f2 seq 5-6, f3 seq 7-8, f4 seq 9, f5 seq 10-13,
  f6 seq 14, f7 seq 15-16, f8 seq 17
"""

# ---------------------------------------------------------------- main store
# frame_id -> events in wire order (op, observation)
MAIN_FRAMES = {
    1: [("refresh", "ob1"), ("refresh", "ob2"), ("refresh", "ob3"),
        ("refresh", "ob4")],
    2: [("refresh", "ob1"), ("refresh", "ob3")],
    3: [("refresh", "ob3"), ("refresh", "ob5")],
    4: [("refresh", "ob6")],
    5: [("remove", "ob1"), ("remove", "ob2"), ("remove", "ob5"),
        ("remove", "ob6")],
    6: [("refresh", "ob7")],
    7: [("remove", "ob3"), ("remove", "ob7")],
    8: [("remove", "ob4")],
}


def _frames_with_ranges(frame_map):
    out, seq = [], 1
    for fid in sorted(frame_map):
        n = len(frame_map[fid])
        out.append((fid, seq, seq + n - 1, n))
        seq += n
    return out


MAIN_RANGES = _frames_with_ranges(MAIN_FRAMES)      # (fid, start, end, count)
MAIN_EVENTS = []                                     # (seq, fid, ordinal, op, obs)
for _fid, _start, _end, _n in MAIN_RANGES:
    for _i, (_op, _obs) in enumerate(MAIN_FRAMES[_fid], 1):
        MAIN_EVENTS.append((_start + _i - 1, _fid, _i, _op, _obs))

# state of the source objects (existing query command) after each import /
# withdrawal, used to check that the events describe real changes
MAIN_PROJECTION = {
    "after_f1": [("ob1", "alice", 1), ("ob2", "alice", 1),
                 ("ob3", "bob", 5), ("ob4", "cara", 1)],
    "after_f2": [("ob1", "alice", 2), ("ob2", "alice", 1),
                 ("ob3", "bob", 6), ("ob4", "cara", 1)],
    "after_f3": [("ob1", "alice", 2), ("ob2", "alice", 1),
                 ("ob3", "bob", 6), ("ob4", "cara", 1)],   # unchanged
    "after_f4": [("ob1", "alice", 2), ("ob2", "alice", 1),
                 ("ob3", "bob", 8), ("ob4", "cara", 1), ("ob5", "alice", 3)],
    "after_g6": [("ob1", "alice", 2), ("ob2", "alice", 1),
                 ("ob3", "bob", 8), ("ob4", "cara", 1), ("ob5", "alice", 3),
                 ("ob6", "alice", 1)],                       # after the rejected batch
    "after_w1": [("ob3", "bob", 8), ("ob4", "cara", 1)],
    "after_post": [("ob3", "bob", 8), ("ob4", "cara", 1),
                   ("ob7", "bob", 1)],
    "after_w2": [("ob4", "cara", 1)],
    "after_w4": [],
}

# Deliveries for the two consumers.  "live" was registered before any import
# (default start = now, i.e. first event), "slow" later with an explicit
# still-retained start of 1.  (from, through, events, [(fid,start,end,count)])
MAIN_DELIVERIES = {
    # f1 has 4 events > target 3 -> the first frame is sent whole
    "live_1": (1, 4, 4, [(1, 1, 4, 4)]),
    # f2 (2 events) fits; adding f3 (2 more) would exceed 3 -> stop at boundary
    "live_2": (5, 6, 2, [(2, 5, 6, 2)]),
    # f3 alone (ob3 and ob5 both grew in the f4 batch)
    "live_3": (7, 8, 2, [(3, 7, 8, 2)]),
    # new observation ob6 after the rejected batch (frame 4)
    "live_4": (9, 9, 1, [(4, 9, 9, 1)]),
    # W1: remove frame committed with the barrier, target 1 -> whole (4)
    "live_5": (10, 13, 4, [(5, 10, 13, 4)]),
    # post-withdrawal intake: only bob's new ob7 (alice suppressed)
    "live_6": (14, 14, 1, [(6, 14, 14, 1)]),
    # W2 {bob}: remove ob3 and ob7
    "live_7": (15, 16, 2, [(7, 15, 16, 2)]),
    # W4 {cara}: remove ob4
    "live_8": (17, 17, 1, [(8, 17, 17, 1)]),
    # slow: registered with --from-seq 1 after live had consumed frames 1-4.
    # target 2, frame 1 has 4 events -> whole, NOT acked yet
    "slow_1": (1, 4, 4, [(1, 1, 4, 4)]),
    "slow_2": (5, 6, 2, [(2, 5, 6, 2)]),
    "slow_3": (7, 9, 3, [(3, 7, 8, 2), (4, 9, 9, 1)]),   # target 4
    "slow_4": (10, 13, 4, [(5, 10, 13, 4)]),             # target 3 -> whole
    "slow_5": (14, 16, 3, [(6, 14, 14, 1), (7, 15, 16, 2)]),
    "slow_6": (17, 17, 1, [(8, 17, 17, 1)]),
}

# trim expectations from the main walkthrough, in order.  After each ack the
# watermark is min(acked) over enabled consumers; only complete frames at or
# below it are deleted.
MAIN_TRIMS = [
    # live 17, slow 0 -> nothing
    {"trimmed_frames": 0, "trimmed_events": 0, "retention_floor": 0},
    # slow has a PENDING delivery (not acknowledged) -> still nothing
    {"trimmed_frames": 0, "trimmed_events": 0, "retention_floor": 0},
    # slow acked frame 1 -> floor 4
    {"trimmed_frames": 1, "trimmed_events": 4, "retention_floor": 4},
    # slow acked frame 2 -> floor 6
    {"trimmed_frames": 1, "trimmed_events": 2, "retention_floor": 6},
    # slow acked frames 3+4 -> floor 9
    {"trimmed_frames": 2, "trimmed_events": 3, "retention_floor": 9},
    # slow acked frame 5 -> floor 13
    {"trimmed_frames": 1, "trimmed_events": 4, "retention_floor": 13},
    # slow acked frames 6+7 -> floor 16
    {"trimmed_frames": 2, "trimmed_events": 3, "retention_floor": 16},
    # slow acked frame 8 -> floor 17
    {"trimmed_frames": 1, "trimmed_events": 1, "retention_floor": 17},
    # nothing left
    {"trimmed_frames": 0, "trimmed_events": 0, "retention_floor": 17},
]

# checkpoints consumed by verify_feed.py --db-checkpoint
DB_CHECKPOINTS = {
    # after the pre-feed imports, before feed registration: no consumer, but
    # events exist for every write made since installation
    "main_start": {
        "frames": [],
        "events": [],
        "consumers": {},
        "retention_floor": 0,
        "tail_seq": 0,
        "removed": [],
    },
    # after obs_bad (rejected): no frame for it, no orphan row
    "after_reject": {
        "frames": MAIN_RANGES[:3],
        "events": MAIN_EVENTS[:8],
        "tail_seq": 8,
        "removed": [],
    },
    # W1 after_register pause: remove frame committed with the barrier and
    # BEFORE the editions stage / bundle cleanup.  live has consumed frames
    # 1..4 (seq 1-9); slow was registered from seq 1 and has not pulled yet.
    "during_w1": {
        "frames": MAIN_RANGES[:5],
        "events": MAIN_EVENTS[:13],
        "tail_seq": 13,
        "removed": [("obs", "ob1"), ("obs", "ob2"), ("obs", "ob5"),
                    ("obs", "ob6")],
        "consumers": {
            "live": {"generation": 1, "start_seq": 1, "acked_seq": 9,
                     "enabled": True},
            "slow": {"generation": 1, "start_seq": 1, "acked_seq": 0,
                     "enabled": True},
        },
    },
    # final state of the main walkthrough: all events trimmed, floor 17
    "main_final": {
        "frames": [],
        "events": [],
        "retention_floor": 17,
        "tail_seq": 17,
        "consumers": {
            "live": {"generation": 1, "start_seq": 1, "acked_seq": 17,
                     "enabled": True},
            "slow": {"generation": 1, "start_seq": 1, "acked_seq": 17,
                     "enabled": True},
        },
        "pending": {},
        "removed": [("obs", o) for o in
                    ["ob1", "ob2", "ob3", "ob4", "ob5", "ob6", "ob7"]],
    },
    # after the rejected batch was cleaned up: state identical to before it
    # (only obs_g6's frame follows at seq 9 later)
    "after_bad_recover": {
        "frames": MAIN_RANGES[:3],
        "events": MAIN_EVENTS[:8],
        "tail_seq": 8,
    },
}

# checkpoints for the legacy (pre-feed schema) store.
# A store built with the pre-feed schema has pre-existing objects
#   alice oba e1, bob obb e1, bob obb e2
# After the current binary runs, the feed must contain ONLY events from the
# new import (bob obb e3 = new highest edition); nothing is back-filled for
# the historical editions.
LEGACY_IMPORT_EVENTS = [(1, 1, 1, "refresh", "obb")]   # seq, frame, ordinal, op, obs
LEGACY_PREEXISTING_OBSERVATIONS = ["oba", "obb"]

LEGACY_CHECKPOINT = {
    "frames": [(1, 1, 1, 1)],
    "events": LEGACY_IMPORT_EVENTS,
    "consumers": {
        "legacy": {"generation": 1, "start_seq": 1, "acked_seq": 0,
                   "enabled": True},
    },
    "retention_floor": 0,
    "tail_seq": 1,
    "pending": {},
}

# ---------------------------------------------------------------- edge store
# study "s1", one person "pp", every import adds exactly one frame:
#   e1: o1 e1, o2 e1        -> f1 seq1-2
#   e2: o1 e2               -> f2 seq3
#   e3: o2 e2               -> f3 seq4
#   e4: o3 e1               -> f4 seq5
#   e5: o4 e1               -> f5 seq6
#   e6: o5 e1               -> f6 seq7
#   e7: o6 e1               -> f7 seq8
EDGE_FRAMES = {
    1: [("refresh", "o1"), ("refresh", "o2")],
    2: [("refresh", "o1")],
    3: [("refresh", "o2")],
    4: [("refresh", "o3")],
    5: [("refresh", "o4")],
    6: [("refresh", "o5")],
    7: [("refresh", "o6")],
}
EDGE_RANGES = _frames_with_ranges(EDGE_FRAMES)      # (fid, start, end, count)
EDGE_EVENTS = []
for _fid, _start, _end, _n in EDGE_RANGES:
    for _i, (_op, _obs) in enumerate(EDGE_FRAMES[_fid], 1):
        EDGE_EVENTS.append((_start + _i - 1, _fid, _i, _op, _obs))

# (from, through, events, [(fid,start,end,count)]) for edge-store pulls
EDGE_DELIVERIES = {
    # edge registered from seq 1; target 3 -> frames 1 (2) + 2 (1)
    "edge_1": (1, 3, 3, [(1, 1, 2, 2), (2, 3, 3, 1)]),
    # frame 3 (seq 4): created, sender killed, replayed with the same token
    "edge_2": (4, 4, 1, [(3, 4, 4, 1)]),
    # e5 arrives (frame 5, seq 6); this delivery is then revoked by disable
    "edge_4": (6, 6, 1, [(5, 6, 6, 1)]),
    # consumer "other" registered with --from-seq 5 (acked 4); by the time it
    # pulls, e5 has also arrived -> frames 4 and 5
    "edge_other": (5, 6, 2, [(4, 5, 5, 1), (5, 6, 6, 1)]),
    # generation 2 re-enables from seq 1: replay all five retained frames
    "edge_replay_1": (1, 6, 6, [(1, 1, 2, 2), (2, 3, 3, 1), (3, 4, 4, 1),
                               (4, 5, 5, 1), (5, 6, 6, 1)]),
    # catch-up after e6/e7: edge (acked 6) gets frames 6 and 7
    "edge_catch_up": (7, 8, 2, [(6, 7, 7, 1), (7, 8, 8, 1)]),
    # other (acked 6) gets frames 6 and 7
    "other_catch_up": (7, 8, 2, [(6, 7, 7, 1), (7, 8, 8, 1)]),
    # ra registered with --from-seq 6 (acked 5) -> same three frames
    "ra_catch_up": (6, 8, 3, [(5, 6, 6, 1), (6, 7, 7, 1), (7, 8, 8, 1)]),
}

# checkpoints for the edge store
EDGE_CHECKPOINTS = {
    # after e1..e3 and the crash/replay/ack tests: edge acked 4, nothing pending
    "edge_ready": {
        "frames": EDGE_RANGES[:3],
        "events": EDGE_EVENTS[:4],
        "consumers": {
            "edge": {"generation": 1, "start_seq": 1, "acked_seq": 4,
                     "enabled": True},
        },
        "retention_floor": 0,
        "tail_seq": 4,
        "pending": {},
    },
    # after four concurrent pulls of the same consumer: exactly ONE pending
    # delivery for seq 5 (frame 4); one token, one range
    "edge_concurrent_pending": {
        "frames": EDGE_RANGES[:4],
        "events": EDGE_EVENTS[:5],
        "consumers": {
            "edge": {"generation": 1, "start_seq": 1, "acked_seq": 4,
                     "enabled": True},
        },
        "retention_floor": 0,
        "tail_seq": 5,
        "pending": {"edge": {"from_seq": 5, "through_seq": 5}},
    },
    # edge disabled (its pending frame-5 delivery revoked; acked stays 5);
    # "other" is enabled on its own position 5
    "edge_disabled": {
        "frames": EDGE_RANGES[:5],
        "events": EDGE_EVENTS[:6],
        "consumers": {
            "edge": {"generation": 1, "start_seq": 1, "acked_seq": 5,
                     "enabled": False},
            "other": {"generation": 1, "start_seq": 5, "acked_seq": 6,
                      "enabled": True},
        },
        "retention_floor": 0,
        "tail_seq": 6,
        "pending": {},
    },
    # edge re-enabled as generation 2 from seq 1 and replayed everything
    "edge_reborn": {
        "frames": EDGE_RANGES[:5],
        "events": EDGE_EVENTS[:6],
        "consumers": {
            "edge": {"generation": 2, "start_seq": 1, "acked_seq": 6,
                     "enabled": True},
            "other": {"generation": 1, "start_seq": 5, "acked_seq": 6,
                      "enabled": True},
        },
        "retention_floor": 0,
        "tail_seq": 6,
        "pending": {},
    },
    # e6/e7 imported; edge/other/ra all caught up to 8
    "edge_events": {
        "frames": EDGE_RANGES,
        "events": EDGE_EVENTS,
        "consumers": {
            "edge": {"generation": 2, "start_seq": 1, "acked_seq": 8,
                     "enabled": True},
            "other": {"generation": 1, "start_seq": 5, "acked_seq": 8,
                      "enabled": True},
            "ra": {"generation": 1, "start_seq": 6, "acked_seq": 8,
                   "enabled": True},
        },
        "retention_floor": 0,
        "tail_seq": 8,
        "pending": {},
    },
    # race A: an explicit-bound trim committed floor 4 first; registration
    # from seq 2 is refused and leaves no consumer row
    "race_a": {
        "frames": EDGE_RANGES[3:],
        "consumers": {
            "edge": {"generation": 2, "start_seq": 1, "acked_seq": 8,
                     "enabled": True},
            "other": {"generation": 1, "start_seq": 5, "acked_seq": 8,
                      "enabled": True},
            "ra": {"generation": 1, "start_seq": 6, "acked_seq": 8,
                   "enabled": True},
        },
        "retention_floor": 4,
        "tail_seq": 8,
        "refused_consumer_absent": "race_old",
    },
    # race B: race_keep (start 5) committed first; the trim waiting on the
    # same lock is capped by race_keep's position, so floor stays 4 and the
    # events race_keep was promised (seq 5) are still retained
    "race_b": {
        "frames": EDGE_RANGES[3:],
        "retention_floor": 4,
        "tail_seq": 8,
        "consumers": {
            "edge": {"generation": 2, "start_seq": 1, "acked_seq": 8,
                     "enabled": True},
            "other": {"generation": 1, "start_seq": 5, "acked_seq": 8,
                      "enabled": True},
            "ra": {"generation": 1, "start_seq": 6, "acked_seq": 8,
                   "enabled": True},
            "race_keep": {"generation": 1, "start_seq": 5, "acked_seq": 4,
                          "enabled": True},
        },
    },
    # all consumers disabled; an explicit upper bound 6 trims frames 4, 5
    "no_consumers": {
        "enabled": [],
        "retention_floor": 6,
        "tail_seq": 8,
        "retained_frames": EDGE_RANGES[5:],   # frames 6, 7 (seqs 7, 8)
    },
}


# ---------------------------------------------------------------- big frame
BIG_EVENTS = 120000
