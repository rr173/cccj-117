"""Hand-written expected answers for the demo walkthrough.

Every value below was computed by hand from the fixture files in
demo/fixtures/ - NOT by running the rvault implementation.  verify.py reads
the real SQLite database and bundle files and compares them field by field
against these literals.

Tuple shapes:
  edition row : (study, observation, edition, person, display_name, measure, memo, batch_tag)
  projection  : (observation, person, edition, display_name, measure, memo)
  owner row   : (study, observation, person)
  barrier row : (study, person)
  receipt item: (line, study, observation, person, edition, disposition)
"""

# ---------------------------------------------------------------- editions
A1E1 = ("alpha", "A1", 1, "ann", "Ann Lee", 10, "ann-e1", "alpha-b1")
A1E2 = ("alpha", "A1", 2, "ann", "Ann Lee", 11, "ann-e2", "alpha-b2")
A1E3 = ("alpha", "A1", 3, "ann", "Ann Lee", 12, "ann-e3", "alpha-b3")
A2E1 = ("alpha", "A2", 1, "bob", "Bob Kim", 20, "bob-e1", "alpha-b1")
A2E2 = ("alpha", "A2", 2, "bob", "Bob Kim", 21, "bob-e2", "alpha-b2")
A2E3 = ("alpha", "A2", 3, "bob", "Bob Kim", 22, "bob-e3", "alpha-b4")
A3E1 = ("alpha", "A3", 1, "cid", "Cid Fox", 30, "cid-e1", "alpha-b1")
A3E2 = ("alpha", "A3", 2, "cid", "Cid Fox", 31, "cid-e2", "alpha-b4")
A4E1 = ("alpha", "A4", 1, "ann", "Ann Lee", 40, "ann-a4", "alpha-b1")
A8E1 = ("alpha", "A8", 1, "bob", "Bob Kim", 80, "bob-a8", "alpha-bz")
B1E1 = ("beta", "B1", 1, "ann", "Ann Lee", 100, "ann-b1", "beta-b1")
B2E1 = ("beta", "B2", 1, "dan", "Dan Yu", 200, "dan-e1", "beta-b1")
C1E1 = ("conc", "C1", 1, "peg", "Peg Ho", 1, "peg-e1", "conc-b1")
C2E1 = ("conc", "C2", 1, "quinn", "Quinn Ao", 2, "quinn-e1", "conc-b1")
L1E1 = ("lockt", "L1", 1, "lo", "Lo Ren", 5, "lo-e1", "lockt-b1")


def pj(row):
    """projection tuple of an edition row"""
    return (row[1], row[3], row[2], row[4], row[5], row[6])


# ---------------------------------------------------------------- owners
OWNERS_BASE = [
    ("alpha", "A1", "ann"), ("alpha", "A2", "bob"), ("alpha", "A3", "cid"),
    ("alpha", "A4", "ann"), ("beta", "B1", "ann"), ("beta", "B2", "dan"),
]
OWNERS_CP5 = sorted(OWNERS_BASE + [("alpha", "A7", "zed"), ("alpha", "A8", "bob")])
OWNERS_CP7 = sorted(OWNERS_CP5 + [("alpha", "A5", "ann")])
OWNERS_CP16 = sorted(OWNERS_CP7 + [("conc", "C1", "peg"), ("conc", "C2", "quinn"),
                                   ("lockt", "L1", "lo")])

# ---------------------------------------------------------------- bundles
BUNDLE_B000001 = {
    "bundle_id": "B000001", "study": "alpha",
    "participants": ["ann", "bob", "cid"],
    "observations": [
        {"observation": "A1", "person": "ann", "edition": 3,
         "display_name": "Ann Lee", "measure": 12, "memo": "ann-e3"},
        {"observation": "A2", "person": "bob", "edition": 2,
         "display_name": "Bob Kim", "measure": 21, "memo": "bob-e2"},
        {"observation": "A3", "person": "cid", "edition": 1,
         "display_name": "Cid Fox", "measure": 30, "memo": "cid-e1"},
        {"observation": "A4", "person": "ann", "edition": 1,
         "display_name": "Ann Lee", "measure": 40, "memo": "ann-a4"},
    ],
    # G1 = {ann, bob}: 12 + 40 + 21 = 73 ; G2 = {cid}: 30
    "aggregates": [
        {"group_code": "G1", "persons": 2, "measure_total": 73},
        {"group_code": "G2", "persons": 1, "measure_total": 30},
    ],
}

_BETA_OBS = [
    {"observation": "B1", "person": "ann", "edition": 1,
     "display_name": "Ann Lee", "measure": 100, "memo": "ann-b1"},
    {"observation": "B2", "person": "dan", "edition": 1,
     "display_name": "Dan Yu", "measure": 200, "memo": "dan-e1"},
]
_BETA_AGG = [
    {"group_code": "G1", "persons": 1, "measure_total": 100},
    {"group_code": "G2", "persons": 1, "measure_total": 200},
]
BUNDLE_B000002 = {"bundle_id": "B000002", "study": "beta",
                  "participants": ["ann", "dan"],
                  "observations": _BETA_OBS, "aggregates": _BETA_AGG}
BUNDLE_B000003 = {"bundle_id": "B000003", "study": "beta",
                  "participants": ["ann", "dan"],
                  "observations": _BETA_OBS, "aggregates": _BETA_AGG}

BUNDLE_B000004 = {
    "bundle_id": "B000004", "study": "alpha",
    "participants": ["bob", "cid"],
    "observations": [
        {"observation": "A2", "person": "bob", "edition": 3,
         "display_name": "Bob Kim", "measure": 22, "memo": "bob-e3"},
        {"observation": "A3", "person": "cid", "edition": 2,
         "display_name": "Cid Fox", "measure": 31, "memo": "cid-e2"},
        {"observation": "A8", "person": "bob", "edition": 1,
         "display_name": "Bob Kim", "measure": 80, "memo": "bob-a8"},
    ],
    # G1 = {bob}: 22 + 80 = 102 ; G2 = {cid}: 31
    "aggregates": [
        {"group_code": "G1", "persons": 1, "measure_total": 102},
        {"group_code": "G2", "persons": 1, "measure_total": 31},
    ],
}

BUNDLE_B000005 = {
    "bundle_id": "B000005", "study": "alpha",
    "participants": ["bob"],
    "observations": [
        {"observation": "A2", "person": "bob", "edition": 3,
         "display_name": "Bob Kim", "measure": 22, "memo": "bob-e3"},
        {"observation": "A8", "person": "bob", "edition": 1,
         "display_name": "Bob Kim", "measure": 80, "memo": "bob-a8"},
    ],
    "aggregates": [{"group_code": "G1", "persons": 1, "measure_total": 102}],
}

_BETA_ANN_OBS = [
    {"observation": "B1", "person": "ann", "edition": 1,
     "display_name": "Ann Lee", "measure": 100, "memo": "ann-b1"},
]
_BETA_ANN_AGG = [{"group_code": "G1", "persons": 1, "measure_total": 100}]
BUNDLE_B000006 = {"bundle_id": "B000006", "study": "beta", "participants": ["ann"],
                  "observations": _BETA_ANN_OBS, "aggregates": _BETA_ANN_AGG}
BUNDLE_B000007 = {"bundle_id": "B000007", "study": "beta", "participants": ["ann"],
                  "observations": _BETA_ANN_OBS, "aggregates": _BETA_ANN_AGG}

BUNDLE_B000008 = {
    "bundle_id": "B000008", "study": "conc",
    "participants": ["quinn"],
    "observations": [
        {"observation": "C2", "person": "quinn", "edition": 1,
         "display_name": "Quinn Ao", "measure": 2, "memo": "quinn-e1"},
    ],
    "aggregates": [{"group_code": "G2", "persons": 1, "measure_total": 2}],
}

BUNDLE_B000009 = {
    "bundle_id": "B000009", "study": "lockt",
    "participants": ["lo"],
    "observations": [
        {"observation": "L1", "person": "lo", "edition": 1,
         "display_name": "Lo Ren", "measure": 5, "memo": "lo-e1"},
    ],
    "aggregates": [{"group_code": "UNGROUPED", "persons": 1, "measure_total": 5}],
}

# ---------------------------------------------------------------- receipts
RCP_ALPHA_B1_ALL_OK = [
    (1, "alpha", "A1", "ann", 1, "accepted"),
    (2, "alpha", "A2", "bob", 1, "accepted"),
    (3, "alpha", "A3", "cid", 1, "accepted"),
    (4, "alpha", "A4", "ann", 1, "accepted"),
]
RCP_ALPHA_B1_AFTER_W1 = [
    (1, "alpha", "A1", "ann", 1, "withdrawn_suppressed"),
    (2, "alpha", "A2", "bob", 1, "accepted"),
    (3, "alpha", "A3", "cid", 1, "accepted"),
    (4, "alpha", "A4", "ann", 1, "withdrawn_suppressed"),
]
RCP_ALPHA_BZ = [
    (1, "alpha", "A7", "zed", 1, "withdrawn_suppressed"),
    (2, "alpha", "A8", "bob", 1, "accepted"),
]
RCP_ALPHA_B4 = [
    (1, "alpha", "A1", "ann", 4, "withdrawn_suppressed"),
    (2, "alpha", "A2", "bob", 3, "accepted"),
    (3, "alpha", "A5", "ann", 1, "withdrawn_suppressed"),
    (4, "alpha", "A3", "cid", 2, "accepted"),
]
RCP_ALPHA_B7 = [(1, "alpha", "A2", "bob", 1, "accepted_existing")]

# ---------------------------------------------------------------- editions per phase
ED_CP1 = [A1E1, A1E2, A1E3, A2E1, A2E2, A3E1, A4E1, B1E1, B2E1]
ED_CP5 = sorted(ED_CP1 + [A8E1])
ED_CP6 = [A2E1, A2E2, A3E1, A8E1, B1E1, B2E1]
ED_CP7 = [A2E1, A2E2, A2E3, A3E1, A3E2, A8E1, B1E1, B2E1]
ED_CP9 = [A2E1, A2E2, A2E3, A8E1, B1E1, B2E1]
ED_CP13 = [A2E1, A2E2, A2E3, A8E1, B1E1]
ED_CP14 = [A2E1, A2E2, A2E3, A8E1]
ED_CP16 = sorted(ED_CP14 + [L1E1])

# ---------------------------------------------------------------- projections
PJ_ALPHA_CP1 = [pj(A1E3), pj(A2E2), pj(A3E1), pj(A4E1)]
PJ_ALPHA_CP5 = PJ_ALPHA_CP1 + [pj(A8E1)]
PJ_ALPHA_CP6 = [pj(A2E2), pj(A3E1), pj(A8E1)]
PJ_ALPHA_CP7 = [pj(A2E3), pj(A3E2), pj(A8E1)]
PJ_ALPHA_CP9 = [pj(A2E3), pj(A8E1)]
PJ_BETA_CP1 = [pj(B1E1), pj(B2E1)]
PJ_BETA_B1_ONLY = [pj(B1E1)]

# ---------------------------------------------------------------- checkpoints
CHECKPOINTS = {
    "cp1": {
        "editions": ED_CP1,
        "owners": OWNERS_BASE,
        "projection_alpha": PJ_ALPHA_CP1,
        "projection_beta": PJ_BETA_CP1,
        "barrier": [],
        "withdrawals": {},
        "withdrawal_bundles": {},
        "bundles_table": [],
        "bundle_files": {},
        "files_absent": [],
        "receipts": {"alpha-b1": RCP_ALPHA_B1_ALL_OK},
        "batches": {"alpha-b1": (4, 0), "alpha-b2": (2, 0),
                    "alpha-b3": (1, 0), "beta-b1": (2, 0)},
    },
    # duplicate resend of alpha-b1 (pre-withdrawal): nothing changes, receipt
    # still shows every item accepted
    "cp2": {
        "editions": ED_CP1,
        "owners": OWNERS_BASE,
        "projection_alpha": PJ_ALPHA_CP1,
        "barrier": [],
        "receipts": {"alpha-b1": RCP_ALPHA_B1_ALL_OK},
        "batches": {"alpha-b1": (4, 0), "alpha-b2": (2, 0),
                    "alpha-b3": (1, 0), "beta-b1": (2, 0)},
    },
    # tampered resend + corrupt batch + ownership conflict: all rejected,
    # state identical to cp1, no stray batches or observations
    "cp3": {
        "editions": ED_CP1,
        "owners": OWNERS_BASE,
        "projection_alpha": PJ_ALPHA_CP1,
        "batches": {"alpha-b1": (4, 0), "alpha-b2": (2, 0),
                    "alpha-b3": (1, 0), "beta-b1": (2, 0)},
        "batches_absent": ["alpha-bad", "alpha-bad2"],
    },
    "cp4": {
        "editions": ED_CP1,
        "bundles_table": [("B000001", "alpha", "active"),
                          ("B000002", "beta", "active"),
                          ("B000003", "beta", "active")],
        "bundle_files": {"B000001": BUNDLE_B000001,
                         "B000002": BUNDLE_B000002,
                         "B000003": BUNDLE_B000003},
        "files_absent": [],
    },
    # W0 registered for zed before any zed data exists; alpha-bz then has
    # zed suppressed and bob accepted
    "cp5": {
        "editions": ED_CP5,
        "owners": OWNERS_CP5,
        "projection_alpha": PJ_ALPHA_CP5,
        "barrier": [("alpha", "zed")],
        "withdrawals": {"W0": ("alpha", "completed", 0)},
        "withdrawal_bundles": {"W0": {}},
        "receipts": {"alpha-bz": RCP_ALPHA_BZ},
        "batches": {"alpha-b1": (4, 0), "alpha-b2": (2, 0), "alpha-b3": (1, 0),
                    "beta-b1": (2, 0), "alpha-bz": (1, 1)},
    },
    # W1 (alpha, {ann}) completed: ann's alpha editions and B000001 gone,
    # beta's ann row untouched
    "cp6": {
        "editions": ED_CP6,
        "owners": OWNERS_CP5,
        "projection_alpha": PJ_ALPHA_CP6,
        "projection_beta": PJ_BETA_CP1,
        "barrier": [("alpha", "ann"), ("alpha", "zed")],
        "withdrawals": {"W0": ("alpha", "completed", 0),
                        "W1": ("alpha", "completed", 4)},
        "withdrawal_bundles": {"W0": {}, "W1": {"B000001": "cleared"}},
        "bundles_table": [("B000001", "alpha", "deleted"),
                          ("B000002", "beta", "active"),
                          ("B000003", "beta", "active")],
        "bundle_files": {"B000002": BUNDLE_B000002, "B000003": BUNDLE_B000003},
        "files_absent": ["B000001"],
    },
    # post-withdrawal traffic: new/old editions for ann suppressed, others
    # accepted; alpha-b1 resent -> duplicate receipt now shows suppression;
    # changed old edition rejected; identical old edition idempotent
    "cp7": {
        "editions": ED_CP7,
        "owners": OWNERS_CP7,
        "projection_alpha": PJ_ALPHA_CP7,
        "barrier": [("alpha", "ann"), ("alpha", "zed")],
        "receipts": {"alpha-b1": RCP_ALPHA_B1_AFTER_W1,
                     "alpha-b4": RCP_ALPHA_B4,
                     "alpha-b7": RCP_ALPHA_B7},
        "batches": {"alpha-b1": (4, 0), "alpha-b2": (2, 0), "alpha-b3": (1, 0),
                    "beta-b1": (2, 0), "alpha-bz": (1, 1), "alpha-b4": (2, 2),
                    "alpha-b7": (1, 0)},
        "batches_absent": ["alpha-b6"],
    },
    "cp8": {
        "editions": ED_CP7,
        "bundles_table": [("B000001", "alpha", "deleted"),
                          ("B000002", "beta", "active"),
                          ("B000003", "beta", "active"),
                          ("B000004", "alpha", "active")],
        "bundle_files": {"B000002": BUNDLE_B000002, "B000003": BUNDLE_B000003,
                         "B000004": BUNDLE_B000004},
        "files_absent": ["B000001"],
    },
    # W2 (alpha, {ann, cid}) overlaps W1; cid's editions and B000004 removed
    "cp9": {
        "editions": ED_CP9,
        "projection_alpha": PJ_ALPHA_CP9,
        "projection_beta": PJ_BETA_CP1,
        "barrier": [("alpha", "ann"), ("alpha", "cid"), ("alpha", "zed")],
        "withdrawals": {"W0": ("alpha", "completed", 0),
                        "W1": ("alpha", "completed", 4),
                        "W2": ("alpha", "completed", 2)},
        "withdrawal_bundles": {"W0": {}, "W1": {"B000001": "cleared"},
                               "W2": {"B000004": "cleared"}},
        "bundles_table": [("B000001", "alpha", "deleted"),
                          ("B000002", "beta", "active"),
                          ("B000003", "beta", "active"),
                          ("B000004", "alpha", "deleted")],
        "bundle_files": {"B000002": BUNDLE_B000002, "B000003": BUNDLE_B000003},
        "files_absent": ["B000001", "B000004"],
    },
    # retry of completed W1 is a no-op; W1 id with a changed set conflicts
    "cp10": {
        "editions": ED_CP9,
        "projection_alpha": PJ_ALPHA_CP9,
        "barrier": [("alpha", "ann"), ("alpha", "cid"), ("alpha", "zed")],
        "withdrawals": {"W0": ("alpha", "completed", 0),
                        "W1": ("alpha", "completed", 4),
                        "W2": ("alpha", "completed", 2)},
    },
    "cp11": {
        "editions": ED_CP9,
        "bundles_table": [("B000001", "alpha", "deleted"),
                          ("B000002", "beta", "active"),
                          ("B000003", "beta", "active"),
                          ("B000004", "alpha", "deleted"),
                          ("B000005", "alpha", "active")],
        "bundle_files": {"B000002": BUNDLE_B000002, "B000003": BUNDLE_B000003,
                         "B000005": BUNDLE_B000005},
        "files_absent": ["B000001", "B000004"],
    },
    # W4 killed right after registration: barrier already hides dan from the
    # projection, but his edition rows are still physically present
    "cp12": {
        "editions": ED_CP9,
        "projection_beta": PJ_BETA_B1_ONLY,
        "barrier": [("alpha", "ann"), ("alpha", "cid"), ("alpha", "zed"),
                    ("beta", "dan")],
        "withdrawals": {"W0": ("alpha", "completed", 0),
                        "W1": ("alpha", "completed", 4),
                        "W2": ("alpha", "completed", 2),
                        "W4": ("beta", "registered", 0)},
        "bundles_table": [("B000001", "alpha", "deleted"),
                          ("B000002", "beta", "active"),
                          ("B000003", "beta", "active"),
                          ("B000004", "alpha", "deleted"),
                          ("B000005", "alpha", "active")],
    },
    # resumed W4: B2 row deleted, B000002 cleared, B000003 was lost externally
    # and is recorded as missing
    "cp13": {
        "editions": ED_CP13,
        "projection_beta": PJ_BETA_B1_ONLY,
        "withdrawals": {"W0": ("alpha", "completed", 0),
                        "W1": ("alpha", "completed", 4),
                        "W2": ("alpha", "completed", 2),
                        "W4": ("beta", "completed", 1)},
        "withdrawal_bundles": {"W4": {"B000002": "cleared", "B000003": "missing"}},
        "bundles_table": [("B000001", "alpha", "deleted"),
                          ("B000002", "beta", "deleted"),
                          ("B000003", "beta", "deleted"),
                          ("B000004", "alpha", "deleted"),
                          ("B000005", "alpha", "active")],
        "bundle_files": {"B000005": BUNDLE_B000005},
        "files_absent": ["B000001", "B000002", "B000003", "B000004"],
    },
    # W7 killed after editions cleared and the first of two bundles deleted
    "cp14": {
        "editions": ED_CP14,
        "projection_beta": [],
        "withdrawals": {"W0": ("alpha", "completed", 0),
                        "W1": ("alpha", "completed", 4),
                        "W2": ("alpha", "completed", 2),
                        "W4": ("beta", "completed", 1),
                        "W7": ("beta", "editions_cleared", 1)},
        "withdrawal_bundles": {"W7": {"B000006": "cleared", "B000007": "pending"}},
        "bundles_table": [("B000001", "alpha", "deleted"),
                          ("B000002", "beta", "deleted"),
                          ("B000003", "beta", "deleted"),
                          ("B000004", "alpha", "deleted"),
                          ("B000005", "alpha", "active"),
                          ("B000006", "beta", "deleted"),
                          ("B000007", "beta", "active")],
        "bundle_files": {"B000005": BUNDLE_B000005, "B000007": BUNDLE_B000007},
        "files_absent": ["B000001", "B000002", "B000003", "B000004", "B000006"],
    },
    "cp15": {
        "editions": ED_CP14,
        "projection_beta": [],
        "withdrawals": {"W0": ("alpha", "completed", 0),
                        "W1": ("alpha", "completed", 4),
                        "W2": ("alpha", "completed", 2),
                        "W4": ("beta", "completed", 1),
                        "W7": ("beta", "completed", 1)},
        "withdrawal_bundles": {"W7": {"B000006": "cleared", "B000007": "cleared"}},
        "bundles_table": [("B000001", "alpha", "deleted"),
                          ("B000002", "beta", "deleted"),
                          ("B000003", "beta", "deleted"),
                          ("B000004", "alpha", "deleted"),
                          ("B000005", "alpha", "active"),
                          ("B000006", "beta", "deleted"),
                          ("B000007", "beta", "deleted")],
        "bundle_files": {"B000005": BUNDLE_B000005},
        "files_absent": ["B000001", "B000002", "B000003", "B000004",
                         "B000006", "B000007"],
    },
    # concurrency: run A bundle aborted (no B000008 leak at that point),
    # run B's B000008 registered then deleted by W6; lockt's B000009 proves
    # the study lock survived a SIGKILLed holder
    "cp16": {
        "editions": ED_CP16,
        "owners": OWNERS_CP16,
        "projection_conc": [],
        "projection_lockt": [pj(L1E1)],
        "barrier": [("alpha", "ann"), ("alpha", "cid"), ("alpha", "zed"),
                    ("beta", "ann"), ("beta", "dan"),
                    ("conc", "peg"), ("conc", "quinn")],
        "withdrawals": {"W0": ("alpha", "completed", 0),
                        "W1": ("alpha", "completed", 4),
                        "W2": ("alpha", "completed", 2),
                        "W4": ("beta", "completed", 1),
                        "W5": ("conc", "completed", 1),
                        "W6": ("conc", "completed", 1),
                        "W7": ("beta", "completed", 1)},
        "withdrawal_bundles": {"W5": {}, "W6": {"B000008": "cleared"}},
        "bundles_table": [("B000001", "alpha", "deleted"),
                          ("B000002", "beta", "deleted"),
                          ("B000003", "beta", "deleted"),
                          ("B000004", "alpha", "deleted"),
                          ("B000005", "alpha", "active"),
                          ("B000006", "beta", "deleted"),
                          ("B000007", "beta", "deleted"),
                          ("B000008", "conc", "deleted"),
                          ("B000009", "lockt", "active")],
        "bundle_files": {"B000005": BUNDLE_B000005, "B000009": BUNDLE_B000009},
        "files_absent": ["B000001", "B000002", "B000003", "B000004",
                         "B000006", "B000007", "B000008"],
        "tmp_empty": True,
    },
    # final residue audit: no withdrawn person's payload anywhere in the
    # managed store; untouched participants still present
    "cp17": {
        "residue": {
            "forbidden": ["Ann Lee", "Cid Fox", "Dan Yu", "Zed Wu",
                          "Peg Ho", "Quinn Ao",
                          "ann-e1", "ann-e2", "ann-e3", "ann-a4", "ann-b1",
                          "cid-e1", "cid-e2", "dan-e1", "zed-e1",
                          "peg-e1", "quinn-e1",
                          "ann-e4", "ann-a5"],
            "required_names": ["Bob Kim", "Lo Ren"],
            "withdrawn_persons": ["ann", "cid", "zed", "dan", "peg", "quinn"],
        },
        "tmp_empty": True,
    },
    # fault injection: the registry path of B000010 was tampered to point
    # outside the managed area.  The withdrawal still erases the canonical
    # bundle file, flags the anomaly, and the outside file is untouched
    # (run_demo asserts its bytes survive).
    "cp18": {
        "editions": ED_CP16,
        "barrier": [("alpha", "ann"), ("alpha", "cid"), ("alpha", "zed"),
                    ("beta", "ann"), ("beta", "dan"),
                    ("conc", "peg"), ("conc", "quinn"),
                    ("vandal", "vic")],
        "withdrawals": {"W0": ("alpha", "completed", 0),
                        "W1": ("alpha", "completed", 4),
                        "W2": ("alpha", "completed", 2),
                        "W4": ("beta", "completed", 1),
                        "W5": ("conc", "completed", 1),
                        "W6": ("conc", "completed", 1),
                        "W7": ("beta", "completed", 1),
                        "WV": ("vandal", "completed", 1)},
        "withdrawal_anomalies": {"WV": 1},
        "withdrawal_bundles": {"WV": {"B000010": "cleared"}},
        "bundles_table": [("B000001", "alpha", "deleted"),
                          ("B000002", "beta", "deleted"),
                          ("B000003", "beta", "deleted"),
                          ("B000004", "alpha", "deleted"),
                          ("B000005", "alpha", "active"),
                          ("B000006", "beta", "deleted"),
                          ("B000007", "beta", "deleted"),
                          ("B000008", "conc", "deleted"),
                          ("B000009", "lockt", "active"),
                          ("B000010", "vandal", "deleted")],
        "files_absent": ["B000001", "B000002", "B000003", "B000004",
                         "B000006", "B000007", "B000008", "B000010"],
        "residue": {
            "forbidden": ["Ann Lee", "Cid Fox", "Dan Yu", "Zed Wu",
                          "Peg Ho", "Quinn Ao", "Vic Mo",
                          "ann-e1", "ann-e2", "ann-e3", "ann-a4", "ann-b1",
                          "cid-e1", "cid-e2", "dan-e1", "zed-e1",
                          "peg-e1", "quinn-e1", "vic-e1",
                          "ann-e4", "ann-a5"],
            "required_names": ["Bob Kim", "Lo Ren"],
            "withdrawn_persons": ["ann", "cid", "zed", "dan", "peg", "quinn",
                                  "vic"],
        },
        "tmp_empty": True,
    },
}
