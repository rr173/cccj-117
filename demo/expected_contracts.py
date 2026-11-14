"""Hand-computed expectations for the versioned input-contract demo.

Every list below was computed BY HAND from the fixture documents in
demo/contract_fixtures/ - not by running rvault.  verify_contracts.py reads
the store's SQLite database and receipt files directly (its own SQL, no
rvault code) and compares them field by field against these literals.

Contract tower built by the demo:

    labA / obs
      v1 (initial)                    7 fields, 5 field-definitions with
                                      names that differ from the canonical
                                      intake names
        `- v2 (compatible upgrade)    adds optional `site` with default
                                      "unknown"; relaxes `note` nullability;
                                      widens `value` integer -> number
             `- v3 (new branch)        removes `note`, adds REQUIRED `flag`

    labB / measurement
      v1 (initial)                    9 fields, different names AND a
                                      different type mix (boolean `verified`,
                                      integer `reading`, optional `batch`)
        `- v2 (compatible upgrade)    widens `batch` integer -> number only
"""

# full published-version tuples: (version, predecessor, compatibility)
LABA_OBS_VERSIONS = [(1, None, "initial"), (2, 1, "compatible"), (3, 2, "branch")]
LABB_MEAS_VERSIONS = [(1, None, "initial"), (2, 1, "compatible")]

# v2 change list against v1, sorted by (field, kind) - hand-computed.
LABA_V2_CHANGES = [
    ("note", "nullable_relaxed", True),
    ("site", "field_added", True),
    ("value", "type_widened", True),
]
# v3 change list against v2, sorted by (field, kind).
LABA_V3_CHANGES = [
    ("flag", "field_added", False),
    ("note", "field_removed", False),
]
LABB_V2_CHANGES = [("batch", "type_widened", True)]

# Per-field change lists of successful registers, as printed by the command.
LABA_V2_REGISTER_STATUS = ("recorded", "compatible")
LABA_V3_REGISTER_STATUS = ("recorded", "branch_required")
LABB_V2_REGISTER_STATUS = ("recorded", "compatible")

# Field definitions stored per contract version, ordered by position:
# (name, type, required, nullable, has_default, default, target)
LABA_V1_FIELDS = [
    ("study", "string", 1, 0, 0, None, "study"),
    ("person", "string", 1, 0, 0, None, "person"),
    ("obs_id", "string", 1, 0, 0, None, "observation"),
    ("rev", "integer", 1, 0, 0, None, "edition"),
    ("label", "string", 1, 0, 0, None, "display_name"),
    ("value", "integer", 1, 0, 0, None, "measure"),
    ("note", "string", 0, 0, 1, "", "memo"),
]
LABA_V2_FIELDS = [
    ("study", "string", 1, 0, 0, None, "study"),
    ("person", "string", 1, 0, 0, None, "person"),
    ("obs_id", "string", 1, 0, 0, None, "observation"),
    ("rev", "integer", 1, 0, 0, None, "edition"),
    ("label", "string", 1, 0, 0, None, "display_name"),
    ("value", "number", 1, 0, 0, None, "measure"),
    ("note", "string", 0, 1, 1, "", "memo"),
    ("site", "string", 0, 0, 1, "unknown", None),
]
LABA_V3_FIELDS = [
    ("study", "string", 1, 0, 0, None, "study"),
    ("person", "string", 1, 0, 0, None, "person"),
    ("obs_id", "string", 1, 0, 0, None, "observation"),
    ("rev", "integer", 1, 0, 0, None, "edition"),
    ("label", "string", 1, 0, 0, None, "display_name"),
    ("value", "number", 1, 0, 0, None, "measure"),
    ("flag", "string", 1, 0, 0, None, "memo"),
    ("site", "string", 0, 0, 1, "unknown", None),
]
LABB_V1_FIELDS = [
    ("study", "string", 1, 0, 0, None, "study"),
    ("subject", "string", 1, 0, 0, None, "person"),
    ("sample", "string", 1, 0, 0, None, "observation"),
    ("attempt", "integer", 1, 0, 0, None, "edition"),
    ("title", "string", 1, 0, 0, None, "display_name"),
    ("reading", "integer", 1, 0, 0, None, "measure"),
    ("remark", "string", 0, 0, 1, "", "memo"),
    ("verified", "boolean", 0, 0, 1, False, None),
    ("batch", "integer", 0, 0, 1, 0, None),
]
LABB_V2_FIELDS = [
    ("study", "string", 1, 0, 0, None, "study"),
    ("subject", "string", 1, 0, 0, None, "person"),
    ("sample", "string", 1, 0, 0, None, "observation"),
    ("attempt", "integer", 1, 0, 0, None, "edition"),
    ("title", "string", 1, 0, 0, None, "display_name"),
    ("reading", "integer", 1, 0, 0, None, "measure"),
    ("remark", "string", 0, 0, 1, "", "memo"),
    ("verified", "boolean", 0, 0, 1, False, None),
    ("batch", "number", 0, 0, 1, 0, None),
]

CONTRACT_FIELDS = {
    ("labA", "obs", 1): LABA_V1_FIELDS,
    ("labA", "obs", 2): LABA_V2_FIELDS,
    ("labA", "obs", 3): LABA_V3_FIELDS,
    ("labB", "measurement", 1): LABB_V1_FIELDS,
    ("labB", "measurement", 2): LABB_V2_FIELDS,
}

# ---------------------------------------------------------------------------
# Rejected registrations: (document, expected code, expected change entries).
# Entries that must be present in the change list (field, kind).  A rejected
# register finishes with exit code 2 and leaves the contract tower unchanged.
# ---------------------------------------------------------------------------
REJECTED_REGISTERS = [
    # attacked against labA obs v2 (candidate would be v3, but v3 is taken
    # by the branch; the demo applies these to a SEPARATE provider clone to
    # keep the numbering clean - see run script)
    ("delete_field", "incompatible_change", [("site", "field_removed")]),
    ("tighten_nullable", "incompatible_change", [("note", "nullable_tightened")]),
    ("narrow_type", "incompatible_change", [("value", "type_narrowed")]),
    ("add_required", "incompatible_change", [("phase", "field_added")]),
    ("change_meaning", "incompatible_change",
     [("label", "target_changed"), ("note", "target_changed")]),
    ("required_change", "incompatible_change",
     [("site", "default_removed"), ("site", "required_changed")]),
    ("default_change", "incompatible_change", [("site", "default_changed")]),
    ("null_default", "invalid_contract", []),
    ("empty_field_name", "invalid_contract", []),
]

# Registry-structure rejections (no field list needed).
REGISTRY_REJECTIONS = [
    ("version_gap", "version_gap"),
    ("unknown_predecessor", "unknown_predecessor"),
    ("first_not_one", "version_gap"),
    ("first_with_predecessor", "invalid_contract_selection"),
    ("content_conflict", "content_conflict"),
    ("disguised_compatible", "incompatible_change"),
]

# ---------------------------------------------------------------------------
# Import expectations.  Stored rows are canonical intake tuples:
# (study, observation, edition, person, display_name, measure, memo, batch_tag)
# ---------------------------------------------------------------------------
LABA_GOOD_ROWS = [
    # note omitted -> default "" ; site is contract metadata, never stored
    ("laba", "L1", 1, "pa-1", "Alpha One", 10, "", "a-good"),
    # note explicitly null and nullable in v2 -> resolves to its default ""
    ("laba", "L2", 1, "pa-2", "Alpha Two", 20, "", "a-good"),
    # fractional measure is legal in v2 (integer->number widening)
    ("laba", "L3", 1, "pa-3", "Alpha Three", 10.5, "north", "a-good"),
    # batch 2, same observation, higher edition
    ("laba", "L1", 2, "pa-1", "Alpha One again", 11, "second", "a-good"),
]

LABB_GOOD_ROWS = [
    # remark & verified & batch omitted -> defaults "", false, 0
    # batch declared `number` in v2 but the stored measure (reading) is int
    ("labb", "M1", 1, "pb-1", "Beta One", 7, "", "b-good"),
    ("labb", "M2", 1, "pb-2", "Beta Two", 8, "see note", "b-good"),
]

# A rejected batch ending with one illegal line: NOTHING from it may be stored.
MIXED_BAD_ROWS_ABSENT = [
    ("laba", "K1", 1, "pk-1", "Mixed One", 1, "", "mixed-bad"),
    ("laba", "K2", 1, "pk-2", "Mixed Two", 2, "", "mixed-bad"),
    ("laba", "K3", 1, "pk-3", "Mixed Three", 3, "", "mixed-bad"),
]

MIXED_BAD_EXPECTED_ERRORS = [
    # line 4 violates: unknown field `secret` and wrong type for `value`
    (4, "secret", "unknown_field"),
    (4, "value", "type_mismatch"),
]

# v3-shaped input must be named explicitly; a v2-shaped document is rejected
# by v3 (unknown/missing fields) rather than silently accepted.
V3_SHAPED_LINE_REJECTED_FIELDS = ["flag", "note"]

# Diagnostics must never echo a payload value: this batch uses marker strings
# in display_name / measure-ish text / memo; none may appear anywhere.
LEAK_MARKERS = ["LEAK-NAME", "LEAK-MEMO", "LEAK-FIELD"]

# ---------------------------------------------------------------------------
# legacy protocol expectations (no contract selectors): the ORIGINAL fixed
# fields still work unchanged, and produce change-feed notifications.
# ---------------------------------------------------------------------------
LEGACY_ROWS = [
    ("legacy", "G1", 1, "pl-1", "Legacy One", 100, "m1", "legacy-good"),
]
LEGACY_BAD_ROWS_ABSENT = [("legacy", "G9", 1 - 9_999_999)]  # never stored

# Change-feed expectations (objects only): which (study, observation) are
# announced `refresh` by the accepted batches and `remove` by the withdrawal.
EXPECTED_FEED_REFRESH = [
    ("laba", "L1"), ("laba", "L2"), ("laba", "L3"), ("laba", "B1"),
    ("labb", "M1"), ("labb", "M2"), ("legacy", "G1"),
]
# The v3 branch batch is accepted later in the demo (explicit version 3).
V3_BRANCH_ROW = ("laba", "B1", 1, "pa-9", "Branch", 3, "checked", "v3-explicit")


def expected_batches(contract_batches):
    """(batch_tag, protocol, provider, material_type, version) tuple list."""
    return contract_batches
