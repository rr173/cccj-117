"""SQLite schema and connection handling.

Logical data model
------------------
batches / batch_items   : delivery credentials and per-line manifests (identifiers only)
observation_owner       : study/observation -> person ownership; kept as a tombstone
                          after withdrawal so old data cannot be resurrected under a
                          different person (identity only, no payload)
editions                : full edition history; payload lives here and ONLY here
                          (plus bundle files). Withdrawal deletes these rows.
withdrawals / withdrawal_persons / withdrawal_bundles :
                          permanent withdrawal registry with staged progress
barrier                 : (study, person) pairs under withdrawal; registered first and
                          consulted by every read/intake path immediately
bundles / bundle_participants :
                          registry of issued bundles; ids are never reused
counters                : monotonic id sequences

Change-feed tables (see changefeed.py):
cf_meta                 : durable feed bookkeeping (head_seq, retention_floor)
cf_frames / cf_events   : committed notification frames and their events
                          (identifiers only - never a research payload)
cf_intents             : per-transaction object intents, deduplicated by
                          SQLite and discarded with the transaction
cf_removed_objects      : every object ever announced as removed; a refresh
                          is never emitted for one of these again
cf_consumers            : named consumers with their own durable acked_seq
cf_deliveries /         : the single pending (or acknowledged) delivery
cf_delivery_frames        range per consumer, with its ack credential

Lineage tables (see lineage.py):
ln_jobs                 : declared processing jobs (job_id -> rule_id + digest
                          of the normalized complete relation; identifiers only)
ln_nodes                : (study, observation, edition) -> provenance kind
                          ('leaf' | 'derived') and the declaring job
ln_edges                : output triple -> input triple (depends-on relation);
                          never rewritten by withdrawal, never inherited by
                          newer editions

Input-contract tables (see contracts.py):
ct_contracts            : one immutable published version per
                          (provider, material_type, version) with its
                          predecessor, explicit compatibility declaration and a
                          content digest; never updated after publication
ct_fields               : field definitions of one contract version (name,
                          type, required, nullable, optional default and an
                          optional canonical target); schema metadata only - no
                          observed research payload is ever stored here
ct_batches              : batch_tag -> the protocol (legacy | contract) and
                          contract version it was admitted under, so the same
                          credential cannot silently switch protocols or
                          contract versions

Mail-channel tables (see mail.py):
ml_messages             : one durable processing record per received message,
                          keyed by the sha256 of the raw message bytes, with
                          the manifest part position, per-state progress
                          (importing | done | rejected) and preflight error
                          categories (identifiers only - never payload)
ml_batches              : (message_digest, manifest seq) -> real batch_tag,
                          content_id, data-part position, declared digest and
                          the per-batch intake outcome (pending | accepted |
                          duplicate | rejected) with an identifier-only detail
"""
from __future__ import annotations

import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS counters(
  name TEXT PRIMARY KEY,
  value INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS batches(
  batch_tag TEXT PRIMARY KEY,
  received_at TEXT NOT NULL,
  digest TEXT NOT NULL,
  accepted_count INTEGER NOT NULL,
  suppressed_count INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS batch_items(
  batch_tag TEXT NOT NULL,
  line_no INTEGER NOT NULL,
  study TEXT NOT NULL,
  observation TEXT NOT NULL,
  person TEXT NOT NULL,
  edition INTEGER NOT NULL,
  disposition TEXT NOT NULL,        -- accepted | accepted_existing | withdrawn_suppressed
  PRIMARY KEY(batch_tag, line_no)
);
CREATE TABLE IF NOT EXISTS observation_owner(
  study TEXT NOT NULL,
  observation TEXT NOT NULL,
  person TEXT NOT NULL,
  PRIMARY KEY(study, observation)
);
CREATE TABLE IF NOT EXISTS editions(
  study TEXT NOT NULL,
  observation TEXT NOT NULL,
  edition INTEGER NOT NULL,
  person TEXT NOT NULL,
  display_name TEXT NOT NULL,
  measure INTEGER NOT NULL,
  memo TEXT NOT NULL,
  batch_tag TEXT NOT NULL,
  PRIMARY KEY(study, observation, edition)
);
CREATE TABLE IF NOT EXISTS withdrawals(
  withdrawal_id TEXT PRIMARY KEY,
  study TEXT NOT NULL,
  person_hash TEXT NOT NULL,
  state TEXT NOT NULL,              -- registered | editions_cleared | completed
  editions_removed INTEGER NOT NULL DEFAULT 0,
  anomalies INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS withdrawal_persons(
  withdrawal_id TEXT NOT NULL,
  person TEXT NOT NULL,
  PRIMARY KEY(withdrawal_id, person)
);
CREATE TABLE IF NOT EXISTS barrier(
  study TEXT NOT NULL,
  person TEXT NOT NULL,
  withdrawal_id TEXT NOT NULL,
  PRIMARY KEY(study, person)
);
CREATE TABLE IF NOT EXISTS bundles(
  bundle_id TEXT PRIMARY KEY,
  study TEXT NOT NULL,
  created_at TEXT NOT NULL,
  path TEXT NOT NULL,               -- relative to store root, always bundles/<id>.json
  digest TEXT NOT NULL,
  state TEXT NOT NULL               -- registering | active | aborted | deleted
);
CREATE TABLE IF NOT EXISTS bundle_participants(
  bundle_id TEXT NOT NULL,
  person TEXT NOT NULL,
  PRIMARY KEY(bundle_id, person)
);
CREATE TABLE IF NOT EXISTS withdrawal_bundles(
  withdrawal_id TEXT NOT NULL,
  bundle_id TEXT NOT NULL,
  state TEXT NOT NULL,              -- pending | cleared | missing | refused
  note TEXT,
  PRIMARY KEY(withdrawal_id, bundle_id)
);
CREATE TABLE IF NOT EXISTS cf_meta(
  name TEXT PRIMARY KEY,
  value INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS cf_frames(
  frame_id INTEGER PRIMARY KEY,
  frame_ts TEXT NOT NULL,
  start_seq INTEGER NOT NULL,
  end_seq INTEGER NOT NULL,
  event_count INTEGER NOT NULL,
  actor TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS cf_events(
  seq INTEGER PRIMARY KEY,
  frame_id INTEGER NOT NULL,
  ordinal INTEGER NOT NULL,
  op TEXT NOT NULL,                 -- refresh | remove
  study TEXT NOT NULL,
  observation TEXT NOT NULL,
  UNIQUE(frame_id, ordinal)
);
CREATE INDEX IF NOT EXISTS cf_events_frame ON cf_events(frame_id, ordinal);
CREATE TABLE IF NOT EXISTS cf_intents(
  session TEXT NOT NULL,            -- one open business transaction
  study TEXT NOT NULL,
  observation TEXT NOT NULL,
  op TEXT NOT NULL,                 -- refresh | remove (remove dominates)
  PRIMARY KEY(session, study, observation)
);
CREATE TABLE IF NOT EXISTS cf_removed_objects(
  study TEXT NOT NULL,
  observation TEXT NOT NULL,
  PRIMARY KEY(study, observation)
);
CREATE TABLE IF NOT EXISTS cf_consumers(
  name TEXT PRIMARY KEY,
  created_at TEXT NOT NULL,
  generation INTEGER NOT NULL,
  start_seq INTEGER NOT NULL,
  acked_seq INTEGER NOT NULL,
  disabled_at TEXT
);
CREATE TABLE IF NOT EXISTS cf_deliveries(
  delivery_id TEXT PRIMARY KEY,
  consumer TEXT NOT NULL,
  generation INTEGER NOT NULL,
  token TEXT NOT NULL,
  from_seq INTEGER NOT NULL,
  through_seq INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  state TEXT NOT NULL,              -- pending | acked | revoked
  acked_at TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS cf_one_pending_delivery
  ON cf_deliveries(consumer) WHERE state='pending';
CREATE TABLE IF NOT EXISTS cf_delivery_frames(
  delivery_id TEXT NOT NULL,
  frame_id INTEGER NOT NULL,
  PRIMARY KEY(delivery_id, frame_id)
);
CREATE TABLE IF NOT EXISTS ln_jobs(
  job_id TEXT PRIMARY KEY,
  rule_id TEXT NOT NULL,
  relation_digest TEXT NOT NULL,  -- digest of the normalized complete relation
  output_count INTEGER NOT NULL,
  edge_count INTEGER NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS ln_nodes(
  study TEXT NOT NULL,
  observation TEXT NOT NULL,
  edition INTEGER NOT NULL,
  kind TEXT NOT NULL,             -- leaf | derived
  job_id TEXT,                    -- declaring job for derived nodes
  PRIMARY KEY(study, observation, edition)
);
CREATE TABLE IF NOT EXISTS ln_edges(
  out_study TEXT NOT NULL,
  out_observation TEXT NOT NULL,
  out_edition INTEGER NOT NULL,
  in_study TEXT NOT NULL,
  in_observation TEXT NOT NULL,
  in_edition INTEGER NOT NULL,
  job_id TEXT NOT NULL,
  PRIMARY KEY(out_study, out_observation, out_edition,
              in_study, in_observation, in_edition)
);
CREATE INDEX IF NOT EXISTS ln_edges_in
  ON ln_edges(in_study, in_observation, in_edition);
CREATE TABLE IF NOT EXISTS ct_contracts(
  provider TEXT NOT NULL,
  material_type TEXT NOT NULL,
  version INTEGER NOT NULL,
  predecessor INTEGER,              -- referenced previous version (NULL for v1)
  compatibility TEXT NOT NULL,      -- initial | compatible | branch
  content_digest TEXT NOT NULL,
  change_summary TEXT NOT NULL,     -- JSON: entry -> {kind, detail} (reviewable)
  published_at TEXT NOT NULL,
  PRIMARY KEY(provider, material_type, version)
);
CREATE INDEX IF NOT EXISTS ct_contracts_pred
  ON ct_contracts(provider, material_type, predecessor);
CREATE TABLE IF NOT EXISTS ct_fields(
  provider TEXT NOT NULL,
  material_type TEXT NOT NULL,
  version INTEGER NOT NULL,
  position INTEGER NOT NULL,
  name TEXT NOT NULL,
  type TEXT NOT NULL,               -- string | integer | number | boolean
  required INTEGER NOT NULL,        -- 0 | 1
  nullable INTEGER NOT NULL,        -- 0 | 1
  default_json TEXT,                -- NULL when the field has no default
  target TEXT,                      -- canonical intake field it maps to, if any
  PRIMARY KEY(provider, material_type, version, name)
);
CREATE TABLE IF NOT EXISTS ct_batches(
  batch_tag TEXT PRIMARY KEY,
  provider TEXT,
  material_type TEXT,
  contract_version INTEGER,
  protocol TEXT NOT NULL            -- legacy | contract | contract_branch
);
CREATE TABLE IF NOT EXISTS ml_messages(
  message_digest TEXT PRIMARY KEY,  -- sha256 of the raw message bytes
  state TEXT NOT NULL,              -- importing | done | rejected
  received_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  manifest_part TEXT,               -- dotted position of the manifest part
  part_count INTEGER NOT NULL,
  batch_count INTEGER NOT NULL,
  accepted_count INTEGER NOT NULL DEFAULT 0,
  duplicate_count INTEGER NOT NULL DEFAULT 0,
  rejected_count INTEGER NOT NULL DEFAULT 0,
  errors_json TEXT                  -- preflight errors (identifiers only)
);
CREATE TABLE IF NOT EXISTS ml_batches(
  message_digest TEXT NOT NULL,
  seq INTEGER NOT NULL,             -- 1-based manifest order
  batch_tag TEXT NOT NULL,
  content_id TEXT NOT NULL,
  part TEXT NOT NULL,               -- dotted position of the data part
  sha256 TEXT NOT NULL,             -- manifest-declared digest of decoded bytes
  status TEXT NOT NULL,             -- pending | accepted | duplicate | rejected
  detail_json TEXT,                 -- intake summary (identifiers only)
  updated_at TEXT NOT NULL,
  PRIMARY KEY(message_digest, seq)
);
"""


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    # Best-effort scrubbing of freed pages. The formal erasure promise is
    # logical only; see README "Erasure scope and residual boundaries".
    conn.execute("PRAGMA secure_delete=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
