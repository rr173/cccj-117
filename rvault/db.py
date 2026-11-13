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
