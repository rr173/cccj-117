"""rvault - local research data intake and withdrawal processing.

Implements:
  - atomic JSONL batch intake keyed by batch_tag (streaming, bounded memory)
  - full edition history with highest-edition-wins projection
  - analyst dataset bundles written only to managed storage
  - staged, crash-safe withdrawal with a permanent barrier registry
  - a persistent outbound change feed (refresh/remove notifications, durable
    re-playable deliveries, per-consumer positions, explicit retention trimming)
  - nested study-table preparation: a declarative plan projected by a streaming
    ijson event parser into an independent relational output database, with a
    crash-safe per-root transaction boundary and resumable progress
  - an independent byte-pair (BPE) vocabulary learning/encoding subsystem with
    its own SQLite training database: deterministic disk-backed merge state,
    local (hit-only) per-round updates, crash-redo rounds, flock-based single
    advancer, and digest-bound streaming encode/decode that is lossless but
    makes no compression-rate guarantee
"""

__version__ = "1.0.0"
