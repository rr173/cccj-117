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
  - offline RDF dataset canonicalization (W3C RDFC-1.0, SHA-256, canonical
    N-Quads) and dataset-identity checking over caller-supplied N-Quads files:
    strict RDF 1.1 parsing, set-semantics deduplication, a deterministic work
    budget, and byte-level comparison; no store access, no lineage, no query
"""

__version__ = "1.0.0"
