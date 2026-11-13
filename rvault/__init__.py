"""rvault - local research data intake and withdrawal processing.

Implements:
  - atomic JSONL batch intake keyed by batch_tag (streaming, bounded memory)
  - full edition history with highest-edition-wins projection
  - analyst dataset bundles written only to managed storage
  - staged, crash-safe withdrawal with a permanent barrier registry
"""

__version__ = "1.0.0"
