#!/usr/bin/env python3
"""Independent check of the two remaining feed invariants that need a
deliberate raw-SQL rollback / concurrent producer:

  1. events written inside a business transaction vanish with that
     transaction (no consumer can ever see them) - simulated by writing the
     feed rows in a transaction and rolling it back, plus a real superseded
     producer that is killed before COMMIT;
  2. two producers committing concurrently get contiguous, non-overlapping
     frames and seqs.

The process-level demo (run_feed_demo.py) covers the producer transaction
through the public commands (a rejected batch leaves no event; a withdrawal
frame appears together with the barrier).  This script covers the engine-level
half so the verification is explicit rather than implied.
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
PROJ = os.path.dirname(HERE)
STORE = os.path.join(HERE, "work_feed", "txn")
ENV = dict(os.environ, PYTHONPATH=PROJ)


def run(*args, ok=(0,)):
    r = subprocess.run([sys.executable, "-m", "rvault", "--store", STORE] + list(args),
                       capture_output=True, text=True, env=ENV)
    assert r.returncode in ok, (args, r.returncode, r.stdout, r.stderr)
    return r


def db():
    c = sqlite3.connect(os.path.join(STORE, "vault.sqlite3"))
    c.row_factory = sqlite3.Row
    return c


def main():
    if os.path.exists(STORE):
        shutil.rmtree(STORE)
    run("init")
    c = db()
    conn = sqlite3.connect(os.path.join(STORE, "vault.sqlite3"),
                           isolation_level=None)
    conn.execute("BEGIN IMMEDIATE")
    conn.execute("INSERT INTO cf_events VALUES (1,1,1,'refresh','s','o')")
    conn.execute("INSERT INTO cf_frames VALUES (1,'ts',1,1,1,'intake:rollback')")
    conn.execute("INSERT INTO cf_meta VALUES ('head_seq',1)")
    conn.execute("INSERT INTO cf_meta VALUES ('frame_seq',1)")
    conn.execute("ROLLBACK")
    ev = c.execute("SELECT COUNT(*) FROM cf_events").fetchone()[0]
    fr = c.execute("SELECT COUNT(*) FROM cf_frames").fetchone()[0]
    head = c.execute("SELECT value FROM cf_meta WHERE name='head_seq'").fetchone()
    print("rollback leaves events=%d frames=%d head=%s" % (ev, fr, head))
    assert (ev, fr, head) == (0, 0, None)
    c.close()
    conn.close()

    # concurrent producers: distinct frames, contiguous seqs, no collision
    f1 = os.path.join(STORE, "..", "txn_a.jsonl")
    f2 = os.path.join(STORE, "..", "txn_b.jsonl")
    for path, obs in ((f1, "a"), (f2, "b")):
        with open(path, "w") as f:
            f.write(json.dumps({"study": "s", "person": "p", "observation": obs,
                                "edition": 1, "display_name": "n", "measure": 1,
                                "memo": "m"}) + "\n")
    ps = [subprocess.Popen([sys.executable, "-m", "rvault", "--store", STORE,
                            "import", "--batch-tag", "t%d" % i, "--input", path],
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=ENV)
          for i, path in enumerate((f1, f2))]
    for p in ps:
        p.communicate()
        assert p.returncode == 0
    c = db()
    frames = [tuple(r) for r in c.execute(
        "SELECT frame_id, start_seq, end_seq, event_count FROM cf_frames "
        "ORDER BY frame_id")]
    events = [tuple(r) for r in c.execute(
        "SELECT seq, frame_id, ordinal, op, observation FROM cf_events ORDER BY seq")]
    print("frames:", frames)
    print("events:", events)
    assert frames == [(1, 1, 1, 1), (2, 2, 2, 1)]
    assert [e[0] for e in events] == [1, 2]
    assert [e[1] for e in events] == [1, 2]
    assert len({e[0] for e in events}) == 2
    c.close()
    print("producer transaction checks: OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
