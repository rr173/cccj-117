"""Revision-level data lineage: registration and traversal of processing
relations.

What is stored
--------------
A lineage declaration says: "job J, applying rule R, produced these output
revisions from these input revisions".  Nodes are exact (study, observation,
edition) triples - never "the current highest edition".  Three tables (see
db.SCHEMA):

  ln_jobs    job_id -> (rule_id, digest of the normalized complete relation)
  ln_nodes   triple -> kind ('leaf' | 'derived') + declaring job
  ln_edges   output triple -> input triple (the depends-on relation)

A revision with no ln_nodes row has UNKNOWN provenance: "no parents yet" is
not the same as "original collection".  Only an explicit `lineage leaf`
declaration marks an original leaf, and only an explicit `lineage declare`
marks a derived output.  The two are mutually exclusive per triple.

Declarations carry identifiers only: no display_name, measure or memo is
copied, and observation values are never touched.  Nothing here runs research
algorithms, refreshes indexes (no change-feed events) or builds bundles.

Registration rules
------------------
  * every node (output or input) must be an existing, currently readable
    revision: present in `editions` and not under the withdrawal barrier.
    Registering provenance for withdrawn or nonexistent revisions is refused.
  * self-dependency and cycles of any length are rejected against the MERGED
    relation (existing edges plus the whole batch); outputs inside one batch
    may depend on each other.
  * a batch is atomic: any conflict leaves no job row, no node rows and no
    partial edges behind (single BEGIN IMMEDIATE transaction, which also
    serializes two racing declarations so opposite directions cannot each
    "pass the check" and merge into a cycle).
  * an output's provenance is fixed once: re-sending the same job id with the
    same rule id and the same normalized relation (input order and duplicate
    inputs are irrelevant) is an idempotent duplicate; the same id with
    changed content is rejected, and so is any other job claiming an output
    that already has provenance.
  * withdrawal never rewrites or deletes lineage rows, and a new higher
    edition inherits nothing from an older one.

Queries
-------
`upstream` / `downstream` walk the edge relation from one exact triple with a
max-hop limit.  One answer comes from ONE read transaction, so a relation
registered concurrently can never leak into an in-flight answer.  The visited
set, distances and the frontier live in TEMP tables (bounded memory, disk
pages managed by SQLite); paths are never enumerated - each node is visited
once.  Every returned node carries its shortest distance and one witness
path: BFS layers, and among the predecessors in the previous layer the
lexicographically smallest (study, observation, edition) triple is chosen as
the parent.  The start node itself is never listed as a dependency.

Upstream node kinds: 'leaf' (declared original), 'unknown' (no declared
provenance), 'derived' (has inputs).  A derived node at the depth limit is
flagged truncated=true (it still has unexpanded upstream); downstream nodes
at the depth limit with further dependents are flagged likewise.  With
--max-depth 0 only the start status is reported and the end line's
depth_limited flag tells apart "nothing there" from "not expanded".

Readability is reported per node: a revision whose editions row is gone (or
whose person is under the barrier) is marked "withdrawn" - its identity still
explains the graph, but no payload is ever read back.  A triple that never
existed is "missing".
"""
from __future__ import annotations

import json

from .syncpoints import sync_point
from .util import now_iso, sha256_hex

_NODE_KEYS = ("study", "observation", "edition")


# --------------------------------------------------------------------------
# errors
# --------------------------------------------------------------------------
class LineageError(Exception):
    code = "lineage_error"
    exit_code = 2

    def __init__(self, message: str, **fields):
        super().__init__(message)
        self.fields = fields

    def as_dict(self) -> dict:
        d = {"code": self.code, "error": str(self)}
        d.update(self.fields)
        return d


class InvalidDeclaration(LineageError):
    code = "invalid_declaration"


class InvalidQuery(LineageError):
    code = "invalid_query"


class UnknownRevision(LineageError):
    code = "unknown_revision"


class RevisionWithdrawn(LineageError):
    code = "revision_withdrawn"


class OutputClaimed(LineageError):
    code = "output_already_claimed"


class LeafConflict(LineageError):
    code = "leaf_conflicts_with_derived"


class JobConflict(LineageError):
    code = "job_conflict"


class SelfDependency(LineageError):
    code = "self_dependency"


class CycleDetected(LineageError):
    code = "cycle_detected"


# --------------------------------------------------------------------------
# small helpers
# --------------------------------------------------------------------------
def _node_dict(t) -> dict:
    return {"study": t[0], "observation": t[1], "edition": t[2]}


def parse_node(obj, what: str = "node"):
    """Validate a {"study","observation","edition"} object; return the triple."""
    if not isinstance(obj, dict):
        raise InvalidDeclaration("%s must be a JSON object" % what)
    if set(obj.keys()) != set(_NODE_KEYS):
        raise InvalidDeclaration(
            "%s must have exactly the fields study, observation, edition" % what)
    s, o, e = obj["study"], obj["observation"], obj["edition"]
    if not isinstance(s, str) or not s:
        raise InvalidDeclaration("%s.study must be a non-empty string" % what)
    if not isinstance(o, str) or not o:
        raise InvalidDeclaration("%s.observation must be a non-empty string" % what)
    if isinstance(e, bool) or not isinstance(e, int) or e < 1:
        raise InvalidDeclaration("%s.edition must be a positive integer" % what)
    return (s, o, e)


def parse_node_arg(text: str):
    """The --node command line argument (JSON object)."""
    try:
        obj = json.loads(text)
    except Exception:
        raise InvalidQuery("--node must be a JSON object with "
                           "study/observation/edition")
    try:
        return parse_node(obj, "node")
    except InvalidDeclaration as e:
        raise InvalidQuery(str(e))


def _read_jsonl(src):
    """Parse a JSONL byte stream into (line_no, object) pairs."""
    items = []
    for line_no, raw in enumerate(src, 1):
        try:
            obj = json.loads(raw.decode("utf-8"))
        except Exception:
            raise InvalidDeclaration("line %d: invalid JSON" % line_no,
                                     line=line_no)
        items.append((line_no, obj))
    return items


def _revision_state(conn, t):
    """(state, person): state is 'active' | 'withdrawn' | 'missing'.

    'withdrawn' covers both the post-barrier/pre-erasure window (edition row
    still present but the person is barred) and cleared editions (row gone,
    identity tombstone kept)."""
    row = conn.execute(
        "SELECT person FROM editions WHERE study=? AND observation=? AND edition=?",
        t).fetchone()
    if row is not None:
        barred = conn.execute("SELECT 1 FROM barrier WHERE study=? AND person=?",
                              (t[0], row["person"])).fetchone()
        return ("withdrawn" if barred else "active"), row["person"]
    owner = conn.execute(
        "SELECT person FROM observation_owner WHERE study=? AND observation=?",
        (t[0], t[1])).fetchone()
    if owner is not None:
        return "withdrawn", owner["person"]
    return "missing", None


def _require_active(conn, t) -> None:
    state, _ = _revision_state(conn, t)
    if state == "missing":
        raise UnknownRevision("revision does not exist", node=_node_dict(t))
    if state == "withdrawn":
        raise RevisionWithdrawn("revision is withdrawn and not readable",
                                node=_node_dict(t))


def _normalize(outputs):
    """Canonical form of a job relation: outputs sorted by triple, each
    input set deduplicated and sorted.  Input order and duplicate inputs do
    not affect equivalence."""
    norm = []
    for out, ins in outputs:
        norm.append((out, tuple(sorted(set(ins)))))
    norm.sort()
    return norm


def _digest(job_id: str, rule_id: str, norm) -> str:
    payload = {"job": job_id, "rule": rule_id,
               "outputs": [{"output": list(o), "inputs": [list(i) for i in ins]}
                           for o, ins in norm]}
    return sha256_hex(json.dumps(payload, sort_keys=True, separators=(",", ":"),
                                 ensure_ascii=False).encode("utf-8"))


def _rollback(conn) -> None:
    try:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
    except Exception:
        pass


# --------------------------------------------------------------------------
# declare: record a processing job
# --------------------------------------------------------------------------
def _parse_declare(src):
    outputs = []
    for line_no, obj in _read_jsonl(src):
        if not isinstance(obj, dict) or set(obj.keys()) != {"output", "inputs"}:
            raise InvalidDeclaration(
                "line %d: expected an object with exactly 'output' and 'inputs'"
                % line_no, line=line_no)
        out = parse_node(obj["output"], "line %d output" % line_no)
        ins = obj["inputs"]
        if not isinstance(ins, list) or not ins:
            raise InvalidDeclaration(
                "line %d: 'inputs' must be a non-empty list" % line_no,
                line=line_no)
        inputs = [parse_node(x, "line %d input" % line_no) for x in ins]
        outputs.append((out, inputs))
    return outputs


_MERGED_EDGES = """
SELECT out_study AS os, out_observation AS oo, out_edition AS oe,
       in_study AS ns, in_observation AS no, in_edition AS ne
FROM ln_edges
UNION ALL
SELECT out_s, out_o, out_e, in_s, in_o, in_e FROM ln_new
"""


def _cycle_check(conn) -> None:
    """Reject if the merged relation (registered edges + ln_new) has a cycle.

    The registered graph is acyclic by invariant, so any new cycle is
    reachable from the new outputs: compute their upstream closure over the
    merged edges (BFS in TEMP tables), then run Kahn's algorithm on that
    subgraph.  Nodes that can never reach in-degree 0 form a cycle.
    """
    conn.execute("CREATE TEMP TABLE IF NOT EXISTS ln_reach("
                 "s TEXT, o TEXT, e INTEGER, PRIMARY KEY(s,o,e))")
    conn.execute("CREATE TEMP TABLE IF NOT EXISTS ln_front("
                 "s TEXT, o TEXT, e INTEGER, PRIMARY KEY(s,o,e))")
    conn.execute("CREATE TEMP TABLE IF NOT EXISTS ln_next("
                 "s TEXT, o TEXT, e INTEGER, PRIMARY KEY(s,o,e))")
    for t in ("ln_reach", "ln_front", "ln_next"):
        conn.execute("DELETE FROM " + t)
    conn.execute("INSERT OR IGNORE INTO ln_front "
                 "SELECT out_s, out_o, out_e FROM ln_new")
    while True:
        conn.execute("INSERT OR IGNORE INTO ln_reach SELECT s,o,e FROM ln_front")
        conn.execute("DELETE FROM ln_next")
        conn.execute(
            "INSERT OR IGNORE INTO ln_next "
            "SELECT m.ns, m.no, m.ne FROM (" + _MERGED_EDGES + ") m "
            "WHERE (m.os, m.oo, m.oe) IN (SELECT s,o,e FROM ln_front) "
            "AND (m.ns, m.no, m.ne) NOT IN (SELECT s,o,e FROM ln_reach)")
        if conn.execute("SELECT COUNT(*) AS c FROM ln_next").fetchone()["c"] == 0:
            break
        conn.execute("DELETE FROM ln_front")
        conn.execute("INSERT INTO ln_front SELECT s,o,e FROM ln_next")

    # Edges of the merged graph with both ends inside the closure.
    conn.execute("CREATE TEMP TABLE IF NOT EXISTS ln_sub("
                 "os TEXT, oo TEXT, oe INTEGER, ns TEXT, no TEXT, ne INTEGER, "
                 "PRIMARY KEY(os,oo,oe,ns,no,ne))")
    conn.execute("DELETE FROM ln_sub")
    conn.execute(
        "INSERT OR IGNORE INTO ln_sub SELECT m.* FROM (" + _MERGED_EDGES + ") m "
        "WHERE (m.os, m.oo, m.oe) IN (SELECT s,o,e FROM ln_reach) "
        "AND (m.ns, m.no, m.ne) IN (SELECT s,o,e FROM ln_reach)")
    # Kahn: deg = number of the node's inputs still present in ln_sub.
    conn.execute("CREATE TEMP TABLE IF NOT EXISTS ln_deg("
                 "s TEXT, o TEXT, e INTEGER, deg INTEGER, PRIMARY KEY(s,o,e))")
    conn.execute("CREATE TEMP TABLE IF NOT EXISTS ln_zero("
                 "s TEXT, o TEXT, e INTEGER, PRIMARY KEY(s,o,e))")
    conn.execute("DELETE FROM ln_deg")
    conn.execute(
        "INSERT INTO ln_deg "
        "SELECT r.s, r.o, r.e, COALESCE(d.c, 0) FROM ln_reach r "
        "LEFT JOIN (SELECT os, oo, oe, COUNT(*) AS c FROM ln_sub "
        "           GROUP BY os, oo, oe) d "
        "ON d.os=r.s AND d.oo=r.o AND d.oe=r.e")
    remaining = conn.execute("SELECT COUNT(*) AS c FROM ln_deg").fetchone()["c"]
    while remaining:
        conn.execute("DELETE FROM ln_zero")
        conn.execute("INSERT INTO ln_zero SELECT s,o,e FROM ln_deg WHERE deg=0")
        n0 = conn.execute("SELECT COUNT(*) AS c FROM ln_zero").fetchone()["c"]
        if n0 == 0:
            sample = [_node_dict((r["s"], r["o"], r["e"])) for r in
                      conn.execute("SELECT s,o,e FROM ln_deg ORDER BY s,o,e LIMIT 8")]
            raise CycleDetected(
                "declaration would create a dependency cycle "
                "(%d revisions remain on a cycle)" % remaining,
                cycle_nodes=sample)
        conn.execute(
            "UPDATE ln_deg SET deg = deg - ("
            "  SELECT COUNT(*) FROM ln_sub JOIN ln_zero z "
            "    ON ln_sub.ns=z.s AND ln_sub.no=z.o AND ln_sub.ne=z.e "
            "  WHERE ln_sub.os=ln_deg.s AND ln_sub.oo=ln_deg.o "
            "    AND ln_sub.oe=ln_deg.e) "
            "WHERE EXISTS ("
            "  SELECT 1 FROM ln_sub JOIN ln_zero z "
            "    ON ln_sub.ns=z.s AND ln_sub.no=z.o AND ln_sub.ne=z.e "
            "  WHERE ln_sub.os=ln_deg.s AND ln_sub.oo=ln_deg.o "
            "    AND ln_sub.oe=ln_deg.e)")
        conn.execute("DELETE FROM ln_deg WHERE (s,o,e) IN (SELECT s,o,e FROM ln_zero)")
        conn.execute("DELETE FROM ln_sub WHERE (ns,no,ne) IN (SELECT s,o,e FROM ln_zero)")
        remaining -= n0


def declare(store, job_id: str, rule_id: str, src):
    """Register one processing job.  Returns (exit_code, result_dict)."""
    if not isinstance(job_id, str) or not job_id:
        raise InvalidDeclaration("job id must be a non-empty string")
    if not isinstance(rule_id, str) or not rule_id:
        raise InvalidDeclaration("rule id must be a non-empty string")
    outputs = _parse_declare(src)
    if not outputs:
        raise InvalidDeclaration("declaration contains no outputs")
    seen = set()
    for out, ins in outputs:
        if out in seen:
            raise InvalidDeclaration("output listed more than once",
                                     node=_node_dict(out))
        seen.add(out)
        if out in ins:
            raise SelfDependency("output depends on itself", node=_node_dict(out))
    norm = _normalize(outputs)
    digest = _digest(job_id, rule_id, norm)
    edges = [(o + i) for o, ins in norm for i in ins]

    conn = store.connect()
    try:
        # BEGIN IMMEDIATE serializes declarations: a racing opposite
        # declaration cannot pass its own check against a stale snapshot.
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute("SELECT rule_id, relation_digest FROM ln_jobs "
                           "WHERE job_id=?", (job_id,)).fetchone()
        if row is not None:
            if row["rule_id"] == rule_id and row["relation_digest"] == digest:
                conn.execute("COMMIT")
                return 0, {"job_id": job_id, "rule_id": rule_id,
                           "status": "duplicate",
                           "note": "identical relation already recorded"}
            conn.execute("ROLLBACK")
            raise JobConflict("job id already recorded with a different rule "
                              "or relation", job_id=job_id)
        nodes = set()
        for out, ins in norm:
            nodes.add(out)
            nodes.update(ins)
        for t in sorted(nodes):
            _require_active(conn, t)
        for out, _ in norm:
            ex = conn.execute("SELECT kind, job_id FROM ln_nodes "
                              "WHERE study=? AND observation=? AND edition=?",
                              out).fetchone()
            if ex is not None:
                raise OutputClaimed(
                    "output already has provenance", node=_node_dict(out),
                    existing_kind=ex["kind"], existing_job=ex["job_id"])
        conn.execute("CREATE TEMP TABLE IF NOT EXISTS ln_new("
                     "out_s TEXT, out_o TEXT, out_e INTEGER, "
                     "in_s TEXT, in_o TEXT, in_e INTEGER, "
                     "PRIMARY KEY(out_s,out_o,out_e,in_s,in_o,in_e))")
        conn.execute("DELETE FROM ln_new")
        conn.executemany("INSERT OR IGNORE INTO ln_new VALUES (?,?,?,?,?,?)", edges)
        _cycle_check(conn)
        # Interleaving hook for tests: validation is complete, the write
        # transaction is still open and holds the database write lock.
        sync_point("lineage.declare.before_commit")
        conn.execute("INSERT INTO ln_jobs VALUES (?,?,?,?,?,?)",
                     (job_id, rule_id, digest, len(norm), len(edges), now_iso()))
        conn.executemany(
            "INSERT INTO ln_nodes VALUES (?,?,?,'derived',?)",
            [(o[0], o[1], o[2], job_id) for o, _ in norm])
        conn.executemany(
            "INSERT INTO ln_edges VALUES (?,?,?,?,?,?,?)",
            [(e[0], e[1], e[2], e[3], e[4], e[5], job_id) for e in edges])
        conn.execute("COMMIT")
        return 0, {"job_id": job_id, "rule_id": rule_id, "status": "recorded",
                   "outputs": len(norm), "edges": len(edges),
                   "relation_digest": digest}
    except Exception:
        _rollback(conn)
        raise
    finally:
        conn.close()


# --------------------------------------------------------------------------
# leaf: declare original collection leaves
# --------------------------------------------------------------------------
def declare_leaves(store, src):
    """Declare revisions as original collection leaves.  Idempotent per
    node; the whole file is one transaction."""
    nodes = []
    for line_no, obj in _read_jsonl(src):
        nodes.append(parse_node(obj, "line %d" % line_no))
    nodes = sorted(set(nodes))
    if not nodes:
        raise InvalidDeclaration("declaration contains no leaves")
    conn = store.connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        already, fresh = 0, []
        for t in nodes:
            _require_active(conn, t)
            ex = conn.execute("SELECT kind, job_id FROM ln_nodes "
                              "WHERE study=? AND observation=? AND edition=?",
                              t).fetchone()
            if ex is not None:
                if ex["kind"] == "leaf":
                    already += 1
                    continue
                raise LeafConflict(
                    "revision is already a derived output; it cannot also be "
                    "an original leaf", node=_node_dict(t),
                    existing_kind=ex["kind"], existing_job=ex["job_id"])
            fresh.append(t)
        conn.executemany("INSERT INTO ln_nodes VALUES (?,?,?,'leaf',NULL)", fresh)
        conn.execute("COMMIT")
        return 0, {"status": "recorded", "declared": len(fresh),
                   "already_leaf": already}
    except Exception:
        _rollback(conn)
        raise
    finally:
        conn.close()


# --------------------------------------------------------------------------
# jobs listing
# --------------------------------------------------------------------------
def list_jobs(store) -> list:
    conn = store.connect()
    try:
        return [{"job_id": r["job_id"], "rule_id": r["rule_id"],
                 "outputs": r["output_count"], "edges": r["edge_count"],
                 "created_at": r["created_at"]}
                for r in conn.execute(
                    "SELECT * FROM ln_jobs ORDER BY job_id")]
    finally:
        conn.close()


# --------------------------------------------------------------------------
# traversal
# --------------------------------------------------------------------------
def _node_info(conn, t):
    """(provenance, job_id, rule_id, state) for one triple."""
    row = conn.execute("SELECT kind, job_id FROM ln_nodes "
                       "WHERE study=? AND observation=? AND edition=?",
                       t).fetchone()
    provenance = row["kind"] if row is not None else "unknown"
    job_id = row["job_id"] if row is not None else None
    rule_id = None
    if job_id is not None:
        r = conn.execute("SELECT rule_id FROM ln_jobs WHERE job_id=?",
                         (job_id,)).fetchone()
        rule_id = r["rule_id"] if r is not None else None
    state, _ = _revision_state(conn, t)
    return provenance, job_id, rule_id, state


def _has_dependents(conn, t) -> bool:
    return conn.execute(
        "SELECT 1 FROM ln_edges WHERE in_study=? AND in_observation=? "
        "AND in_edition=?", t).fetchone() is not None


def _expand(conn, direction: str, dist: int) -> int:
    """Add one BFS layer to q_vis.  Returns the number of new nodes.

    Among the predecessors in the previous layer the parent is the
    lexicographically smallest (study, observation, edition) triple - that
    fixes the witness path deterministically.
    """
    if direction == "upstream":
        # neighbors = inputs of the frontier; parent = the frontier node
        sql = """
        INSERT INTO q_vis
        SELECT st, ob, ed, :d, ps, po, pe FROM (
          SELECT e.in_study AS st, e.in_observation AS ob, e.in_edition AS ed,
                 e.out_study AS ps, e.out_observation AS po, e.out_edition AS pe,
                 ROW_NUMBER() OVER (
                   PARTITION BY e.in_study, e.in_observation, e.in_edition
                   ORDER BY e.out_study, e.out_observation, e.out_edition) AS rn
          FROM ln_edges e
          WHERE (e.out_study, e.out_observation, e.out_edition)
                IN (SELECT st, ob, ed FROM q_vis WHERE dist = :d0)
            AND (e.in_study, e.in_observation, e.in_edition)
                NOT IN (SELECT st, ob, ed FROM q_vis)
        ) WHERE rn = 1
        """
    else:
        # neighbors = outputs that have a frontier node as input
        sql = """
        INSERT INTO q_vis
        SELECT st, ob, ed, :d, ps, po, pe FROM (
          SELECT e.out_study AS st, e.out_observation AS ob, e.out_edition AS ed,
                 e.in_study AS ps, e.in_observation AS po, e.in_edition AS pe,
                 ROW_NUMBER() OVER (
                   PARTITION BY e.out_study, e.out_observation, e.out_edition
                   ORDER BY e.in_study, e.in_observation, e.in_edition) AS rn
          FROM ln_edges e
          WHERE (e.in_study, e.in_observation, e.in_edition)
                IN (SELECT st, ob, ed FROM q_vis WHERE dist = :d0)
            AND (e.out_study, e.out_observation, e.out_edition)
                NOT IN (SELECT st, ob, ed FROM q_vis)
        ) WHERE rn = 1
        """
    conn.execute(sql, {"d": dist, "d0": dist - 1})
    return conn.execute("SELECT changes() AS c").fetchone()["c"]


def _witness_path(conn, row):
    """Follow parent pointers back to the start; return start..node."""
    chain = [(row["st"], row["ob"], row["ed"])]
    ps, po, pe = row["ps"], row["po"], row["pe"]
    while ps is not None:
        chain.append((ps, po, pe))
        r = conn.execute("SELECT ps, po, pe FROM q_vis "
                         "WHERE st=? AND ob=? AND ed=?", (ps, po, pe)).fetchone()
        ps, po, pe = r["ps"], r["po"], r["pe"]
    chain.reverse()
    return [_node_dict(t) for t in chain]


def traverse(store, node, max_depth, direction: str):
    """Yield JSONL lines for one upstream/downstream query.

    Everything - node identity, provenance kind, edges - is read inside ONE
    read transaction, so the answer is a single consistent snapshot.  The
    visited set / distances / frontier live in TEMP tables; results stream
    out row by row.
    """
    if max_depth is not None and max_depth < 0:
        raise InvalidQuery("max depth must be >= 0")
    conn = store.connect()
    try:
        conn.execute("BEGIN")
        provenance, job_id, rule_id, state = _node_info(conn, node)
        start = {"type": "start", "direction": direction, "node": _node_dict(node),
                 "max_depth": max_depth, "provenance": provenance, "state": state}
        if job_id is not None:
            start["job_id"] = job_id
            start["rule_id"] = rule_id
        has_dependents = _has_dependents(conn, node)
        if direction == "downstream":
            start["has_dependents"] = has_dependents
        conn.execute("CREATE TEMP TABLE IF NOT EXISTS q_vis("
                     "st TEXT, ob TEXT, ed INTEGER, dist INTEGER, "
                     "ps TEXT, po TEXT, pe INTEGER, PRIMARY KEY(st,ob,ed))")
        conn.execute("DELETE FROM q_vis")
        conn.execute("INSERT INTO q_vis VALUES (?,?,?,0,NULL,NULL,NULL)", node)
        # Interleaving hook: the start node has been read inside the read
        # transaction; a concurrent declaration committed now must not leak
        # into this answer.
        sync_point("lineage.query.after_start")
        yield json.dumps(start, ensure_ascii=False)

        depth = 0
        while max_depth is None or depth < max_depth:
            if _expand(conn, direction, depth + 1) == 0:
                break
            depth += 1

        count = 0
        depth_limited = False
        for row in conn.execute(
                "SELECT * FROM q_vis WHERE dist >= 1 ORDER BY dist, st, ob, ed"):
            t = (row["st"], row["ob"], row["ed"])
            prov, rjob, rrule, rstate = _node_info(conn, t)
            if max_depth is not None and row["dist"] == max_depth:
                if direction == "upstream":
                    truncated = prov == "derived"
                else:
                    truncated = _has_dependents(conn, t)
            else:
                truncated = False
            depth_limited = depth_limited or truncated
            rec = {"type": "node", "node": _node_dict(t), "distance": row["dist"],
                   "provenance": prov, "state": rstate, "truncated": truncated,
                   "path": _witness_path(conn, row)}
            if rjob is not None:
                rec["job_id"] = rjob
                rec["rule_id"] = rrule
            count += 1
            yield json.dumps(rec, ensure_ascii=False)
        if max_depth == 0:
            # Nothing was expanded; say so instead of implying "no relations".
            if direction == "upstream":
                depth_limited = provenance == "derived"
            else:
                depth_limited = has_dependents
        conn.execute("COMMIT")
        yield json.dumps({"type": "end", "direction": direction, "nodes": count,
                          "complete": True, "depth_limited": depth_limited},
                         ensure_ascii=False)
    finally:
        conn.close()
