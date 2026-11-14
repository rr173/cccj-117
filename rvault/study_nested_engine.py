"""Streaming event-driven projection engine for nested study tables.

The engine consumes ijson parser events - never whole documents and never
one assembled root object - and projects them into the plan's output
database.  State held in memory, and its bound:

  * structure stack: one entry per open container in the document.  An
    object entry holds the key set used for duplicate-key detection plus a
    link to the plan map node and the row being filled; an array entry
    holds its element index and the child table it crosses.  Depth is
    bounded by MAX_NESTING (plus the plan depth), so a pathologically
    nested document is rejected instead of growing without bound.
  * per root record: at most one open row frame per level of the current
    JSON path.  A row frame holds only that table's declared columns, so a
    parent whose scalar fields appear *after* a huge child array costs
    O(plan width), never O(array length): the parent row is simply
    inserted when its object ends, using a row identity computed before
    any child was seen, so children can carry `parent_row_id` from the
    moment they are written.  (This is the "pending parent fields" state;
    no input-proportional buffering is involved, and no in-memory object
    tree is assembled.)
  * duplicate-key detection: one set of key strings per OPEN object.  An
    object with more than MAX_KEYS_PER_OBJECT distinct keys is rejected;
    that is the explicit bound for this state (it scales with one object,
    not with the file).
  * diagnostics: only the first violation per root, a category and an
    identifier path - never a payload value.
  * transaction staging: rows of one root go into SQLite inside one
    transaction.  The page cache is bounded by SQLite's cache_size and the
    rollback journal lives on disk, so staging a large root is bounded in
    memory and unbounded only on disk.

Each root's rows and its durable per-root record commit together.  A
rejected root rolls back every row it produced; the rejection record is
written afterwards and parsing continues with the next root.  A syntax
error rolls back the current root, records the abort position and stops -
no half tree survives.
"""
from __future__ import annotations

import decimal
import hashlib
import json
import math
import os

import ijson

from . import study_nested as sn
from .syncpoints import sync_point
from .util import now_iso, sha256_hex

MAX_NESTING = 512
MAX_KEYS_PER_OBJECT = 100000
MAX_NUMBER_EXPONENT = 100000

UNMAPPED = "<unmapped>"

PRESENCE_MISSING = 0
PRESENCE_VALUE = 1
PRESENCE_NULL = 2


# ---------------------------------------------------------------------------
# errors
# ---------------------------------------------------------------------------

class RootRejected(Exception):
    """One root object violated the plan; its rows are rolled back."""

    def __init__(self, category, path, **fields):
        super().__init__(category)
        self.category = category
        self.path = list(path)
        self.fields = fields

    def as_dict(self):
        return {"category": self.category, "path": path_text(self.path),
                "fields": self.fields}


class SourceError(Exception):
    """The whole source cannot be processed (syntax damage, wrong shape)."""

    def __init__(self, category, message, position, basis, root_index=None):
        super().__init__(message)
        self.category = category
        self.message = message
        self.position = position
        self.basis = basis
        self.root_index = root_index

    def as_dict(self):
        return {"code": self.category, "error": self.message,
                "position": self.position, "position_basis": self.basis,
                "root_index": self.root_index}


class UnknownSource(sn.PlanError):
    exit_code = 4


class SourceChanged(sn.PlanError):
    exit_code = 4


class TypeMismatch(Exception):
    def __init__(self, declared, actual):
        super().__init__(declared)
        self.declared = declared
        self.actual = actual


class NumberOutOfRange(Exception):
    def __init__(self, declared):
        super().__init__(declared)
        self.declared = declared


# ---------------------------------------------------------------------------
# identity and values
# ---------------------------------------------------------------------------

def row_id(digest, root_index, array_path) -> str:
    """Repeatable identity: source digest + top-level index + array path.

    `array_path` is the full array coordinate from the top-level item to this
    row: for every array crossed, the child table's name and the element
    index, e.g. [["visits",0],["readings",1]].  It contains no business
    value: an identical participant in another file (another digest) does
    not collide, two identical items in one file stay two rows, and two
    different child tables at element 0 cannot collide either.  The root
    row's array path is the empty list.
    """
    raw = json.dumps([digest, root_index, array_path], separators=(",", ":"))
    return sha256_hex(raw.encode("utf-8"))


def index_path_text(index_path) -> str:
    return json.dumps(list(index_path), separators=(",", ":"))


def path_text(segments) -> str:
    """JSON-pointer rendering; '~' and '/' inside keys are escaped."""
    return "/" + "/".join(str(s).replace("~", "~0").replace("/", "~1")
                          for s in segments)


def normalize(kind, value, declared):
    """Return (stored value, presence marker) for one parser value.

    Presence: 0 missing, 1 value present (including 0, false and ""), 2
    explicit JSON null.  Numbers stay exact: plain-decimal text is produced
    from the parser value (int or Decimal) and never passes through a binary
    float.
    """
    if kind == "null":
        return None, PRESENCE_NULL
    if declared == "string":
        if kind != "string":
            raise TypeMismatch(declared, kind)
        return value, PRESENCE_VALUE
    if declared == "boolean":
        if kind != "boolean":
            raise TypeMismatch(declared, kind)
        return (1 if value else 0), PRESENCE_VALUE
    if kind != "number":
        raise TypeMismatch(declared, kind)
    if isinstance(value, bool):
        raise TypeMismatch(declared, kind)
    if isinstance(value, int):
        d = decimal.Decimal(value)
    elif isinstance(value, float):       # not produced by basic_parse
        if not math.isfinite(value):
            raise TypeMismatch(declared, kind)
        d = decimal.Decimal(repr(value))
    else:
        d = value
    if not isinstance(d, decimal.Decimal) or not d.is_finite():
        raise TypeMismatch(declared, kind)
    if abs(d.adjusted()) > MAX_NUMBER_EXPONENT:
        raise NumberOutOfRange(declared)
    if declared == "integer":
        if d != d.to_integral_value():
            raise TypeMismatch(declared, kind)
        return format(d.to_integral_value(), "f"), PRESENCE_VALUE
    return format(d, "f"), PRESENCE_VALUE


# ---------------------------------------------------------------------------
# runtime frames
# ---------------------------------------------------------------------------

class RowFrame:
    __slots__ = ("table", "row_id", "parent_row_id", "root_row_id",
                 "index_path", "array_path", "root_index", "ordinal",
                 "values", "present", "written")

    def __init__(self, table, rowid, parent_row_id, root_row_id, index_path,
                 array_path, root_index, ordinal):
        self.table = table
        self.row_id = rowid
        self.parent_row_id = parent_row_id
        self.root_row_id = root_row_id
        self.index_path = index_path
        self.array_path = array_path
        self.root_index = root_index
        self.ordinal = ordinal
        self.values = [None] * len(table.columns)
        self.present = [PRESENCE_MISSING] * len(table.columns)
        self.written = False


class ObjFrame:
    __slots__ = ("node", "row", "table_row", "is_table", "keys", "path", "key")

    def __init__(self, node, row, table_row, is_table, path):
        self.node = node
        self.row = row
        self.table_row = table_row
        self.is_table = is_table
        self.keys = set()
        self.path = path
        self.key = None


class ArrFrame:
    __slots__ = ("child", "parent_row", "path", "index", "is_child")

    def __init__(self, child, parent_row, path, is_child):
        self.child = child
        self.parent_row = parent_row
        self.path = path
        self.index = -1
        self.is_child = is_child


# ---------------------------------------------------------------------------
# reader and parser error positions
# ---------------------------------------------------------------------------

class CountingReader:
    """Tracks consumed bytes and the chunk currently handed to the parser."""

    def __init__(self, f, chunk_size=65536):
        self.f = f
        self.chunk_size = chunk_size
        self.bytes_consumed = 0
        self.chunk_start = 0
        self.chunk = b""

    def read(self, n=-1):
        if n is None or n < 0:
            n = self.chunk_size
        data = self.f.read(n)
        if data:
            self.chunk = data
            self.chunk_start = self.bytes_consumed
        self.bytes_consumed += len(data)
        return data


def _render_window(chunk, offset):
    """Reproduce yajl's verbose context line for one candidate offset.

    yajl prints (40-offset) spaces then the document from 0 when offset < 30,
    otherwise 10 spaces then the 60 bytes [offset-30, offset+30).  Newlines
    and carriage returns inside the window are shown as spaces.
    """
    n = len(chunk)
    spaces = 40 - offset if offset < 30 else 10
    start = 0 if offset < 30 else offset - 30
    end = min(offset + 30, n)
    body = bytes(32 if c in (10, 13) else c for c in chunk[start:end])
    return " " * spaces + body.decode("utf-8", "replace")


def parser_error_position(reader, exc):
    """Return (absolute byte position, basis) for a parser exception.

    A damaged document must be reported with the position where reading can
    no longer continue.  The parser's message carries a deterministic
    context window; we render every candidate offset against the chunk that
    produced the error and accept a position only if exactly one offset
    reproduces the window.  Otherwise the count of bytes already handed to
    the parser is reported as an explicit upper bound (never a guess).
    """
    arg = exc.args[0] if exc.args else ""
    msg = arg.decode("utf-8", "replace") if isinstance(arg, bytes) else str(arg)
    if "premature EOF" in msg:
        return reader.bytes_consumed, "end_of_file"
    lines = msg.split("\n")
    if len(lines) >= 3 and "(right here)" in lines[2]:
        chunk = reader.chunk
        matches = [o for o in range(0, len(chunk) + 1)
                   if _render_window(chunk, o) == lines[1]]
        if len(matches) == 1:
            return reader.chunk_start + matches[0], "exact_offset"
        if matches:
            return reader.chunk_start + matches[0], "first_matching_offset"
    return reader.bytes_consumed, "upper_bound"


# ---------------------------------------------------------------------------
# projector
# ---------------------------------------------------------------------------

class Projector:
    def __init__(self, plan, conn, digest, skip_roots, chunk_size=65536):
        self.plan = plan
        self.conn = conn
        self.digest = digest
        self.skip_roots = skip_roots or set()
        self.chunk_size = chunk_size
        self.root_table = plan.by_name[plan.root_name]
        self.root_node = self.root_table.mapping
        self.max_depth = MAX_NESTING + 2 * len(plan.tables)
        self._col_index = {
            t.name: {c.name: i for i, c in enumerate(t.columns)}
            for t in plan.tables}
        self._insert_sql = {}
        self.stack = []
        self.mode = "idle"           # idle | project | skip
        self.root_index = -1
        self.item_depth = 0
        self.root_row = None
        self.root_counts = {}
        self.root_ordinals = {}
        self.roots_seen = 0
        self.roots_committed = 0
        self.roots_skipped = 0
        self.roots_rejected = 0
        self.tx_open = False
        self.last_rejection = None
        self.skip_reject = None

    # -- SQL -------------------------------------------------------------
    def _sql_for(self, table):
        sql = self._insert_sql.get(table.name)
        if sql is None:
            cols = [n for n, _, _, _ in sn.source_columns()]
            cols += [c.name for c in table.columns]
            cols += [c.name + "__present" for c in table.columns]
            sql = "INSERT INTO %s (%s) VALUES (%s)" % (
                sn.ident(table.name), ", ".join(sn.qcol(c) for c in cols),
                ", ".join("?" for _ in cols))
            self._insert_sql[table.name] = sql
        return sql

    def _insert(self, row):
        if row.written:
            return
        if not self.tx_open:
            raise RuntimeError("row insert outside a root transaction")
        values = [row.row_id, row.parent_row_id, row.root_row_id,
                  self.digest, row.root_index,
                  index_path_text(row.index_path),
                  json.dumps(row.array_path, separators=(",", ":")),
                  row.ordinal]
        values.extend(row.values)
        values.extend(row.present)
        self.conn.execute(self._sql_for(row.table), values)
        row.written = True
        self.root_counts[row.table.name] = (
            self.root_counts.get(row.table.name, 0) + 1)

    # -- transactions ------------------------------------------------------
    def _begin_root_tx(self):
        self.conn.execute("BEGIN IMMEDIATE")
        self.tx_open = True
        self.root_counts = {}
        self.root_ordinals = {t.name: 0 for t in self.plan.tables}

    def _next_ordinal(self, table_name):
        n = self.root_ordinals[table_name]
        self.root_ordinals[table_name] = n + 1
        return n

    def _commit_root(self):
        counts = json.dumps(self.root_counts, sort_keys=True,
                            separators=(",", ":"))
        self.conn.execute(
            "INSERT OR REPLACE INTO ns_roots(source_digest, root_index, "
            "status, category, path, detail_json, root_row_id, counts_json, "
            "updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
            (self.digest, self.root_index, "committed", None, None, None,
             self.root_row.row_id if self.root_row else None, counts,
             now_iso()))
        sync_point("nested.root_before_commit")
        sync_point("nested.root_before_commit.%d" % self.root_index)
        self.conn.execute("COMMIT")
        self.tx_open = False
        self.roots_committed += 1
        sync_point("nested.root_committed")
        sync_point("nested.root_committed.%d" % self.root_index)
        sync_point("nested.root_committed.%d" % self.root_index)

    def _rollback_root(self):
        if self.tx_open:
            try:
                self.conn.execute("ROLLBACK")
            finally:
                self.tx_open = False

    def _record_rejection(self, rejection):
        detail = json.dumps(rejection.as_dict(), sort_keys=True,
                            separators=(",", ":"))
        try:
            self.conn.execute("BEGIN IMMEDIATE")
            self.conn.execute(
                "INSERT OR REPLACE INTO ns_roots(source_digest, root_index, "
                "status, category, path, detail_json, root_row_id, "
                "counts_json, updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
                (self.digest, self.root_index, "rejected",
                 rejection.category, path_text(rejection.path), detail,
                 None, None, now_iso()))
            self.conn.execute("COMMIT")
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise
        self.roots_rejected += 1
        self.last_rejection = rejection

    # -- driver -----------------------------------------------------------
    def begin_root(self, index):
        """Start projecting one root object (its start_map was consumed)."""
        self.root_index = index
        self.roots_seen = index + 1
        self._begin_root_tx()
        self.stack = []
        self.root_row = RowFrame(
            self.root_table, row_id(self.digest, index, []),
            None, None, [], [], index,
            self._next_ordinal(self.root_table.name))
        self.root_row.root_row_id = self.root_row.row_id
        self._push_object(self.root_node, self.root_row, self.root_row, True,
                          ())

    def handle(self, event, value):
        self._project_event(event, value)

    def reject_root(self, rejection):
        """Roll back this root's rows and persist the rejection record."""
        self._rollback_root()
        self.stack = []
        self.root_row = None
        self._record_rejection(rejection)

    def reject_rootless(self, index, rejection):
        self.root_index = index
        self.roots_seen = index + 1
        self._record_rejection(rejection)

    def skip_committed(self, index):
        self.root_index = index
        self.roots_seen = index + 1
        self.roots_skipped += 1

    def commit_root(self):
        self.stack = []
        self._commit_root()
        self.root_row = None
        sync_point("nested.root_committed")
        sync_point("nested.root_committed.%d" % self.root_index)

    # -- structure ---------------------------------------------------------
    def _check_depth(self, path):
        if len(self.stack) + 1 > self.max_depth:
            raise RootRejected("nesting_too_deep", path,
                               limit=self.max_depth)

    def _push_object(self, node, row, table_row, is_table, path):
        self._check_depth(path)
        self.stack.append(ObjFrame(node, row, table_row, is_table, path))

    def _push_array(self, child, parent_row, path, is_child):
        self._check_depth(path)
        self.stack.append(ArrFrame(child, parent_row, path, is_child))

    def _project_event(self, event, value):
        top = self.stack[-1] if self.stack else None
        if isinstance(top, ObjFrame):
            self._object_event(top, event, value)
        elif isinstance(top, ArrFrame):
            self._array_event(top, event, value)
        else:
            raise RuntimeError("event outside a container")

    def _object_event(self, frame, event, value):
        if event == "map_key":
            if not isinstance(value, str):
                raise RuntimeError("non-string map key")
            if value in frame.keys:
                # Diagnostics must never echo payload values.  A key name at
                # a position the plan knows is structural metadata; a key in
                # an unmapped object is not trusted as such, so only the
                # containing object's path is reported there.
                declared = frame.node is not None and (
                    value in frame.node.leaves or value in frame.node.maps
                    or value in frame.node.branch)
                if declared:
                    raise RootRejected("duplicate_key",
                                       frame.path + (value,), key=value)
                raise RootRejected("duplicate_key", list(frame.path),
                                   in_unmapped_object=True)
            if len(frame.keys) >= MAX_KEYS_PER_OBJECT:
                raise RootRejected("object_keys_exceeded",
                                   list(frame.path),
                                   limit=MAX_KEYS_PER_OBJECT)
            frame.keys.add(value)
            frame.key = value
            return
        if event == "end_map":
            self.stack.pop()
            if frame.is_table:
                self._insert(frame.row)
            return
        key = frame.key
        if key is None:
            raise RuntimeError("value outside a map key")
        path = frame.path + (key,)
        node = frame.node
        leaves = node.leaves.get(key) if node is not None else None
        submap = node.maps.get(key) if node is not None else None
        branch = node.branch.get(key) if node is not None else None

        if event == "start_map":
            if leaves:
                raise RootRejected("type_mismatch", path,
                                   declared=leaves[0].type, actual="object")
            if branch is not None:
                raise RootRejected("array_expected", path, actual="object",
                                   table=branch.name)
            if submap is not None:
                if frame.table_row is None:
                    raise RuntimeError("column tree without a row")
                self._push_object(submap, None, frame.table_row, False, path)
            else:
                # An unmapped branch: the key name is not plan metadata and
                # could itself be payload, so the diagnostic path stops at
                # the last declared segment.
                self._push_object(None, None, None, False,
                                  frame.path + (UNMAPPED,))
            return
        if event == "start_array":
            if leaves:
                raise RootRejected("type_mismatch", path,
                                   declared=leaves[0].type, actual="array")
            if submap is not None:
                raise RootRejected("structure_mismatch", path,
                                   expected="object", actual="array")
            if branch is not None:
                if frame.table_row is None:
                    raise RuntimeError("child array without a parent row")
                self._push_array(branch, frame.table_row, path, True)
                return
            self._push_array(None, None, frame.path + (UNMAPPED,), False)
            return
        # scalar
        if leaves:
            if len(leaves) != 1:
                raise RuntimeError("multiple leaves for one key")
            col = leaves[0]
            try:
                stored, presence = normalize(event, value, col.type)
            except TypeMismatch as tm:
                raise RootRejected("type_mismatch", path,
                                   declared=tm.declared, actual=tm.actual)
            except NumberOutOfRange:
                raise RootRejected("number_out_of_range", path,
                                   declared=col.type,
                                   max_exponent=MAX_NUMBER_EXPONENT)
            idx = self._col_index[frame.table_row.table.name][col.name]
            frame.table_row.present[idx] = presence
            frame.table_row.values[idx] = stored
            return
        if submap is not None:
            if event == "null":
                # an explicit null where the plan expects an intermediate
                # object: every column under it is absent (not an error)
                return
            raise RootRejected("structure_mismatch", path,
                               expected="object", actual=event)
        if branch is not None:
            if event == "null":
                raise RootRejected("array_is_null", path, table=branch.name)
            raise RootRejected("array_expected", path, actual=event,
                               table=branch.name)
        # unknown scalar: not projected

    def _array_event(self, frame, event, value):
        if event == "end_array":
            self.stack.pop()
            return
        if not frame.is_child:
            if event == "end_map" or event == "end_array":
                self.stack.pop()
                return
            frame.index += 1
            path = frame.path + (frame.index,)
            if event == "start_map":
                self._push_object(None, None, None, False, path)
            elif event == "start_array":
                self._push_array(None, None, path, False)
            return
        frame.index += 1
        index_path = frame.parent_row.index_path + [frame.index]
        path = frame.path + (frame.index,)
        if event != "start_map":
            raise RootRejected("item_not_object", path, actual=event,
                               table=frame.child.name)
        table = frame.child
        array_path = frame.parent_row.array_path + [[table.name, frame.index]]
        row = RowFrame(
            table, row_id(self.digest, self.root_index, array_path),
            frame.parent_row.row_id, frame.parent_row.root_row_id, index_path,
            array_path, self.root_index, self._next_ordinal(table.name))
        self._push_object(table.mapping, row, row, True, path)


# ---------------------------------------------------------------------------
# one preparation run
# ---------------------------------------------------------------------------

def _spool_and_hash(store, input_path, chunk_size):
    tmp_path = os.path.join(
        store.tmp_dir, "nested.%d.%s.input"
        % (os.getpid(), os.urandom(4).hex()))
    sponge = hashlib.sha256()
    size = 0
    with open(input_path, "rb") as src, open(tmp_path, "wb") as dst:
        while True:
            chunk = src.read(chunk_size)
            if not chunk:
                break
            sponge.update(chunk)
            dst.write(chunk)
            size += len(chunk)
    return sponge.hexdigest(), size, tmp_path


def prepare(store, plan, input_path, db_path, require_existing=False,
            chunk_size=65536):
    """Prepare one JSON source into the plan's output database.

    The source is first spooled to the store's scratch area while its digest
    is computed, so the digest recorded in the journal is exactly the bytes
    that get parsed.  The journal (progress, committed roots, rejections)
    lives in the managed store; the projected rows live in the output
    database.  Already-committed roots are skipped by index; uncommitted
    roots are reprocessed from scratch.
    """
    out = sn.open_output(db_path)
    try:
        sn.ensure_output_schema(out, plan)
        digest, size, tmp_path = _spool_and_hash(store, input_path, chunk_size)
        try:
            run = _run(out, plan, digest, size, tmp_path, require_existing,
                       chunk_size)
        finally:
            try:
                os.remove(tmp_path)
            except FileNotFoundError:
                pass
        return run
    finally:
        out.close()


def _run(out, plan, digest, size, tmp_path, require_existing, chunk_size):
    existing = out.execute(
        "SELECT * FROM ns_runs WHERE source_digest=?", (digest,)).fetchone()
    if require_existing and existing is None:
        raise UnknownSource(
            "unknown_source",
            "no run record for this source digest; run prepare first",
            source_digest=digest)
    if existing is not None and existing["plan_digest"] != plan.digest:
        raise sn.PlanError(
            "plan_mismatch",
            "this source was prepared under a different plan",
            source_digest=digest, run_plan=existing["plan_digest"],
            plan=plan.digest)
    if existing is not None and existing["input_size"] != size:
        raise SourceChanged(
            "source_changed", "source size differs from its run record",
            source_digest=digest, recorded=existing["input_size"], actual=size)
    if existing is None:
        out.execute(
            "INSERT INTO ns_runs(source_digest, plan_digest, input_size, "
            "status, roots_committed, roots_rejected, roots_seen, "
            "error_category, error_position, error_position_basis, "
            "started_at, updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            (digest, plan.digest, size, "in_progress", 0, 0, 0, None, None,
             None, now_iso(), now_iso()))
    else:
        out.execute(
            "UPDATE ns_runs SET status='in_progress', error_category=NULL, "
            "error_position=NULL, error_position_basis=NULL, updated_at=? "
            "WHERE source_digest=?", (now_iso(), digest))
    committed = {r["root_index"] for r in out.execute(
        "SELECT root_index FROM ns_roots WHERE source_digest=? "
        "AND status='committed'", (digest,))}
    _drop_uncommitted_rows(out, plan, digest, committed)

    projector = Projector(plan, out, digest, committed, chunk_size)
    sync_point("nested.before_parse")
    reader = None
    abort = None
    try:
        with open(tmp_path, "rb") as src:
            reader = CountingReader(src, chunk_size)
            _drive(projector, reader, chunk_size)
    except SourceError as exc:
        projector._rollback_root()
        abort = exc
    except ijson.JSONError as exc:
        projector._rollback_root()
        position, basis = parser_error_position(reader, exc)
        abort = SourceError("json_syntax",
                            "the JSON document is damaged; processing stopped",
                            position, basis,
                            projector.root_index
                            if projector.root_index >= 0 else None)
    except UnicodeDecodeError:
        projector._rollback_root()
        abort = SourceError("json_syntax",
                            "the JSON document contains invalid UTF-8",
                            reader.bytes_consumed, "upper_bound",
                            projector.root_index
                            if projector.root_index >= 0 else None)
    except Exception:
        projector._rollback_root()
        raise

    counts = _table_counts(out, digest)
    totals = _root_totals(out, digest)
    if abort is not None:
        out.execute(
            "UPDATE ns_runs SET status='aborted_syntax', error_category=?, "
            "error_position=?, error_position_basis=?, roots_committed=?, "
            "roots_rejected=?, roots_seen=?, updated_at=? "
            "WHERE source_digest=?",
            (abort.category, abort.position, abort.basis,
             totals["committed"], totals["rejected"], projector.roots_seen,
             now_iso(), digest))
    else:
        out.execute(
            "UPDATE ns_runs SET status=?, error_category=NULL, "
            "error_position=NULL, error_position_basis=NULL, "
            "roots_committed=?, roots_rejected=?, roots_seen=?, updated_at=? "
            "WHERE source_digest=?",
            ("complete_with_rejections" if totals["rejected"] else "complete",
             totals["committed"], totals["rejected"], projector.roots_seen,
             now_iso(), digest))
    summary = {
        "source_digest": digest,
        "plan_digest": plan.digest,
        "input_size": size,
        "roots_seen": projector.roots_seen,
        "roots_committed_this_run": projector.roots_committed,
        "roots_skipped_already_committed": projector.roots_skipped,
        "roots_rejected_this_run": projector.roots_rejected,
        "roots_committed_total": totals["committed"],
        "roots_rejected_total": totals["rejected"],
        "rows": counts,
        "database": os.path.abspath(
            out.execute("PRAGMA database_list").fetchone()["file"]),
    }
    if abort is not None:
        summary["status"] = "aborted"
        summary["error"] = abort.as_dict()
    else:
        summary["status"] = ("complete" if not totals["rejected"]
                             else "complete_with_rejections")
    return summary


def _drive(projector, reader, chunk_size):
    """Consume the top-level array and hand each item to the projector.

    Top-level structure lives here: the document must be one array; its
    items are the roots.  The projector only sees events inside an item,
    so an item's own start/end is balanced by this loop.  A non-array
    document, a non-object item, or trailing data is reported as a source
    or root failure without reading further.
    """
    depth = 0
    top_seen = False
    top_closed = False
    in_item = False
    root_index = -1
    parser = ijson.basic_parse(reader, buf_size=chunk_size)
    for event, value in parser:
        if not top_seen:
            if event != "start_array":
                raise SourceError(
                    "top_level_not_array",
                    "the top-level JSON value must be an array", 0,
                    "document_start")
            top_seen = True
            depth = 1
            continue
        if not in_item:
            if event == "end_array" and depth == 1:
                top_closed = True
                continue
            if top_closed:
                raise SourceError(
                    "trailing_data",
                    "unexpected data after the top-level array",
                    reader.bytes_consumed, "end_of_file")
            # start of one top-level item
            root_index += 1
            in_item = True
            if event == "start_map":
                if root_index in projector.skip_roots:
                    projector.skip_committed(root_index)
                    projector.mode = "skip"
                else:
                    projector.begin_root(root_index)
                    projector.mode = "project"
                depth = 2
                continue
            if event == "start_array":
                depth = 2
                # non-object item: skip it and record a rejection when done
                projector.root_index = root_index
                projector.roots_seen = root_index + 1
                projector.mode = "skip"
                projector.skip_reject = RootRejected(
                    "root_not_object", [], actual="array")
                continue
            # scalar item
            depth = 1
            in_item = False
            projector.reject_rootless(root_index, RootRejected(
                "root_not_object", [], actual=event))
            projector.mode = "idle"
            continue
        if in_item:
            if event in ("start_map", "start_array"):
                depth += 1
            elif event in ("end_map", "end_array"):
                depth -= 1
            item_closed = (depth == 1 and event in ("end_map", "end_array"))
            if projector.mode == "skip":
                if item_closed:
                    in_item = False
                    rej = projector.skip_reject
                    if rej is not None:
                        projector._record_rejection(rej)
                        projector.skip_reject = None
                    projector.mode = "idle"
                continue
            try:
                projector.handle(event, value)
            except RootRejected as rejection:
                projector.reject_root(rejection)
                projector.mode = "skip"
                if item_closed:
                    in_item = False
                    projector.mode = "idle"
                continue
            if item_closed:
                in_item = False
                try:
                    projector.commit_root()
                except RootRejected as rejection:
                    projector.reject_root(rejection)
                projector.mode = "idle"
    # The parser's stop must be checked: a missing closing bracket is
    # reported by ijson as a JSONError (premature EOF) before this point,
    # so here we only verify the structural invariants we observed.
    if not top_seen:
        raise SourceError("empty_document",
                          "the input contains no JSON value",
                          reader.bytes_consumed, "document_start")
    if not top_closed:
        raise SourceError(
            "json_syntax",
            "the top-level array was not closed",
            reader.bytes_consumed, "end_of_file")
def _drop_uncommitted_rows(out, plan, digest, committed):
    """Delete rows of this source whose root is not durably committed.

    The normal commit path makes rows and the per-root record appear
    atomically, so this is a no-op.  It exists so a database that somehow
    contains a half tree (for example rows left by a killed writer whose
    transaction was not rolled back, or by an operator) is repaired
    deterministically before that root is reprocessed, instead of colliding
    with the primary key.  Rows of committed roots are never touched.
    """
    keep = sorted(committed)
    for t in plan.tables:
        sql = ('DELETE FROM %s WHERE source_digest=? AND root_index NOT IN '
               '(%s)' % (sn.ident(t.name),
                         ",".join("?" for _ in keep)) if keep else
               'DELETE FROM %s WHERE source_digest=?' % sn.ident(t.name))
        args = [digest] + keep
        out.execute(sql, args)


def _root_totals(conn, digest):
    row = conn.execute(
        "SELECT SUM(status='committed') c, SUM(status='rejected') r "
        "FROM ns_roots WHERE source_digest=?", (digest,)).fetchone()
    return {"committed": row["c"] or 0, "rejected": row["r"] or 0}


def _table_counts(conn, digest):
    counts = {}
    for t in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'ns_%' "
            "AND name != 'meta' ORDER BY name"):
        name = t["name"]
        counts[name] = conn.execute(
            "SELECT COUNT(*) c FROM %s WHERE source_digest=?"
            % sn.ident(name), (digest,)).fetchone()["c"]
    return counts


# ---------------------------------------------------------------------------
# read interface
# ---------------------------------------------------------------------------

class UnknownTable(Exception):
    pass


def plan_tables(conn):
    row = conn.execute("SELECT value FROM meta WHERE key='plan_digest'"
                       ).fetchone()
    return row is not None


def table_names(conn):
    names = []
    for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'ns_%' "
            "AND name != 'meta' ORDER BY name"):
        names.append(r["name"])
    return names


def table_columns(conn, table):
    names = [r["name"] for r in conn.execute(
        "SELECT name FROM pragma_table_info(?)", (table,))]
    if not names:
        raise UnknownTable(table)
    return names


def iter_rows(conn, table, source_digest=None, root_index=None,
              parent_row_id=None, limit=None):
    """Yield projected rows one at a time, in reproducible input order.

    Order is (root_index, ordinal, row_id): creation order of rows inside a
    root, and root order in the file.  The cursor is never materialized;
    `limit` just stops yielding.  Unknown tables raise UnknownTable when the
    function is called, before iteration starts.
    """
    # validate eagerly (the generator itself would defer this)
    cols = table_columns(conn, table)
    return _iter_rows(conn, cols, table, source_digest, root_index,
                      parent_row_id, limit)


def _iter_rows(conn, cols, table, source_digest, root_index, parent_row_id,
               limit):
    where = []
    args = []
    if source_digest is not None:
        where.append("source_digest=?")
        args.append(source_digest)
    if root_index is not None:
        where.append("root_index=?")
        args.append(root_index)
    if parent_row_id is not None:
        where.append("parent_row_id=?")
        args.append(parent_row_id)
    if not where:
        where = ["1=1"]
    sql = ("SELECT * FROM %s WHERE %s ORDER BY root_index, ordinal, row_id"
           % (sn.ident(table), " AND ".join(where)))
    cur = conn.execute(sql, args)
    n = 0
    for row in cur:
        if limit is not None and n >= limit:
            return
        n += 1
        yield _row_dict(row, cols)


def _row_dict(row, cols):
    out = {
        "row_id": row["row_id"],
        "parent_row_id": row["parent_row_id"],
        "root_row_id": row["root_row_id"],
        "source": {
            "digest": row["source_digest"],
            "root_index": row["root_index"],
            "index_path": json.loads(row["index_path"]),
            "array_path": json.loads(row["array_path"]),
            "ordinal": row["ordinal"],
        },
        "values": {},
    }
    for c in cols:
        if c in sn.INTERNAL_COLUMNS or c.endswith("__present"):
            continue
        out["values"][c] = {"value": row[c],
                            "presence": row[c + "__present"]}
    return out


def children(conn, table, parent_row_id, limit=None):
    """Direct child rows of one parent row, in reproducible order."""
    return iter_rows(conn, table, parent_row_id=parent_row_id, limit=limit)


def root_records(conn, source_digest=None, root_index=None):
    where = []
    args = []
    if source_digest is not None:
        where.append("source_digest=?")
        args.append(source_digest)
    if root_index is not None:
        where.append("root_index=?")
        args.append(root_index)
    sql = ("SELECT * FROM ns_roots"
           + (" WHERE " + " AND ".join(where) if where else "")
           + " ORDER BY source_digest, root_index")
    for r in conn.execute(sql, args):
        yield {
            "source_digest": r["source_digest"],
            "root_index": r["root_index"],
            "status": r["status"],
            "category": r["category"],
            "path": r["path"],
            "root_row_id": r["root_row_id"],
            "counts": json.loads(r["counts_json"]) if r["counts_json"] else None,
            "detail": json.loads(r["detail_json"]) if r["detail_json"] else None,
        }


def run_records(conn, source_digest=None):
    sql = "SELECT * FROM ns_runs"
    args = []
    if source_digest is not None:
        sql += " WHERE source_digest=?"
        args.append(source_digest)
    sql += " ORDER BY started_at, source_digest"
    for r in conn.execute(sql, args):
        yield {k: r[k] for k in r.keys()}
