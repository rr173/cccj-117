"""Nested study-table preparation: declarative plan and output schema.

The module is deliberately independent of the seven-field observation
protocol and of the mail channel.  It reads one JSON file (a top-level array
of root objects) and writes its own relational SQLite database, one physical
table per plan table.  It never writes to vault.sqlite3, never emits
change-feed notifications and never computes research measures.

Plan format (JSON object, exactly version/root/tables):

  {
    "version": 1,
    "root": {
      "name": "participants",
      "columns": [{"name": "person", "path": ["person"], "type": "string"}, ...]
    },
    "tables": [
      {"name": "visits", "parent": "participants", "path": ["visits"],
       "columns": [{"name": "visit_label", "path": ["label"], "type": "string"},
                   ...]},
      ...
    ]
  }

  * table names are unique in the plan and must not begin with `ns_` (the
    internal bookkeeping namespace);
  * a child's `parent` must name a declared table (root or another child);
    parent pointers must be acyclic;
  * a child's `path` is a relative path from the parent object to the array
    holding the child items.  It is a list of literal string segments;
    `segments[0]` is a key of the parent object, later segments address
    nested maps.  The final segment is the array.  A path never means
    "index into an array": crossing an array always means a new table;
  * a column's `path` is a relative path from the object it belongs to
    (root object or child-array item) to a scalar.  It may traverse maps but
    never an array;
  * `type` is one of string / integer / decimal / boolean.  integer and
    decimal values are stored as exact decimal text (never via binary float);
  * column names are unique per table.  A name ending in `__present` or
    equal to an internal identity column is rejected, as is a path used for
    two columns in one table;
  * within one table, a key position cannot be simultaneously a scalar
    column, a map and a child array; two child tables of the same parent may
    not target the same path.  Such conflicting definitions refuse the
    whole plan before any input byte is read.

Everything else (physical naming, row identity, internal columns) is
handled by the engine.
"""
from __future__ import annotations

import json

TYPES = ("string", "integer", "decimal", "boolean")
RESERVED_TABLE_PREFIX = "ns_"
RESERVED_TABLE_PREFIXES = ("ns_", "sqlite_")
RESERVED_TABLE_NAMES = ("meta",)
INTERNAL_COLUMNS = ("row_id", "parent_row_id", "root_row_id", "source_digest",
                    "root_index", "index_path", "array_path", "ordinal")


class PlanError(Exception):
    """The plan is invalid.  Nothing has been read from the input file."""

    code = "invalid_plan"
    exit_code = 2

    def __init__(self, category, message, **fields):
        super().__init__(message)
        self.category = category
        self.fields = dict(fields)
        self.fields.setdefault("category", category)

    def as_dict(self):
        d = {"code": self.code, "error": str(self), "plan_error": self.category}
        d.update(self.fields)
        return d


def is_name(value) -> bool:
    return isinstance(value, str) and value != ""


def parse_segments(value, where):
    if not isinstance(value, list) or not value:
        raise PlanError("invalid_path",
                        "%s must be a non-empty array of string segments"
                        % where, where=where)
    for seg in value:
        if not isinstance(seg, str):
            raise PlanError("invalid_path",
                            "%s segments must be strings; array positions are "
                            "not addressable by a path" % where, where=where)
        if seg == "":
            raise PlanError("invalid_path",
                            "%s contains an empty key segment" % where,
                            where=where)
    return list(value)


class Column:
    __slots__ = ("name", "path", "type")

    def __init__(self, name, path, type_):
        self.name = name
        self.path = path
        self.type = type_

    def as_dict(self):
        return {"name": self.name, "path": self.path, "type": self.type}


class Table:
    __slots__ = ("name", "parent", "path", "columns", "mapping", "children")

    def __init__(self, name, parent, path, columns):
        self.name = name
        self.parent = parent
        self.path = path          # relative to the parent object ([] for root)
        self.columns = columns
        self.mapping = None       # MapNode, built by the compiler
        self.children = []

    def as_dict(self):
        return {"name": self.name, "parent": self.parent, "path": self.path,
                "columns": [c.as_dict() for c in self.columns]}


class MapNode:
    """Projection tree for one table's object level.

    A key position has exactly one role:
      * a scalar leaf for one column (`leaves`),
      * an intermediate map (`maps`),
      * an array crossing that opens a child table (`branch`).
    """

    __slots__ = ("leaves", "maps", "branch")

    def __init__(self):
        self.leaves = {}   # key -> list[Column] (path exhausted at this key)
        self.maps = {}     # key -> MapNode
        self.branch = {}   # key -> Table (child array)


def _insert_column(root_node, path, column, table):
    node = root_node
    for depth, seg in enumerate(path[:-1]):
        if seg in node.leaves:
            raise PlanError(
                "conflicting_definition",
                "key %r of table %r is used both as a scalar column and as "
                "an intermediate object" % (seg, table), table=table,
                key=seg)
        if seg in node.branch:
            raise PlanError(
                "conflicting_definition",
                "key %r of table %r is used both as a child array and as an "
                "intermediate object" % (seg, table), table=table, key=seg)
        node = node.maps.setdefault(seg, MapNode())
    leaf = path[-1]
    if leaf in node.maps:
        raise PlanError(
            "conflicting_definition",
            "key %r of table %r is used both as an intermediate object and "
            "as a scalar column" % (leaf, table), table=table, key=leaf)
    if leaf in node.branch:
        raise PlanError(
            "conflicting_definition",
            "key %r of table %r is used both as a child array and as a "
            "scalar column" % (leaf, table), table=table, key=leaf)
    node.leaves.setdefault(leaf, []).append(column)


def _insert_branch(root_node, path, child_table):
    node = root_node
    for seg in path[:-1]:
        if seg in node.leaves:
            raise PlanError(
                "conflicting_definition",
                "key %r of table %r is used both as a scalar column and as "
                "the path of child table %r"
                % (seg, child_table.parent, child_table.name),
                table=child_table.parent, key=seg,
                child=child_table.name)
        if seg in node.branch:
            raise PlanError(
                "conflicting_definition",
                "key %r of table %r is already the array of child table %r; "
                "another child cannot cross it"
                % (seg, child_table.parent, node.branch[seg].name),
                table=child_table.parent, key=seg,
                child=child_table.name)
        node = node.maps.setdefault(seg, MapNode())
    leaf = path[-1]
    if leaf in node.branch:
        raise PlanError(
            "duplicate_table_target",
            "child tables %r and %r of parent %r target the same path"
            % (node.branch[leaf].name, child_table.name, child_table.parent),
            table=child_table.parent, key=leaf)
    if leaf in node.leaves:
        raise PlanError(
            "conflicting_definition",
            "key %r of table %r is used both as a scalar column and as the "
            "array of child table %r"
            % (leaf, child_table.parent, child_table.name),
            table=child_table.parent, key=leaf, child=child_table.name)
    if leaf in node.maps:
        raise PlanError(
            "conflicting_definition",
            "key %r of table %r is used both as an intermediate object and "
            "as the array of child table %r"
            % (leaf, child_table.parent, child_table.name),
            table=child_table.parent, key=leaf, child=child_table.name)
    node.branch[leaf] = child_table


class Plan:
    def __init__(self, raw):
        self.raw = raw
        self.tables = []
        self.by_name = {}
        self.root_name = None
        self.digest = None
        self._compile()

    def _compile(self):
        raw = self.raw
        if not isinstance(raw, dict):
            raise PlanError("invalid_plan", "plan must be a JSON object")
        if set(raw.keys()) != {"version", "root", "tables"}:
            raise PlanError("invalid_plan",
                            "plan must have exactly the keys version, root "
                            "and tables")
        if raw["version"] != 1:
            raise PlanError("unsupported_version",
                            "unsupported plan version", version=raw["version"])
        root = raw["root"]
        if not isinstance(root, dict):
            raise PlanError("invalid_plan", "root must be an object")
        if set(root.keys()) != {"name", "columns"}:
            raise PlanError("invalid_plan",
                            "root must have exactly name and columns")
        root_name = root.get("name")
        if not is_name(root_name):
            raise PlanError("invalid_table_name",
                            "root table name must be a non-empty string")
        self._check_table_name(root_name, "root.name")
        root_columns = self._compile_columns(root["columns"], "root.columns",
                                             root_name)
        root_table = Table(root_name, None, [], root_columns)

        tables_raw = raw["tables"]
        if not isinstance(tables_raw, list):
            raise PlanError("invalid_plan", "tables must be an array")
        children = []
        names = {root_name}
        for i, t in enumerate(tables_raw):
            where = "tables[%d]" % i
            if not isinstance(t, dict):
                raise PlanError("invalid_plan", "%s must be an object" % where,
                                where=where)
            if set(t.keys()) != {"name", "parent", "path", "columns"}:
                raise PlanError("invalid_plan",
                                "%s must have exactly name, parent, path and "
                                "columns" % where, where=where)
            name = t.get("name")
            if not is_name(name):
                raise PlanError("invalid_table_name",
                                "%s.name must be a non-empty string" % where,
                                where=where)
            self._check_table_name(name, where + ".name")
            if name in names:
                raise PlanError("duplicate_table",
                                "duplicate table name %r" % name, table=name)
            names.add(name)
            parent = t.get("parent")
            if not is_name(parent):
                raise PlanError("unknown_parent",
                                "%s.parent must be a non-empty string" % where,
                                where=where)
            path = parse_segments(t.get("path"), where + ".path")
            cols = self._compile_columns(t.get("columns"),
                                         where + ".columns", name)
            children.append(Table(name, parent, path, cols))

        # parent existence (root is a valid parent) and table-level cycles
        for t in children:
            if t.parent != root_name and t.parent not in names:
                raise PlanError("unknown_parent",
                                "table %r names unknown parent %r"
                                % (t.name, t.parent), table=t.name,
                                parent=t.parent)
        parent_of = {t.name: t.parent for t in children}
        for start in parent_of:
            seen = []
            cur = start
            while cur != root_name:
                if cur in seen:
                    raise PlanError("parent_cycle",
                                    "parent pointers form a cycle",
                                    cycle=seen + [cur])
                seen.append(cur)
                cur = parent_of.get(cur, root_name)

        self.root_name = root_name
        self.tables = [root_table] + children
        self.by_name = {t.name: t for t in self.tables}
        for t in children:
            self.by_name[t.parent].children.append(t)

        # Build the projection tree per table: its own columns plus the
        # first path segment of each direct child; conflicts are plan errors.
        for t in self.tables:
            node = MapNode()
            for c in t.columns:
                _insert_column(node, c.path, c, t.name)
            for ch in t.children:
                _insert_branch(node, ch.path, ch)
            t.mapping = node

        # stable traversal order: root then depth-first declaration order
        order = []

        def visit(t):
            order.append(t)
            for ch in t.children:
                visit(ch)

        visit(root_table)
        if len(order) != len(self.tables):
            raise PlanError("parent_cycle", "unreachable plan tables")
        self.tables = order
        for t in self.tables:
            t.children = [c for c in order if c.parent == t.name]
        self.digest = sha256_text(canonical_plan_text(raw))

    def _check_table_name(self, name, where):
        if not is_name(name):
            raise PlanError("invalid_table_name",
                            "%s must be a non-empty string" % where,
                            where=where)
        if (name.startswith(RESERVED_TABLE_PREFIXES)
                or name in RESERVED_TABLE_NAMES):
            raise PlanError("reserved_table_name",
                            "table name %r is reserved (prefixes %s and %s "
                            "are internal)" % (
                                name, ", ".join(RESERVED_TABLE_PREFIXES),
                                ", ".join(RESERVED_TABLE_NAMES)),
                            table=name)

    def _compile_columns(self, cols_raw, where, table):
        if not isinstance(cols_raw, list) or not cols_raw:
            raise PlanError("invalid_plan",
                            "%s must be a non-empty array" % where,
                            where=where, table=table)
        cols = []
        names = set()
        paths = {}
        for j, c in enumerate(cols_raw):
            w = "%s[%d]" % (where, j)
            if not isinstance(c, dict):
                raise PlanError("invalid_plan", "%s must be an object" % w,
                                where=w)
            if set(c.keys()) != {"name", "path", "type"}:
                raise PlanError("invalid_plan",
                                "%s must have exactly name, path and type" % w,
                                where=w)
            name = c.get("name")
            if not is_name(name):
                raise PlanError("invalid_column",
                                "%s.name must be a non-empty string" % w,
                                where=w, table=table)
            if name in INTERNAL_COLUMNS:
                raise PlanError("reserved_column_name",
                                "column %r conflicts with an internal identity "
                                "column" % name, table=table, column=name)
            if name.endswith("__present"):
                raise PlanError("reserved_column_name",
                                "column names ending in __present are reserved "
                                "for presence markers", table=table,
                                column=name)
            if name in names:
                raise PlanError("duplicate_column",
                                "duplicate column name %r" % name,
                                table=table, column=name)
            names.add(name)
            type_ = c.get("type")
            if type_ not in TYPES:
                raise PlanError("invalid_column_type",
                                "column %r has unsupported type %r (expected "
                                "one of %s)"
                                % (name, type_, ",".join(TYPES)),
                                table=table, column=name)
            path = parse_segments(c.get("path"), w + ".path")
            key = tuple(path)
            if key in paths:
                raise PlanError("duplicate_column_path",
                                "columns %r and %r of table %r project the "
                                "same path" % (paths[key], name, table),
                                table=table, column=name)
            paths[key] = name
            cols.append(Column(name, path, type_))
        return cols


def canonical_plan_text(raw) -> str:
    return json.dumps(raw, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"))


def sha256_text(text: str) -> str:
    import hashlib
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def load_plan(path: str) -> Plan:
    try:
        with open(path, "rb") as f:
            data = f.read()
    except OSError as e:
        raise PlanError("plan_unreadable", "cannot read plan file: %s" % e)
    try:
        raw = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise PlanError("plan_not_json", "plan file is not valid JSON: %s" % e)
    return Plan(raw)


# ---------------------------------------------------------------------------
# physical schema
# ---------------------------------------------------------------------------

def ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


qcol = ident


def source_columns():
    """(name, sqlite type, not null, primary key) for identity columns."""
    return [
        ("row_id", "TEXT", 1, 1),
        ("parent_row_id", "TEXT", 0, 0),
        ("root_row_id", "TEXT", 1, 0),
        ("source_digest", "TEXT", 1, 0),
        ("root_index", "INTEGER", 1, 0),
        ("index_path", "TEXT", 1, 0),
        ("array_path", "TEXT", 1, 0),
        ("ordinal", "INTEGER", 1, 0),
    ]


def table_ddl(t: Table) -> str:
    parts = []
    for name, sqltype, nn, pk in source_columns():
        if pk:
            continue
        parts.append("%s %s%s" % (ident(name), sqltype,
                                  " NOT NULL" if nn else ""))
    parts.append("%s TEXT NOT NULL PRIMARY KEY" % ident("row_id"))
    for c in t.columns:
        value_type = {"string": "TEXT", "integer": "TEXT",
                      "decimal": "TEXT", "boolean": "INTEGER"}[c.type]
        parts.append("%s %s" % (ident(c.name), value_type))
        parts.append("%s INTEGER NOT NULL DEFAULT 0"
                     % ident(c.name + "__present"))
    return "CREATE TABLE IF NOT EXISTS %s (%s)" % (ident(t.name),
                                                   ", ".join(parts))


def index_ddls(t: Table) -> list:
    return [
        "CREATE INDEX %s ON %s (%s)" % (
            ident("ix_" + t.name + "__parent"), ident(t.name),
            ident("parent_row_id")),
        "CREATE INDEX {0} ON {1} ({2}, {3})".format(
            ident("ix_" + t.name + "__source"), ident(t.name),
            ident("source_digest"), ident("ordinal")),
        "CREATE INDEX {0} ON {1} ({2})".format(
            ident("ix_" + t.name + "__root"), ident(t.name),
            ident("root_row_id")),
    ]


NESTED_OUTPUT_DDL = [
    """CREATE TABLE IF NOT EXISTS meta(
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
)""",
    """CREATE TABLE IF NOT EXISTS ns_runs(
  source_digest TEXT PRIMARY KEY,
  plan_digest TEXT NOT NULL,
  input_size INTEGER NOT NULL,
  status TEXT NOT NULL,
  roots_committed INTEGER NOT NULL DEFAULT 0,
  roots_rejected INTEGER NOT NULL DEFAULT 0,
  roots_seen INTEGER NOT NULL DEFAULT 0,
  error_category TEXT,
  error_position INTEGER,
  error_position_basis TEXT,
  started_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
)""",
    """CREATE TABLE IF NOT EXISTS ns_roots(
  source_digest TEXT NOT NULL,
  root_index INTEGER NOT NULL,
  status TEXT NOT NULL,
  category TEXT,
  path TEXT,
  detail_json TEXT,
  root_row_id TEXT,
  counts_json TEXT,
  updated_at TEXT NOT NULL,
  PRIMARY KEY(source_digest, root_index)
)""",
]


def ensure_output_schema(conn, plan: Plan) -> None:
    """Create (or verify) the output database schema.

    The plan digest is bound to the database before any input is read.  A
    different plan is refused.  All DDL runs in one transaction so a crash
    cannot leave a half-created schema.
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        for stmt in NESTED_OUTPUT_DDL:
            conn.execute(stmt)
        row = conn.execute(
            "SELECT value FROM meta WHERE key='plan_digest'").fetchone()
        if row is not None:
            if row["value"] != plan.digest:
                raise PlanError(
                    "plan_mismatch",
                    "output database is bound to a different plan",
                    output_plan=row["value"], plan=plan.digest)
            conn.execute("COMMIT")
            return
        for t in plan.tables:
            conn.execute(table_ddl(t))
        for t in plan.tables:
            for stmt in index_ddls(t):
                conn.execute(stmt)
        conn.execute("INSERT INTO meta(key, value) VALUES('plan_digest', ?)",
                     (plan.digest,))
        conn.execute("INSERT INTO meta(key, value) VALUES('plan_json', ?)",
                     (canonical_plan_text(plan.raw),))
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise


# ---------------------------------------------------------------------------
# managed run journal (inside the store's vault.sqlite3)
# ---------------------------------------------------------------------------

def open_output(path: str):
    import sqlite3
    conn = sqlite3.connect(path, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=DELETE")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA cache_size=-8192")   # bounded page cache: 8 MiB
    return conn
