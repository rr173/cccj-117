"""Persistent outbound change feed for downstream retrieval indexes.

What the feed is
----------------
A notification says only: "this (study, observation) must be re-read from the
source" (`refresh`) or "this (study, observation) has been withdrawn and must
never be used again" (`remove`).  It never carries display_name / measure /
memo, and there is no per-consumer payload copy, snapshot or historical
payload download.  Consumers re-read (or drop) the existing source paths.

Producers (and the transaction they share)
------------------------------------------
Events are produced ONLY inside the business transaction that causes them and
are committed or rolled back with it - never "confirm success, then append a
notification":

  * intake (`intake.import_batch`): a new observation, or a newly stored
    highest edition -> refresh.  Re-delivering an edition that already exists
    unchanged (`accepted_existing`), and accepting a legal older edition that
    does not become the maximum, produce no event.  Barrier-suppressed lines
    are recorded in the receipt but never produce events.  An aborted batch
    leaves nothing to consume.
  * withdrawal (`withdraw.register`): the transaction that inserts the barrier
    rows also writes remove events for every observation id owned by each
    newly withdrawn person.  They are consumable the instant the barrier is
    committed - file cleanup may still be running.  Barrier inserts use
    INSERT OR IGNORE, so an object claimed by an earlier withdrawal can never
    be announced again by a later one.

Event identity and ordering
---------------------------
  seq       global integer assigned inside the business transaction.
  frame_id  consecutive number of one business operation (one commit).
  frame_ts  timestamp of the frame's first event.
  Within a frame events are deduplicated by object and sorted by
  (study, observation); for one object `remove` dominates `refresh`.  A
  `remove` already emitted for an object is never emitted twice.  Across
  frames the wire order is exactly seq order: (frame_ts, seq) with frame_id
  numbering is unambiguous.

Consumers
---------
A consumer is a persistent named process with its own durable `acked_seq`.  A
new registration defaults to "now" (the current tail); historical events are
not delivered unless an explicit start is requested.  An explicit start must
still be retained - otherwise registration fails with the gap and the
earliest available position.  Two consumers never advance each other's
position.  Registering enables the consumer (receivable); disabling releases
its retention hold and revokes outstanding credentials; re-enabling the same
name is a new lifecycle with a new generation and mandatory new start point.

Delivery
--------
`pull` materialises at most ONE pending delivery per consumer, durably
(`cf_deliveries` + `cf_delivery_frames`), before any line is written.  Frames
are committed frames chosen whole up to the target event count: a frame is
never split and the first frame over the target is sent whole.  Everything up
to that frame's end is pinned, so a re-pull replays the same range and the
same token even after new writes or after the sending process died halfway
through its output.  Acknowledgement is explicit and advances the position
only to the delivery's through_seq; the same token may be re-acked; tokens
from another consumer, an older lifecycle, or not the current pending range
advance nothing.

Wire protocol (one JSON-text line per item)
-------------------------------------------
  START delivery=<token> from=<s> through=<e> target=<n> frames=<k> head=<h> empty=<0|1>
  FRAME id=<fid> index=<i> of=<k> ts=<ts> start=<s> end=<e> events=<n>
  EVENT seq=<n> frame=<fid> ordinal=<i> op=<refresh|remove> study=<s> observation=<o>
  ENDFRAME id=<fid> events=<n> first=<s> last=<e>
  END events=<n> frames=<k> empty=<0|1>
An empty result has START empty=1 (no token) and END events=0 frames=0
empty=1; no delivery row is created.  A consumer detects truncation because
the per-frame counts, the frame count and the final END line are missing or
inconsistent.

A very large frame is streamed record by record from a SQLite cursor; it is
never assembled into a Python array.

Retention
---------
`trim` is explicit.  It may delete only complete frames that every ENABLED
consumer has fully acknowledged; a slow (still enabled) consumer blocks it.
With no enabled consumer at all, trimming requires the operator's explicit
upper bound (--through-seq).  The durable floor lives in `cf_meta`.  Trimming
and registration serialize on the same store-wide flock and both re-check the
floor under that lock, so a concurrent trim/register yields one explainable
outcome: registration refused with the floor, or registration accepted while
the events are still retained.

Installation: the tables come from `db.SCHEMA`, which `Store.ensure()` runs
on every command, so no separate migration step is needed; see README §9.
"""
from __future__ import annotations

import os
import time
from typing import Iterator, Optional

from .store import study_lock
from .util import now_iso, sha256_hex

SYNC_DIR_ENV = "RVAULT_CF_SYNC_DIR"
SYNC_WAIT_ENV = "RVAULT_CF_SYNC_WAIT"
SYNC_TIMEOUT = 120.0


# --------------------------------------------------------------------------
# sync points (self-contained namespace: the demo does not need to export
# RVAULT_SYNC_DIR for every command)
# --------------------------------------------------------------------------
def cf_sync(name: str) -> None:
    d = os.environ.get(SYNC_DIR_ENV)
    if d:
        _sync_in(d, name)
    sd = os.environ.get("RVAULT_SYNC_DIR")
    if sd and sd != d:
        _sync_in(sd, name)


def _sync_in(d: str, name: str) -> None:
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, name + ".reached"), "w") as f:
        f.write(str(os.getpid()))
    wait = {w.strip() for w in os.environ.get(SYNC_WAIT_ENV, "").split(",") if w.strip()}
    if name not in wait:
        return
    proceed = os.path.join(d, name + ".proceed")
    deadline = time.time() + SYNC_TIMEOUT
    while time.time() < deadline:
        if os.path.exists(proceed):
            return
        time.sleep(0.05)
    raise RuntimeError("change-feed sync point timed out: " + name)


# --------------------------------------------------------------------------
# errors
# --------------------------------------------------------------------------
class ChangeFeedError(Exception):
    code = "cf_error"
    exit_code = 2

    def __init__(self, message: str, **fields):
        super().__init__(message)
        self.fields = fields

    def as_dict(self) -> dict:
        d = {"code": self.code, "error": str(self)}
        d.update(self.fields)
        return d


class UnknownConsumer(ChangeFeedError):
    code = "unknown_consumer"


class ConsumerExists(ChangeFeedError):
    code = "consumer_exists"


class LifecycleConflict(ChangeFeedError):
    code = "lifecycle_conflict"


class ConsumerDisabled(ChangeFeedError):
    code = "consumer_disabled"


class StartOutOfRange(ChangeFeedError):
    """A requested start is older than the retained floor (a data gap)."""

    code = "start_out_of_range"
    exit_code = 6

    def __init__(self, requested: int, floor: int, earliest: Optional[int],
                 reason: str = "gap"):
        super().__init__("requested start %d is older than the retained floor %d"
                         % (requested, floor))
        self.requested = requested
        self.floor = floor
        self.earliest = earliest
        self.reason = reason

    def as_dict(self) -> dict:
        return {"code": self.code,
                "error": "requested start is no longer retained",
                "reason": self.reason,
                "requested_seq": self.requested,
                "retention_floor": self.floor,
                "earliest_available_seq": self.earliest}


class DeliveryConflict(ChangeFeedError):
    code = "delivery_in_progress"


class AckRejected(ChangeFeedError):
    code = "ack_rejected"


class TrimError(ChangeFeedError):
    code = "trim_rejected"


class UnsupportedMode(ChangeFeedError):
    code = "unsupported_mode"


# --------------------------------------------------------------------------
# primitives
# --------------------------------------------------------------------------
def cf_lock(store):
    """Serialize feed bookkeeping (event commit, delivery materialisation,
    ack, trim, register/disable).  Study '*' is never used by business
    operations, so a paused consumer never blocks intake/withdraw."""
    return study_lock(store, "*")


def _meta(conn, name: str) -> int:
    row = conn.execute("SELECT value FROM cf_meta WHERE name=?", (name,)).fetchone()
    return 0 if row is None else int(row["value"])


def _set_meta(conn, name: str, value: int) -> None:
    conn.execute("INSERT INTO cf_meta(name, value) VALUES (?,?) "
                 "ON CONFLICT(name) DO UPDATE SET value=excluded.value", (name, value))


def _consumer(conn, name: str):
    return conn.execute("SELECT * FROM cf_consumers WHERE name=?", (name,)).fetchone()


def _live_consumer(conn, name: str):
    return conn.execute("SELECT * FROM cf_consumers WHERE name=? AND disabled_at IS NULL",
                        (name,)).fetchone()


def _tail(conn) -> int:
    return _meta(conn, "head_seq")


def _earliest_available(conn) -> int:
    """First position a consumer may start consuming from.  Positions at or
    below the retention floor have been deleted; when nothing at all is
    retained the first future event's position is reported."""
    floor = _meta(conn, "retention_floor")
    row = conn.execute("SELECT MIN(seq) AS s FROM cf_events").fetchone()
    retained = _tail(conn) + 1 if row is None or row["s"] is None else int(row["s"])
    return max(floor + 1, retained)



def _rollback(conn) -> None:
    try:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
    except Exception:
        pass



# --------------------------------------------------------------------------
# producer-side collector
# --------------------------------------------------------------------------
class EventCollector:
    """Collects object intents for ONE open business transaction.

    Intents live in the `cf_intents` staging table, keyed by a per-transaction
    session id, so deduplication, ordering and the "remove dominates refresh"
    rule are done by SQLite (on disk, with a bounded page cache) instead of a
    Python dict that grows with the batch.  That keeps a 200k-line intake
    batch's memory flat.  `commit()` turns the staged intents into exactly one
    committed frame inside the caller's transaction.
    """

    def __init__(self, session: Optional[str] = None):
        self.session = session or ("S" + sha256_hex(
            ("%d\x00%d" % (os.getpid(), time.time_ns())).encode("utf-8"))[:16])


    def stage(self, conn, study: str, observation: str, op: str) -> None:
        """Record one intent on the transaction's connection."""
        if op == "remove":
            conn.execute(
                "INSERT INTO cf_intents(session, study, observation, op) "
                "VALUES (?,?,?,'remove') ON CONFLICT(session, study, observation) "
                "DO UPDATE SET op='remove'", (self.session, study, observation))
        else:
            # never downgrade a removal in the same transaction
            conn.execute(
                "INSERT INTO cf_intents(session, study, observation, op) "
                "VALUES (?,?,?,'refresh') ON CONFLICT(session, study, observation) "
                "DO NOTHING", (self.session, study, observation))

    def commit(self, conn, actor: str) -> Optional[int]:
        """Convert the staged intents into ONE frame inside the caller's open
        transaction and return its frame_id (None when empty).  The frame rows
        and the business rows are committed or rolled back together.

        The frame is built with INSERT ... SELECT + ROW_NUMBER(), so a batch
        with hundreds of thousands of objects streams from the staging table
        into the frame without materialising a Python list."""
        alive = ("session=? AND NOT EXISTS (SELECT 1 FROM cf_removed_objects r "
                 "WHERE r.study=cf_intents.study "
                 "  AND r.observation=cf_intents.observation)")
        count = conn.execute(
            "SELECT COUNT(*) AS c FROM cf_intents WHERE " + alive,
            (self.session,)).fetchone()["c"]
        if count == 0:
            conn.execute("DELETE FROM cf_intents WHERE session=?", (self.session,))
            return None
        head = _tail(conn)
        ts = now_iso()
        frame_id = _meta(conn, "frame_seq") + 1
        start_seq, end_seq = head + 1, head + count
        conn.execute(
            "INSERT INTO cf_events(frame_id, ordinal, seq, op, study, observation) "
            "SELECT ?, ROW_NUMBER() OVER (ORDER BY study, observation), "
            "       ? + ROW_NUMBER() OVER (ORDER BY study, observation), "
            "       op, study, observation "
            "FROM cf_intents WHERE " + alive,
            (frame_id, head, self.session))
        conn.execute(
            "INSERT INTO cf_frames(frame_id, frame_ts, start_seq, end_seq, "
            "event_count, actor) VALUES (?,?,?,?,?,?)",
            (frame_id, ts, start_seq, end_seq, count, actor))
        _set_meta(conn, "head_seq", end_seq)
        _set_meta(conn, "frame_seq", frame_id)
        conn.execute(
            "INSERT OR IGNORE INTO cf_removed_objects(study, observation) "
            "SELECT study, observation FROM cf_intents WHERE " + alive +
            " AND op='remove'", (self.session,))
        conn.execute("DELETE FROM cf_intents WHERE session=?", (self.session,))
        cf_sync("feed.commit.finalize")
        return frame_id


# --------------------------------------------------------------------------
# producers
# --------------------------------------------------------------------------
def on_withdrawal_registered(conn, wid: str, study: str, persons: list) -> Optional[int]:
    """Called by withdraw.register inside the barrier-registration transaction.

    Emits `remove` for every observation id owned by each newly withdrawn
    person.  Only NEWLY claimed barrier pairs produce events: the barrier
    insert uses INSERT OR IGNORE, and we derive the object set from the
    pairs this transaction actually claimed.  That is what makes the frame
    match the source objects exactly, and what stops an overlapping later
    withdrawal from re-announcing an object the first one already removed.

    `observation_owner` is kept as an identity tombstone after erasure, so
    the join finds exactly the objects that exist (or existed) for these
    persons in this study.
    """
    col = EventCollector()
    rows = conn.execute(
        "SELECT o.study, o.observation FROM observation_owner o "
        "JOIN barrier b ON b.study=o.study AND b.person=o.person "
        "WHERE o.study=? AND o.person IN (%s) AND b.withdrawal_id=? "
        "ORDER BY o.study, o.observation"
        % ",".join("?" * len(persons)),
        [study] + list(persons) + [wid]).fetchall()
    for r in rows:
        col.stage(conn, r["study"], r["observation"], "remove")
    return col.commit(conn, "withdraw:" + wid)


class LineTracker:
    """Wraps one intake line.  `begin` records the object's current highest
    edition before the line is applied; `end` asks the database whether the
    maximum grew and turns that into a refresh intent.  It observes the write
    that actually happened, so duplicate deliveries and legal older editions
    (which insert no row) never produce an event.  `end` also emits the
    after-write sync point used by the demo to interleave a withdrawal
    between a line's insert and the batch COMMIT."""

    def __init__(self, conn, collector: EventCollector):
        self.conn = conn
        self.collector = collector
        self._prev = None
        self._study = None
        self._obs = None

    def begin(self, study: str, observation: str) -> None:
        self._study, self._obs = study, observation
        self._prev = self.conn.execute(
            "SELECT MAX(edition) AS m FROM editions WHERE study=? AND observation=?",
            (study, observation)).fetchone()["m"]
        cf_sync("feed.intake.before_line")

    def end(self) -> None:
        study, obs, prev = self._study, self._obs, self._prev
        self._prev = self._study = self._obs = None
        if study is None:
            return
        # ONE read of the current maximum decides everything:
        #   * grew                      -> new highest edition, refresh
        #   * unchanged / still lower   -> duplicate or legal older edition: no event
        #   * gone (rows deleted)       -> the object was withdrawn and erased
        #                                  between the two statements; the
        #                                  withdrawal's own remove frame owns it
        now = self.conn.execute(
            "SELECT MAX(edition) AS m FROM editions WHERE study=? AND observation=?",
            (study, obs)).fetchone()["m"]
        if now is not None and (prev is None or now > prev):
            self.collector.stage(self.conn, study, obs, "refresh")


# --------------------------------------------------------------------------
# registration / lifecycle
# --------------------------------------------------------------------------
def register(store, name: str, start_from: Optional[int] = None,
             reactivate: bool = False) -> dict:
    """Create (or re-enable) a consumer.

    `start_from` is the first event seq the consumer wants to receive.  By
    default the consumer starts at the current tail (future changes only).
    An explicit position must still be retained; an older one fails with the
    gap and the earliest available position - it is never silently moved.
    """
    if not isinstance(name, str) or not name:
        raise ValueError("consumer name must be a non-empty string")
    if start_from is not None and start_from < 1:
        raise ValueError("start position must be >= 1")
    with cf_lock(store):
        conn = store.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            head = _tail(conn)
            floor = _meta(conn, "retention_floor")
            row = _consumer(conn, name)
            if row is not None and row["disabled_at"] is None:
                raise ConsumerExists("consumer %r is already registered" % name,
                                     consumer=name)
            if row is not None and not reactivate:
                raise ConsumerDisabled(
                    "consumer %r is disabled; re-enabling is a new lifecycle and "
                    "requires --reactivate with an explicit start" % name,
                    consumer=name, tail_seq=head, retention_floor=floor)
            if row is not None and start_from is None:
                raise LifecycleConflict(
                    "re-enabling consumer %r starts a new lifecycle: an explicit "
                    "start position is required" % name, consumer=name)
            if start_from is None:
                first_seq, mode = head + 1, "now"
            else:
                if start_from > head + 1:
                    raise ValueError("start position %d is beyond the current tail %d"
                                     % (start_from, head))
                if start_from <= floor:
                    raise StartOutOfRange(start_from, floor,
                                          _earliest_available(conn))
                first_seq, mode = start_from, "explicit"
            generation = 1 if row is None else int(row["generation"]) + 1
            conn.execute(
                "INSERT INTO cf_consumers(name, created_at, generation, start_seq, "
                "acked_seq, disabled_at) VALUES (?,?,?,?,?,NULL) "
                "ON CONFLICT(name) DO UPDATE SET created_at=excluded.created_at, "
                "generation=excluded.generation, start_seq=excluded.start_seq, "
                "acked_seq=excluded.acked_seq, disabled_at=NULL",
                (name, now_iso(), generation, first_seq, first_seq - 1))
            # Interleaving hook: while this point is held the registration
            # transaction owns the feed lock, so a concurrent trim sees the
            # new consumer (and its retention hold) only after commit.
            cf_sync("feed.register.before_commit")
            conn.execute("COMMIT")
            return {"consumer": name, "generation": generation,
                    "start_seq": first_seq, "acked_seq": first_seq - 1,
                    "mode": mode, "retention_floor": floor, "tail_seq": head}
        except Exception:
            _rollback(conn)
            raise
        finally:
            conn.close()


def disable(store, name: str) -> dict:
    with cf_lock(store):
        conn = store.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = _live_consumer(conn, name)
            if row is None:
                if _consumer(conn, name) is None:
                    raise UnknownConsumer("unknown consumer %r" % name, consumer=name)
                raise ConsumerDisabled("consumer %r is already disabled" % name,
                                       consumer=name)
            conn.execute("UPDATE cf_consumers SET disabled_at=? WHERE name=?",
                         (now_iso(), name))
            pending = [r["delivery_id"] for r in conn.execute(
                "SELECT delivery_id FROM cf_deliveries WHERE consumer=? AND state='pending'",
                (name,))]
            conn.execute("DELETE FROM cf_delivery_frames WHERE delivery_id IN "
                         "(SELECT delivery_id FROM cf_deliveries "
                         "WHERE consumer=? AND state='pending')", (name,))
            conn.execute("DELETE FROM cf_deliveries WHERE consumer=? AND state='pending'",
                         (name,))
            conn.execute("UPDATE cf_deliveries SET state='revoked' "
                         "WHERE consumer=? AND state='acked'", (name,))
            conn.execute("COMMIT")
            cf_sync("feed.after_disable")
            return {"consumer": name, "enabled": False,
                    "generation": int(row["generation"]),
                    "revoked_deliveries": len(pending),
                    "retention_hold_released": True, "tail_seq": _tail(conn)}
        except Exception:
            _rollback(conn)
            raise
        finally:
            conn.close()


# --------------------------------------------------------------------------
# delivery
# --------------------------------------------------------------------------
def _pick_frames(conn, after: int, target: int) -> list:
    """Whole committed frames with start_seq > after, at most `target`
    events; the first frame is always taken whole, even when it exceeds the
    target.  Streams the frame index instead of reading every frame row."""
    chosen, total = [], 0
    cur = conn.execute(
        "SELECT frame_id, frame_ts, start_seq, end_seq, event_count FROM cf_frames "
        "WHERE start_seq > ? ORDER BY start_seq", (after,))
    for r in cur:
        if chosen and total + r["event_count"] > target:
            break
        chosen.append(r)
        total += r["event_count"]
        if total >= target:
            break
    return chosen


def _delivery_frames(conn, delivery_id: str) -> list:
    return conn.execute(
        "SELECT f.frame_id, f.frame_ts, f.start_seq, f.end_seq, f.event_count "
        "FROM cf_delivery_frames df JOIN cf_frames f ON f.frame_id = df.frame_id "
        "WHERE df.delivery_id=? ORDER BY f.start_seq", (delivery_id,)).fetchall()


def _materialize(conn, consumer_row, target: int) -> dict:
    """Return the single pending delivery (creating it when absent).
    Re-pulling while a delivery is pending returns its exact range and token,
    regardless of events written in the meantime."""
    name = consumer_row["name"]
    pending = conn.execute(
        "SELECT * FROM cf_deliveries WHERE consumer=? AND state='pending'",
        (name,)).fetchone()
    if pending is not None:
        return {"delivery": pending, "frames": _delivery_frames(conn, pending["delivery_id"]),
                "replayed": True}
    acked = int(consumer_row["acked_seq"])
    head = _tail(conn)
    if head <= acked:
        return {"delivery": None, "frames": [], "replayed": False}
    floor = _meta(conn, "retention_floor")
    if acked < floor:
        raise StartOutOfRange(acked, floor, _earliest_available(conn))
    chosen = _pick_frames(conn, acked, target)
    if not chosen:
        return {"delivery": None, "frames": [], "replayed": False}
    end = int(chosen[-1]["end_seq"])
    delivery_id = "D" + sha256_hex(
        ("cf\x00%s\x00%d\x00%d\x00%d\x00%d" % (
            name, int(consumer_row["generation"]), acked + 1, end,
            time.time_ns())).encode("utf-8"))[:24]
    conn.execute(
        "INSERT INTO cf_deliveries(delivery_id, consumer, generation, token, "
        "from_seq, through_seq, created_at, state) VALUES (?,?,?,?,?,?,?,'pending')",
        (delivery_id, name, int(consumer_row["generation"]), delivery_id,
         acked + 1, end, now_iso()))
    conn.executemany(
        "INSERT INTO cf_delivery_frames(delivery_id, frame_id) VALUES (?,?)",
        [(delivery_id, int(f["frame_id"])) for f in chosen])
    return {"delivery": conn.execute(
        "SELECT * FROM cf_deliveries WHERE delivery_id=?", (delivery_id,)).fetchone(),
        "frames": chosen, "replayed": False}


def _require_live(conn, name: str):
    row = _live_consumer(conn, name)
    if row is not None:
        return row
    if _consumer(conn, name) is None:
        raise UnknownConsumer("unknown consumer %r" % name, consumer=name)
    raise ConsumerDisabled("consumer %r is disabled" % name, consumer=name)


def iter_delivery(store, name: str, target: int = 1000) -> Iterator[str]:
    """Yield protocol lines.  The range/token are committed before the first
    line; a killed sender is replayed identically on the next pull.  Frames
    are streamed through SQLite cursors, one event at a time.

    If nothing new is pending, the read path never opens a write transaction,
    so an in-flight business write (which holds SQLite's RESERVED lock) does
    not turn an empty result into a blocking call: uncommitted notifications
    are simply not visible, exactly like the business rows they belong to.
    """
    if target < 1:
        raise UnsupportedMode("target must be >= 1")
    with cf_lock(store):
        conn = store.connect()
        try:
            consumer = _require_live(conn, name)
            pending = conn.execute(
                "SELECT * FROM cf_deliveries WHERE consumer=? AND state='pending'",
                (name,)).fetchone()
            head = _tail(conn)
            if pending is None and head <= int(consumer["acked_seq"]):
                info = {"delivery": None, "frames": []}
            else:
                conn.execute("BEGIN IMMEDIATE")
                consumer = _require_live(conn, name)
                info = _materialize(conn, consumer, target)
                head = _tail(conn)
                conn.execute("COMMIT")
        except Exception:
            _rollback(conn)
            raise
        finally:
            conn.close()

    d, frames = info["delivery"], info["frames"]
    if d is None or not frames:
        yield "START empty=1 frames=0 head=%d\n" % head
        yield "END events=0 frames=0 empty=1\n"
        return
    yield ("START delivery=%s from=%d through=%d target=%d frames=%d head=%d "
           "events=%d empty=0 replay=%d\n"
           % (d["delivery_id"], d["from_seq"], d["through_seq"], target,
              len(frames), head,
              sum(int(f["event_count"]) for f in frames),
              1 if info["replayed"] else 0))
    emitted = 0
    conn2 = store.connect()
    try:
        for idx, f in enumerate(frames, 1):
            fid = int(f["frame_id"])
            yield ("FRAME id=%d index=%d of=%d ts=%s start=%d end=%d events=%d\n"
                   % (fid, idx, len(frames), f["frame_ts"], f["start_seq"],
                      f["end_seq"], f["event_count"]))
            n = 0
            first = last = None
            cur = conn2.execute(
                "SELECT seq, ordinal, op, study, observation FROM cf_events "
                "WHERE frame_id=? ORDER BY ordinal", (fid,))
            for ev in cur:
                yield ("EVENT seq=%d frame=%d ordinal=%d op=%s study=%s observation=%s\n"
                       % (ev["seq"], fid, ev["ordinal"], ev["op"],
                          ev["study"], ev["observation"]))
                n += 1
                emitted += 1
                if first is None:
                    first = ev["seq"]
                last = ev["seq"]
                cf_sync("feed.delivery.event.%d" % emitted)
            cf_sync("feed.delivery.during_frame")
            yield ("ENDFRAME id=%d events=%d first=%s last=%s\n"
                   % (fid, n, first, last))
    finally:
        conn2.close()
    # If the iterator is abandoned here (client disconnected / killed) the
    # pending delivery stays; the next pull resumes with the same token.
    cf_sync("feed.delivery.after_end")
    yield ("END events=%d frames=%d empty=0\n" % (emitted, len(frames)))


def preview(store, name: str, target: int = 1000) -> Iterator[str]:
    """Human/backlog view of the next delivery; creates no delivery and
    carries no ack-able token."""
    conn = store.connect()
    try:
        row = _require_live(conn, name)
        pending = conn.execute(
            "SELECT * FROM cf_deliveries WHERE consumer=? AND state='pending'",
            (name,)).fetchone()
        if pending is not None:
            frames = _delivery_frames(conn, pending["delivery_id"])
            total = sum(int(f["event_count"]) for f in frames)
            yield ("START preview=1 delivery=%s from=%d through=%d target=%d "
                   "frames=%d head=%d empty=0\n"
                   % (pending["delivery_id"], pending["from_seq"],
                      pending["through_seq"], target, len(frames), _tail(conn)))
            yield ("END events=%d frames=%d empty=0 preview=1\n" % (total, len(frames)))
            return
        acked, head = int(row["acked_seq"]), _tail(conn)
        if head <= acked:
            yield ("START preview=1 empty=1 frames=0 head=%d\n" % head)
            yield "END events=0 frames=0 empty=1 preview=1\n"
            return
        floor = _meta(conn, "retention_floor")
        if acked < floor:
            raise StartOutOfRange(acked, floor, _earliest_available(conn))
        chosen = _pick_frames(conn, acked, target)
        total = sum(int(f["event_count"]) for f in chosen)
        yield ("START preview=1 from=%d through=%d target=%d frames=%d head=%d empty=%d\n"
               % (acked + 1, chosen[-1]["end_seq"] if chosen else acked, target,
                  len(chosen), head, 0 if chosen else 1))
        yield ("END events=%d frames=%d empty=%d preview=1\n"
               % (total, len(chosen), 0 if chosen else 1))
    finally:
        conn.close()


# --------------------------------------------------------------------------
# acknowledgement
# --------------------------------------------------------------------------
def ack(store, name: str, token: str) -> dict:
    """Explicitly acknowledge a delivery token.  Idempotent for the same
    token; never lets a foreign or stale token advance the position."""
    if not isinstance(token, str) or not token:
        raise AckRejected("a delivery token is required")
    with cf_lock(store):
        conn = store.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = _require_live(conn, name)
            d = conn.execute(
                "SELECT * FROM cf_deliveries WHERE consumer=? AND delivery_id=?",
                (name, token)).fetchone()
            if d is None:
                # Not this consumer's token (foreign token, typo, or a
                # delivery revoked by disable).
                if conn.execute("SELECT 1 FROM cf_deliveries WHERE delivery_id=?",
                                (token,)).fetchone() is not None:
                    raise AckRejected("token belongs to another consumer",
                                      consumer=name)
                raise AckRejected("unknown token for this consumer", consumer=name)
            if int(d["generation"]) != int(row["generation"]):
                raise AckRejected("token belongs to an earlier consumer lifecycle",
                                  consumer=name)
            if d["state"] == "acked":
                conn.execute("COMMIT")
                return {"consumer": name, "token": token,
                        "acked_seq": int(row["acked_seq"]), "replayed": True}
            if d["state"] != "pending":
                raise AckRejected("token is not the current pending delivery",
                                  consumer=name)
            pending = conn.execute(
                "SELECT delivery_id FROM cf_deliveries WHERE consumer=? AND state='pending'",
                (name,)).fetchone()
            if pending is None or pending["delivery_id"] != token:
                raise AckRejected("token is not the current pending delivery",
                                  consumer=name)
            new_ack = max(int(row["acked_seq"]), int(d["through_seq"]))
            conn.execute("UPDATE cf_consumers SET acked_seq=? WHERE name=?",
                         (new_ack, name))
            conn.execute("UPDATE cf_deliveries SET state='acked', acked_at=? "
                         "WHERE delivery_id=?", (now_iso(), token))
            conn.execute("COMMIT")
            cf_sync("feed.after_ack")
            return {"consumer": name, "token": token, "acked_seq": new_ack,
                    "replayed": False}
        except Exception:
            _rollback(conn)
            raise
        finally:
            conn.close()


# --------------------------------------------------------------------------
# retention
# --------------------------------------------------------------------------
def _trim_body(conn, through: Optional[int]) -> dict:
    live = conn.execute(
        "SELECT name, acked_seq FROM cf_consumers WHERE disabled_at IS NULL "
        "ORDER BY name").fetchall()
    floor, head = _meta(conn, "retention_floor"), _tail(conn)
    if through is not None and through > head:
        raise TrimError("through position %d is beyond the current tail %d"
                        % (through, head))
    if live:
        limit = min(int(r["acked_seq"]) for r in live)
        effective = limit if through is None else min(limit, through)
    else:
        if through is None:
            raise TrimError("no enabled consumers: refusing to delete with an "
                            "unbounded range; pass --through-seq for an explicit "
                            "upper bound")
        effective = through
    if effective <= floor:
        return {"trimmed_events": 0, "trimmed_frames": 0, "retention_floor": floor,
                "earliest_available_seq": _earliest_available(conn),
                "tail_seq": head, "trimmed_through": floor,
                "blocked_by": [r["name"] for r in live
                               if int(r["acked_seq"]) < floor]}
    # Only complete frames entirely at or below the boundary.
    cut = conn.execute(
        "SELECT frame_id, start_seq, end_seq, event_count FROM cf_frames "
        "WHERE end_seq <= ? ORDER BY start_seq", (effective,)).fetchall()
    if cut:
        new_floor = int(cut[-1]["end_seq"])
        cf_sync("feed.trim.before_delete")
        ids = [int(r["frame_id"]) for r in cut]
        conn.executemany("DELETE FROM cf_events WHERE frame_id=?", [(i,) for i in ids])
        conn.executemany("DELETE FROM cf_frames WHERE frame_id=?", [(i,) for i in ids])
        conn.executemany("DELETE FROM cf_delivery_frames WHERE frame_id=?",
                         [(i,) for i in ids])
        _set_meta(conn, "retention_floor", new_floor)
        return {"trimmed_events": sum(int(r["event_count"]) for r in cut),
                "trimmed_frames": len(ids), "retention_floor": new_floor,
                "earliest_available_seq": _earliest_available(conn),
                "tail_seq": head, "trimmed_through": new_floor,
                "trimmed_frame_ids": ids,
                "blocked_by": [r["name"] for r in live
                               if int(r["acked_seq"]) < new_floor]}
    return {"trimmed_events": 0, "trimmed_frames": 0, "retention_floor": floor,
            "earliest_available_seq": _earliest_available(conn),
            "tail_seq": head, "trimmed_through": floor,
            "blocked_by": [r["name"] for r in live if int(r["acked_seq"]) < floor]}


def trim(store, through: Optional[int] = None) -> dict:
    """Delete only complete frames every enabled consumer has acknowledged.
    The floor update, the deletes and the decision are one transaction."""
    with cf_lock(store):
        conn = store.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            result = _trim_body(conn, through)
            conn.execute("COMMIT")
            cf_sync("feed.trim.after_commit")
            return result
        except Exception:
            _rollback(conn)
            raise
        finally:
            conn.close()


def status(store, name: Optional[str] = None) -> dict:
    """Persistent backlog view: per consumer position/lag plus retention."""
    conn = store.connect()
    try:
        base = {"retention_floor": _meta(conn, "retention_floor"),
                "earliest_available_seq": _earliest_available(conn),
                "tail_seq": _tail(conn),
                "enabled_consumers": [], "disabled_consumers": []}
        for r in conn.execute("SELECT * FROM cf_consumers ORDER BY name"):
            entry = {"name": r["name"], "generation": int(r["generation"]),
                     "start_seq": int(r["start_seq"]),
                     "acked_seq": int(r["acked_seq"]),
                     "enabled": r["disabled_at"] is None,
                     "backlog_events": max(0, _tail(conn) - int(r["acked_seq"]))}
            pending = conn.execute(
                "SELECT delivery_id, from_seq, through_seq FROM cf_deliveries "
                "WHERE consumer=? AND state='pending'", (r["name"],)).fetchone()
            if pending is not None:
                entry["pending_delivery"] = {
                    "token": pending["delivery_id"],
                    "from_seq": int(pending["from_seq"]),
                    "through_seq": int(pending["through_seq"])}
            (base["enabled_consumers"] if entry["enabled"]
             else base["disabled_consumers"]).append(entry)
        if name is not None:
            row = _consumer(conn, name)
            if row is None:
                raise UnknownConsumer("unknown consumer %r" % name, consumer=name)
            base["consumer"] = next(
                e for group in (base["enabled_consumers"], base["disabled_consumers"])
                for e in group if e["name"] == name)
        base["frames_retained"] = int(conn.execute(
            "SELECT COUNT(*) AS c FROM cf_frames").fetchone()["c"])
        base["events_retained"] = int(conn.execute(
            "SELECT COUNT(*) AS c FROM cf_events").fetchone()["c"])
        return base
    finally:
        conn.close()

