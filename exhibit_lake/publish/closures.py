"""按场馆与本地日期维护水位线、封账、获准重开及封账后事件的留证。"""
from __future__ import annotations

import sqlite3

from exhibit_lake.catalog.registry import iso_day, now, require_id
from exhibit_lake.catalog.store import Store
from exhibit_lake.errors import Conflict, InvalidInput, NotFound, StateError
from exhibit_lake.publish import day_state
from exhibit_lake.publish.snapshots import Snapshots


REOPEN_REQUIRED = "approved_by and reason are required to reopen a closed day"
CLOSE_REQUIRED = "closed_by and reason are required to close a day"


def require_text(value: str, label: str) -> str:
    if not isinstance(value, str) or not 1 <= len(value.strip()) <= 200:
        raise InvalidInput(f"{label} must be a nonempty string of at most 200 characters")
    return value.strip()


class Closures:
    def __init__(self, store: Store, snapshots: Snapshots):
        self.store = store
        self.snapshots = snapshots

    # ------------------------------------------------------------------ read
    def state(self, venue_id: str, day: str, *, offset: int = 0, limit: int = 100) -> dict:
        iso_day(day, "local_day")
        if offset < 0 or not 1 <= limit <= 500:
            raise InvalidInput("invalid held-event page")
        with self.store.transaction(write=False) as conn:
            if not Store.one(conn, "SELECT venue_id FROM venues WHERE venue_id=?", (venue_id,)):
                raise NotFound("venue not found")
            result = day_state.read_state(conn, venue_id, day)
            result["held"] = self._held(conn, venue_id, day, offset=offset, limit=limit)
            result["history"] = self._history(conn, venue_id, day)
            return result

    def _held(self, conn: sqlite3.Connection, venue_id: str, day: str,
              *, offset: int = 0, limit: int = 100) -> dict:
        total = day_state.held_events(conn, venue_id, day)
        rows = Store.many(conn, "SELECT e.event_id,e.source_id,e.batch_id,e.row_no,e.exhibit_id,e.event_at,"
                          "e.held_at,e.held_closure_version,e.held_reason,e.released_at,e.late,"
                          "e.published_at,b.received_at,b.request_key "
                          "FROM accepted_events e JOIN batches b ON b.batch_id=e.batch_id "
                          "WHERE e.venue_id=? AND e.local_day=? AND e.held_by_freeze=1 "
                          "ORDER BY b.received_at,e.source_id,e.event_id LIMIT ? OFFSET ?",
                          (venue_id, day, limit, offset))
        return {"total": total, "offset": offset, "limit": limit, "events": rows}

    @staticmethod
    def _history(conn: sqlite3.Connection, venue_id: str, day: str) -> list[dict]:
        return Store.many(conn, "SELECT version,status,snapshot_id,generation,watermark_event_at,"
                          "watermark_received_at,closed_at,closed_by,close_reason,close_request_key,"
                          "reopened_at,reopened_by,approved_by,reopen_reason,reopen_request_key,"
                          "released_events FROM day_closures WHERE venue_id=? AND local_day=? "
                          "ORDER BY version", (venue_id, day))

    # ----------------------------------------------------------------- close
    def close(self, venue_id: str, day: str, *, snapshot_id: str | None = None,
              expected_head: str | None = None, expected_closure_version: int | None = None,
              closed_by: str | None = None, reason: str | None = None,
              request_key: str | None = None) -> dict:
        """Freeze the day on its current published head.

        The closure stores the event-time watermark that the closed report
        covers, so later arrivals can be classified against a fixed cut.
        """
        iso_day(day, "local_day")
        if not closed_by or not reason:
            raise InvalidInput(CLOSE_REQUIRED)
        require_id(closed_by, "closed_by")
        require_text(reason, "reason")
        if request_key is not None:
            require_id(request_key, "request_key")
        with self.store.transaction() as conn:
            if not Store.one(conn, "SELECT venue_id FROM venues WHERE venue_id=?", (venue_id,)):
                raise NotFound("venue not found")
            part = day_state.partition(conn, venue_id, day)
            if part is None:
                raise InvalidInput("day has no published report to close")
            replay = self._replay(conn, venue_id, day, request_key, "close")
            if replay is not None:
                return replay
            if snapshot_id is None:
                snapshot_id = part["head_id"]
            if snapshot_id is None:
                raise InvalidInput("snapshot_id of the day's published report is required")
            snapshot = Store.one(conn, "SELECT * FROM snapshots WHERE snapshot_id=?", (snapshot_id,))
            if not snapshot or snapshot["venue_id"] != venue_id or snapshot["local_day"] != day:
                raise NotFound("snapshot not found in requested partition")
            if snapshot["status"] != "published" or part["head_id"] != snapshot_id:
                raise StateError("only the partition's published head can be closed")
            if part["closed"]:
                raise StateError("day is already closed",
                                 details={"closure_version": part["closure_version"],
                                          "snapshot_id": part["head_id"]})
            self._expect(part, expected_head, expected_closure_version)
            version = part["closure_version"] + 1
            stamp = now()
            # A frozen day must have exactly one interpretable version: drop
            # pending candidates (keeping their manifests and audit records)
            # so a reopen starts from a clean staging slate.
            staged = Store.many(conn, "SELECT snapshot_id FROM snapshots WHERE venue_id=? "
                                "AND local_day=? AND status='staged' ORDER BY generation",
                                (venue_id, day))
            for candidate in staged:
                conn.execute("UPDATE snapshots SET status='abandoned' WHERE snapshot_id=?",
                             (candidate["snapshot_id"],))
                Store.audit(conn, "snapshot.abandoned_at_close", candidate["snapshot_id"],
                            {"venue_id": venue_id, "local_day": day}, now())
            # Freeze the published version.  Accepted-but-unpublished events
            # are reception evidence from here on: they are held rather than
            # silently absorbed into the closed number.
            pending = self._pending(conn, venue_id, day, snapshot_id)
            for item in pending:
                conn.execute("UPDATE accepted_events SET held_by_freeze=1,held_at=?,held_reason=?,"
                             "held_closure_version=? WHERE source_id=? AND event_id=? "
                             "AND held_by_freeze=0",
                             (stamp, "unpublished_at_close", version,
                              item["source_id"], item["event_id"]))
            watermark = day_state.recompute_watermark(conn, venue_id, day)
            updated = conn.execute("UPDATE partitions SET closed=1,closure_version=? "
                                   "WHERE venue_id=? AND local_day=? AND closed=0 AND head_id IS ?",
                                   (version, venue_id, day, snapshot_id))
            if updated.rowcount != 1:
                raise Conflict("concurrent closure update")
            conn.execute("INSERT INTO day_closures(venue_id,local_day,version,status,snapshot_id,generation,"
                         "watermark_event_at,watermark_received_at,closed_at,closed_by,close_reason,"
                         "close_request_key,released_events) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,0)",
                         (venue_id, day, version, "closed", snapshot_id, snapshot["generation"],
                          watermark, stamp, stamp, closed_by, reason, request_key))
            Store.audit(conn, "day.closed", f"{venue_id}/{day}",
                        {"version": version, "snapshot_id": snapshot_id,
                         "generation": snapshot["generation"], "watermark_event_at": watermark,
                         "held_unpublished": len(pending),
                         "abandoned_candidates": [x["snapshot_id"] for x in staged],
                         "closed_by": closed_by, "reason": reason, "request_key": request_key}, stamp)
            return self._describe(conn, venue_id, day, version, latest_version=version)

    # ---------------------------------------------------------------- reopen
    def reopen(self, venue_id: str, day: str, *, approved_by: str,
               reason: str, expected_head: str | None = None,
               expected_closure_version: int | None = None,
               request_key: str | None = None) -> dict:
        """Reopen a closed day under explicit approval.

        Events held while the day was closed keep their reception evidence
        (``held_at``, originating batch and raw rows) and are released into an
        explicit backfill generation instead of merging into the closed one.
        """
        iso_day(day, "local_day")
        if not approved_by or not reason:
            raise InvalidInput(REOPEN_REQUIRED)
        require_id(approved_by, "approved_by")
        require_text(reason, "reason")
        if request_key is not None:
            require_id(request_key, "request_key")
        with self.store.transaction() as conn:
            if not Store.one(conn, "SELECT venue_id FROM venues WHERE venue_id=?", (venue_id,)):
                raise NotFound("venue not found")
            part = day_state.partition(conn, venue_id, day)
            if part is None:
                raise StateError("day is not closed")
            replay = self._replay(conn, venue_id, day, request_key, "reopen")
            if replay is not None:
                return replay
            if not part["closed"]:
                raise StateError("day is not closed")
            self._expect(part, expected_head, expected_closure_version)
            latest = day_state.latest_closure(conn, venue_id, day)
            if latest is None or latest["status"] != "closed":
                raise StateError("current closure record is not open")
            version = part["closure_version"] + 1
            stamp = now()
            released = Store.many(conn, "SELECT source_id,event_id FROM accepted_events "
                                  "WHERE venue_id=? AND local_day=? AND held_by_freeze=1 "
                                  "ORDER BY source_id,event_id", (venue_id, day))
            updated = conn.execute("UPDATE partitions SET closed=0,closure_version=? "
                                   "WHERE venue_id=? AND local_day=? AND closed=1 AND head_id IS ?",
                                   (version, venue_id, day, part["head_id"]))
            if updated.rowcount != 1:
                raise Conflict("concurrent reopen update")
            for item in released:
                conn.execute("UPDATE accepted_events SET held_by_freeze=0,held_reason=NULL,released_at=? "
                             "WHERE source_id=? AND event_id=? AND held_by_freeze=1",
                             (stamp, item["source_id"], item["event_id"]))
            conn.execute("UPDATE day_closures SET status='reopened',reopened_at=?,reopened_by=?,"
                         "approved_by=?,reopen_reason=?,reopen_request_key=?,released_events=? "
                         "WHERE venue_id=? AND local_day=? AND version=?",
                         (stamp, approved_by, approved_by, reason, request_key, len(released),
                          venue_id, day, latest["version"]))
            Store.audit(conn, "day.reopened", f"{venue_id}/{day}",
                        {"version": version, "previous_version": latest["version"],
                         "approved_by": approved_by, "reason": reason,
                         "closed_snapshot_id": latest["snapshot_id"],
                         "released_events": len(released), "request_key": request_key}, stamp)
            if released:
                Store.audit(conn, "events.released_after_reopen", f"{venue_id}/{day}",
                            {"closure_version": latest["version"], "events": len(released),
                             "source_ids": sorted({x["source_id"] for x in released})}, stamp)
            return self._describe(conn, venue_id, day, version)

    # --------------------------------------------------------------- helpers
    @staticmethod
    def _pending(conn: sqlite3.Connection, venue_id: str, day: str, snapshot_id: str) -> list[dict]:
        return Store.many(
            conn, "SELECT e.source_id,e.event_id FROM accepted_events e "
            "WHERE e.venue_id=? AND e.local_day=? AND e.held_by_freeze=0 AND NOT EXISTS("
            "SELECT 1 FROM snapshot_members m WHERE m.snapshot_id=? AND m.source_id=e.source_id "
            "AND m.event_id=e.event_id) ORDER BY e.source_id,e.event_id",
            (venue_id, day, snapshot_id))

    @staticmethod
    def _expect(part: dict, expected_head: str | None, expected_closure_version: int | None) -> None:
        if expected_head is not None and part["head_id"] != expected_head:
            raise Conflict("expected head differs from current partition",
                           details={"head_id": part["head_id"]})
        if expected_closure_version is not None and part["closure_version"] != expected_closure_version:
            raise Conflict("closure state changed since expected",
                           details={"closure_version": part["closure_version"],
                                    "closed": bool(part["closed"])})

    def _replay(self, conn: sqlite3.Connection, venue_id: str, day: str,
                request_key: str | None, action: str) -> dict | None:
        """Return the prior result of the same idempotent transition.

        A request key is scoped to one transition on one partition: keys are
        never reused across close and reopen, and never across days.
        """
        if request_key is None:
            return None
        column = "close_request_key" if action == "close" else "reopen_request_key"
        previous = Store.one(conn, f"SELECT * FROM day_closures WHERE venue_id=? AND local_day=? "
                              f"AND {column}=? ORDER BY version DESC LIMIT 1",
                              (venue_id, day, request_key))
        if previous is None:
            other = Store.one(conn, "SELECT version FROM day_closures WHERE venue_id=? AND local_day=? "
                              "AND (close_request_key=? OR reopen_request_key=?) LIMIT 1",
                              (venue_id, day, request_key, request_key))
            if other is not None:
                raise Conflict("request key was used for a different closure transition")
            return None
        state = "closed" if action == "close" else "reopened"
        if previous["status"] != state:
            raise Conflict("request key was used for a different closure transition")
        part = day_state.partition(conn, venue_id, day)
        # A close leaves the partition at the record's version, a reopen one
        # version past it.  Anything further along means the key belongs to a
        # superseded transition and must not mask a newer state.
        expected_version = previous["version"] + (0 if action == "close" else 1)
        if part["closure_version"] != expected_version or bool(part["closed"]) != (action == "close"):
            raise Conflict("request key was used for an earlier closure version",
                           details={"request_version": previous["version"],
                                    "closure_version": part["closure_version"],
                                    "closed": bool(part["closed"])})
        return self._describe(conn, venue_id, day, part["closure_version"],
                              latest_version=previous["version"], replay=True)

    def _describe(self, conn: sqlite3.Connection, venue_id: str, day: str, version: int,
                  *, latest_version: int | None = None, replay: bool = False) -> dict:
        """Describe the partition after a transition.

        ``version`` is the partition's new closure version; the transition
        record itself usually still carries the previous version because a
        reopen closes out the record it reopens.
        """
        result = day_state.read_state(conn, venue_id, day)
        record = Store.one(conn, "SELECT * FROM day_closures WHERE venue_id=? AND local_day=? AND version=?",
                           (venue_id, day, latest_version if latest_version is not None else version))
        result["version"] = version
        result["transition"] = record["status"] if record else None
        result["record"] = record
        result["history"] = self._history(conn, venue_id, day)
        result["replay"] = replay
        return result
