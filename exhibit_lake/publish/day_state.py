"""按场馆与本地日期读取水位线、封账状态并维护分区行。"""
from __future__ import annotations

import sqlite3

from exhibit_lake.catalog.store import Store


def partition(conn: sqlite3.Connection, venue_id: str, day: str) -> dict | None:
    return Store.one(conn, "SELECT * FROM partitions WHERE venue_id=? AND local_day=?", (venue_id, day))


def ensure_partition(conn: sqlite3.Connection, venue_id: str, day: str) -> dict:
    part = partition(conn, venue_id, day)
    if part is None:
        conn.execute("INSERT INTO partitions(venue_id,local_day,generation,head_id,closed,closure_version) "
                     "VALUES(?,?,0,NULL,0,0)", (venue_id, day))
        part = partition(conn, venue_id, day)
    return part


def unheld_max_event_at(conn: sqlite3.Connection, venue_id: str, day: str) -> str | None:
    row = Store.one(conn, "SELECT MAX(event_at) AS value FROM accepted_events "
                    "WHERE venue_id=? AND local_day=? AND held_by_freeze=0", (venue_id, day))
    return row["value"]


def recompute_watermark(conn: sqlite3.Connection, venue_id: str, day: str,
                        received_at: str | None = None) -> str | None:
    """Advance the event-time watermark, never moving it backwards.

    The watermark is the greatest event time covered by a publication.  A
    backfilled batch can lower the observed maximum, but a report that was
    already published must keep its cut, so the stored value is monotone.
    """
    part = partition(conn, venue_id, day)
    observed = unheld_max_event_at(conn, venue_id, day)
    watermark = max(filter(None, (part["watermark_event_at"] if part else None, observed)),
                    default=None)
    if received_at is None:
        conn.execute("UPDATE partitions SET watermark_event_at=? WHERE venue_id=? AND local_day=?",
                     (watermark, venue_id, day))
    else:
        conn.execute("UPDATE partitions SET watermark_event_at=?,watermark_received_at=? "
                     "WHERE venue_id=? AND local_day=?", (watermark, received_at, venue_id, day))
    return watermark


def held_events(conn: sqlite3.Connection, venue_id: str, day: str) -> int:
    return Store.one(conn, "SELECT COUNT(*) AS n FROM accepted_events "
                     "WHERE venue_id=? AND local_day=? AND held_by_freeze=1", (venue_id, day))["n"]


def latest_closure(conn: sqlite3.Connection, venue_id: str, day: str) -> dict | None:
    return Store.one(conn, "SELECT * FROM day_closures WHERE venue_id=? AND local_day=? "
                      "ORDER BY version DESC LIMIT 1", (venue_id, day))


def read_state(conn: sqlite3.Connection, venue_id: str, day: str) -> dict:
    """Return the consistency-critical closure/watermark view of one partition."""
    part = partition(conn, venue_id, day)
    latest = latest_closure(conn, venue_id, day)
    if latest is not None:
        latest = {
            "version": latest["version"], "status": latest["status"],
            "snapshot_id": latest["snapshot_id"], "generation": latest["generation"],
            "closed_at": latest["closed_at"], "closed_by": latest["closed_by"],
            "close_reason": latest["close_reason"], "reopened_at": latest["reopened_at"],
            "reopened_by": latest["reopened_by"], "approved_by": latest["approved_by"],
            "reopen_reason": latest["reopen_reason"],
        }
    return {
        "venue_id": venue_id,
        "local_day": day,
        "closed": bool(part["closed"]) if part else False,
        "closure_version": part["closure_version"] if part else 0,
        "generation": part["generation"] if part else 0,
        "head_id": part["head_id"] if part else None,
        "watermark_event_at": part["watermark_event_at"] if part else None,
        "watermark_received_at": part["watermark_received_at"] if part else None,
        "held_events": held_events(conn, venue_id, day),
        "latest_closure": latest,
    }
