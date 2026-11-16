"""事件批次的不可变原始落地和幂等接收。"""
from __future__ import annotations

import json
import sqlite3
import uuid

from exhibit_lake.catalog.registry import now, require_id
from exhibit_lake.catalog.store import Store
from exhibit_lake.errors import Conflict, InvalidInput, NotFound
from exhibit_lake.ingest.validation import canonical, digest, event
from exhibit_lake.publish.day_state import partition as day_partition


class Batches:
    def __init__(self, store: Store):
        self.store = store

    def ingest(self, source_id: str, contract_id: str, request_key: str, rows: list[object]) -> dict:
        require_id(source_id, "source_id")
        require_id(contract_id, "contract_id")
        require_id(request_key, "request_key")
        if not isinstance(rows, list) or not 1 <= len(rows) <= 5000:
            raise InvalidInput("batch must contain between 1 and 5000 events")
        try:
            payload_hash = digest(rows)
        except (TypeError, ValueError):
            raise InvalidInput("batch must contain JSON-compatible finite values") from None
        with self.store.transaction() as conn:
            previous = Store.one(conn, "SELECT * FROM batches WHERE source_id=? AND request_key=?", (source_id, request_key))
            if previous:
                if previous["payload_hash"] != payload_hash or previous["contract_id"] != contract_id:
                    raise Conflict("request key already used for different content")
                return self._report(conn, previous["batch_id"], replay=True)
            source = Store.one(conn, "SELECT * FROM sources WHERE source_id=?", (source_id,))
            if not source:
                raise NotFound("source not found")
            if not source["active"]:
                raise Conflict("source inactive")
            contract = Store.one(conn, "SELECT * FROM contracts WHERE contract_id=? AND source_id=?", (contract_id, source_id))
            if not contract:
                raise NotFound("contract not found for source")
            venue = Store.one(conn, "SELECT * FROM venues WHERE venue_id=?", (source["venue_id"],))
            exhibits = {x["exhibit_id"]: x for x in Store.many(conn, "SELECT * FROM exhibits WHERE venue_id=?", (source["venue_id"],))}
            batch_id = uuid.uuid4().hex
            received_at = now()
            conn.execute("INSERT INTO batches(batch_id,source_id,request_key,payload_hash,contract_id,status,"
                         "received_at,committed_at,total_rows,valid_rows,rejected_rows,held_rows) "
                         "VALUES(?,?,?,?,?,?,?,?,?,?,?,0)",
                         (batch_id, source_id, request_key, payload_hash, contract_id, "landing",
                          received_at, None, len(rows), 0, 0))
            valid = 0
            rejected = 0
            held = 0
            for row_no, row in enumerate(rows, 1):
                body = canonical(row)
                row_hash = digest(row)
                conn.execute("INSERT INTO raw_rows VALUES(?,?,?,?)", (batch_id, row_no, body, row_hash))
                normalized, problem = event(row, contract, source, venue, exhibits)
                if normalized is not None:
                    existing = Store.one(conn, "SELECT payload_hash FROM accepted_events WHERE source_id=? AND event_id=?",
                                         (source_id, normalized["event_id"]))
                    if existing:
                        problem = {"code": "duplicate_event" if existing["payload_hash"] == row_hash else "event_conflict",
                                   "detail": "event identity was already accepted"}
                    else:
                        held_count = self._insert_event(conn, batch_id, row_no, normalized, received_at)
                        held += held_count
                        valid += 1
                if problem is not None:
                    conn.execute("INSERT INTO rejected_rows VALUES(?,?,?,?)",
                                 (batch_id, row_no, problem["code"], problem["detail"]))
                    rejected += 1
            conn.execute("UPDATE batches SET status='committed',committed_at=?,valid_rows=?,rejected_rows=?,"
                         "held_rows=? WHERE batch_id=?",
                         (now(), valid, rejected, held, batch_id))
            if held:
                Store.audit(conn, "events.held_after_close", batch_id,
                            {"source_id": source_id, "events": held}, now())
            Store.audit(conn, "batch.committed", batch_id,
                        {"source_id": source_id, "valid": valid, "rejected": rejected,
                         "held": held}, now())
            return self._report(conn, batch_id, replay=False)

    def _insert_event(self, conn: sqlite3.Connection, batch_id: str, row_no: int,
                      normalized: dict, received_at: str) -> int:
        """Land one accepted event, holding it when its day is already closed.

        Raw rows are always kept; the hold flags keep post-close events out of
        automatic staging until the day is explicitly reopened and a new
        version is published.
        """
        part = day_partition(conn, normalized["venue_id"], normalized["local_day"])
        if part is None:
            conn.execute("INSERT INTO partitions(venue_id,local_day,generation,head_id,closed,closure_version) "
                         "VALUES(?,?,0,NULL,0,0)", (normalized["venue_id"], normalized["local_day"]))
            part = day_partition(conn, normalized["venue_id"], normalized["local_day"])
        closed = bool(part["closed"])
        behind_watermark = bool(part["watermark_event_at"]
                                and normalized["event_at"] <= part["watermark_event_at"])
        conn.execute("INSERT INTO accepted_events(event_id,source_id,batch_id,row_no,venue_id,exhibit_id,"
                     "event_at,local_day,kind,value_num,category,visitor_hash,payload_hash,published_at,"
                     "late,held_by_freeze,held_at,held_closure_version,held_reason) "
                     "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,NULL,?,?,?,?,?)",
                     (normalized["event_id"], normalized["source_id"], batch_id, row_no,
                      normalized["venue_id"], normalized["exhibit_id"], normalized["event_at"],
                      normalized["local_day"], normalized["kind"], normalized["value_num"],
                      normalized["category"], normalized["visitor_hash"], normalized["payload_hash"],
                      int(behind_watermark), int(closed),
                      received_at if closed else None,
                      part["closure_version"] if closed else None,
                      "day_closed" if closed else None))
        return int(closed)

    def _report(self, conn: sqlite3.Connection, batch_id: str, *, replay: bool) -> dict:
        batch = Store.one(conn, "SELECT * FROM batches WHERE batch_id=?", (batch_id,))
        rejected = Store.many(conn, "SELECT row_no,code,detail FROM rejected_rows WHERE batch_id=? ORDER BY row_no", (batch_id,))
        return {"batch_id": batch_id, "source_id": batch["source_id"], "request_key": batch["request_key"],
                "status": batch["status"], "total_rows": batch["total_rows"], "valid_rows": batch["valid_rows"],
                "rejected_rows": batch["rejected_rows"], "held_rows": batch["held_rows"],
                "rejections": rejected, "replay": replay}

    def report(self, batch_id: str) -> dict:
        with self.store.transaction(write=False) as conn:
            if not Store.one(conn, "SELECT batch_id FROM batches WHERE batch_id=?", (batch_id,)):
                raise NotFound("batch not found")
            return self._report(conn, batch_id, replay=False)

    def raw(self, batch_id: str, *, offset: int = 0, limit: int = 100) -> dict:
        if offset < 0 or not 1 <= limit <= 500:
            raise InvalidInput("invalid raw row page")
        with self.store.transaction(write=False) as conn:
            if not Store.one(conn, "SELECT batch_id FROM batches WHERE batch_id=?", (batch_id,)):
                raise NotFound("batch not found")
            rows = Store.many(conn, "SELECT row_no,payload_json,payload_hash FROM raw_rows WHERE batch_id=? ORDER BY row_no LIMIT ? OFFSET ?",
                              (batch_id, limit, offset))
        return {"batch_id": batch_id, "rows": [{"row_no": x["row_no"], "payload": json.loads(x["payload_json"]),
                                                 "payload_hash": x["payload_hash"]} for x in rows]}
