"""使用合成事件的离线端到端演示。"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

from exhibit_lake.service import ExhibitLake


def main() -> None:
    with tempfile.TemporaryDirectory() as folder:
        lake = ExhibitLake(Path(folder) / "demo.db")
        lake.register_venue("showroom", "Synthetic Innovation Showroom", "Asia/Shanghai")
        lake.register_exhibit("robot", "showroom", "Synthetic Robot", "Sample Maker", "2026-01-01")
        lake.register_source("counter", "showroom", "visit")
        lake.register_source("kiosk", "showroom", "feedback")
        lake.register_contract("count-v1", "counter", ["event_id", "exhibit_id", "event_at", "count"],
                               "2026-01-01T00:00:00+00:00")
        lake.register_contract("rating-v1", "kiosk", ["event_id", "exhibit_id", "event_at", "rating", "visitor_id"],
                               "2026-01-01T00:00:00+00:00")
        lake.ingest("counter", "count-v1", "batch-1", [{"event_id": "v1", "exhibit_id": "robot",
                   "event_at": "2026-03-01T10:00:00+08:00", "count": 12}])
        lake.ingest("kiosk", "rating-v1", "batch-2", [{"event_id": "f1", "exhibit_id": "robot",
                   "event_at": "2026-03-01T10:30:00+08:00", "rating": 5, "visitor_id": "visitor-1"}])
        publication = lake.publish_day("showroom", "2026-03-01")
        result = lake.analytics.day("showroom", "2026-03-01")
        assert result["total_visitors"] == 12 and result["total_feedback"] == 1
        assert lake.snapshots.verify(publication["snapshot_id"])["valid"]

        closure = lake.close_day("showroom", "2026-03-01", closed_by="demo-ops",
                                 reason="daily-report-delivered", request_key="demo-close")
        late = lake.ingest("counter", "count-v1", "batch-3", [{"event_id": "v2", "exhibit_id": "robot",
                            "event_at": "2026-03-01T18:00:00+08:00", "count": 4}])
        assert late["held_rows"] == 1
        frozen = lake.analytics.day("showroom", "2026-03-01")
        assert frozen["snapshot_id"] == publication["snapshot_id"] and frozen["total_visitors"] == 12

        reopened = lake.reopen_day("showroom", "2026-03-01", approved_by="venue-lead",
                                   reason="late-device-batch", request_key="demo-reopen")
        backfill = lake.publish_day("showroom", "2026-03-01", publication["snapshot_id"])
        assert backfill["generation"] == 2
        assert lake.analytics.day("showroom", "2026-03-01")["total_visitors"] == 16
        assert lake.analytics.day("showroom", "2026-03-01", publication["snapshot_id"])["total_visitors"] == 12
        print(json.dumps({"publication": publication["snapshot_id"],
                          "closure_version": reopened["closure_version"],
                          "backfill": backfill["snapshot_id"],
                          "frozen_report": frozen, "report": lake.analytics.day("showroom", "2026-03-01")},
                         ensure_ascii=False))


if __name__ == "__main__":
    main()
