"""按场馆/日期封账、水位线与获准重开的端到端规则。"""
import tempfile
import threading
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from exhibit_lake.api import create_app
from exhibit_lake.errors import Conflict, InvalidInput, NotFound, StateError
from exhibit_lake.service import ExhibitLake


class ClosureFlowTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.path = Path(self.folder.name) / "lake.db"
        self.lake = ExhibitLake(self.path)
        self.lake.register_venue("venue", "Venue", "Asia/Shanghai")
        self.lake.register_exhibit("robot", "venue", "Robot", "Maker", "2026-01-01")
        self.lake.register_source("counter", "venue", "visit")
        self.lake.register_contract("v1", "counter",
                                    ["event_id", "exhibit_id", "event_at", "count"],
                                    "2026-01-01T00:00:00+00:00")

    def visit(self, identity="a", at="2026-03-01T10:00:00+08:00", count=1):
        return {"event_id": identity, "exhibit_id": "robot", "event_at": at, "count": count}

    def publish(self, head=None):
        return self.lake.publish_day("venue", "2026-03-01", head)

    def closed_day(self):
        self.lake.ingest("counter", "v1", "base", [self.visit("base", count=5)])
        snapshot = self.publish()
        result = self.lake.close_day("venue", "2026-03-01", closed_by="ops-lead",
                                     reason="daily-report-sent", request_key="close-1")
        self.assertEqual(result["closed"], True)
        return snapshot, result

    # --------------------------------------------------------------- closing
    def test_close_freezes_published_head_and_watermark(self):
        snapshot, result = self.closed_day()
        self.assertEqual(result["head_id"], snapshot["snapshot_id"])
        self.assertEqual(result["latest_closure"]["snapshot_id"], snapshot["snapshot_id"])
        self.assertEqual(result["watermark_event_at"], "2026-03-01T02:00:00+00:00")
        state = self.lake.closures.state("venue", "2026-03-01")
        self.assertTrue(state["closed"])
        self.assertEqual(state["history"][0]["closed_by"], "ops-lead")
        with self.assertRaises(StateError):
            self.lake.close_day("venue", "2026-03-01", closed_by="again", reason="again")

    def test_close_requires_a_published_head(self):
        with self.assertRaises(InvalidInput):
            self.lake.close_day("venue", "2026-03-01", closed_by="ops", reason="nothing-to-freeze")

    def test_close_requires_operator_and_reason(self):
        self.lake.ingest("counter", "v1", "base", [self.visit()])
        self.publish()
        with self.assertRaises(InvalidInput):
            self.lake.close_day("venue", "2026-03-01", reason="no-operator")
        with self.assertRaises(InvalidInput):
            self.lake.close_day("venue", "2026-03-01", closed_by="ops", reason="  ")
        self.assertFalse(self.lake.closures.state("venue", "2026-03-01")["closed"])

    def test_close_rejects_stale_expected_head(self):
        self.lake.ingest("counter", "v1", "base", [self.visit()])
        self.publish()
        with self.assertRaises(Conflict):
            self.lake.close_day("venue", "2026-03-01", expected_head="wrong",
                                closed_by="ops", reason="stale")

    def test_close_request_key_is_idempotent(self):
        self.closed_day()
        again = self.lake.close_day("venue", "2026-03-01", closed_by="ops-lead",
                                    reason="retry", request_key="close-1")
        self.assertEqual(again["version"], 1)
        self.assertTrue(again["closed"])

    def test_close_request_key_cannot_be_reused_for_another_transition(self):
        self.closed_day()
        self.lake.reopen_day("venue", "2026-03-01", approved_by="lead",
                             reason="backfill", request_key="reopen-1")
        with self.assertRaises(Conflict):
            self.lake.close_day("venue", "2026-03-01", closed_by="ops",
                                reason="ping", request_key="reopen-1")

    def test_staging_and_publishing_are_rejected_while_closed(self):
        snapshot, _ = self.closed_day()
        with self.assertRaises(StateError):
            self.lake.snapshots.stage("venue", "2026-03-01", snapshot["snapshot_id"])
        self.assertEqual(self.lake.snapshots.head("venue", "2026-03-01")["closed"], 1)

    # -------------------------------------------------------- late ingestion
    def test_late_batch_after_close_is_evidence_but_not_published(self):
        snapshot, _ = self.closed_day()
        batch = self.lake.ingest("counter", "v1", "late", [self.visit("late", count=9)])
        self.assertEqual(batch["valid_rows"], 1)
        self.assertEqual(batch["held_rows"], 1)
        # The frozen report does not move even though the event exists.
        report = self.lake.analytics.day("venue", "2026-03-01")
        self.assertEqual(report["snapshot_id"], snapshot["snapshot_id"])
        self.assertEqual(report["total_visitors"], 5)
        self.assertEqual(report["held_events"], 1)
        # The raw reception evidence is still reachable.
        self.assertEqual(self.lake.batches.raw(batch["batch_id"])["rows"][0]["payload"]["count"], 9)
        provenance = self.lake.provenance.event("counter", "late")
        self.assertEqual(provenance["batch"]["batch_id"], batch["batch_id"])
        self.assertEqual(provenance["snapshots"], [])
        state = self.lake.closures.state("venue", "2026-03-01")
        self.assertEqual(state["held"]["total"], 1)
        self.assertEqual(state["held"]["events"][0]["held_reason"], "day_closed")
        self.assertEqual(state["held"]["events"][0]["held_closure_version"], 1)

    def test_preview_warns_that_rows_will_be_held(self):
        self.closed_day()
        preview = self.lake.preview.inspect("counter", "v1", [self.visit("late", count=9)])
        self.assertEqual(preview["would_accept"], 1)
        self.assertEqual(preview["would_hold"], 1)
        self.assertTrue(preview["rows"][0]["held_after_close"])
        self.assertEqual(self.lake.snapshots.inventory("venue", "2026-03-01")["held_events"], 0)

    def test_held_events_do_not_enter_next_staging_without_reopen(self):
        self.closed_day()
        self.lake.ingest("counter", "v1", "late", [self.visit("late", count=9)])
        with self.assertRaises(StateError):
            self.lake.snapshots.stage("venue", "2026-03-01")

    # ---------------------------------------------------------------- reopen
    def test_reopen_requires_approval_and_reason(self):
        self.closed_day()
        with self.assertRaises(InvalidInput):
            self.lake.reopen_day("venue", "2026-03-01", approved_by="", reason="x")
        with self.assertRaises(InvalidInput):
            self.lake.reopen_day("venue", "2026-03-01", approved_by="lead", reason="")

    def test_reopen_creates_traceable_version_and_backfill_generation(self):
        snapshot, _ = self.closed_day()
        self.lake.ingest("counter", "v1", "late", [self.visit("late", count=9)])
        reopened = self.lake.reopen_day("venue", "2026-03-01", approved_by="venue-lead",
                                        reason="device-batch-arrived", request_key="reopen-1")
        self.assertEqual(reopened["closed"], False)
        self.assertEqual(reopened["closure_version"], 2)
        history = reopened["history"]
        self.assertEqual([x["status"] for x in history], ["reopened"])
        self.assertEqual(history[0]["approved_by"], "venue-lead")
        self.assertEqual(history[0]["released_events"], 1)
        backfill = self.publish(snapshot["snapshot_id"])
        self.assertEqual(backfill["parent_id"], snapshot["snapshot_id"])
        self.assertEqual(backfill["generation"], 2)
        report = self.lake.analytics.day("venue", "2026-03-01")
        self.assertEqual(report["total_visitors"], 14)
        self.assertEqual(report["held_events"], 0)
        # The closed version remains readable and unchanged.
        frozen = self.lake.analytics.day("venue", "2026-03-01", snapshot["snapshot_id"])
        self.assertEqual(frozen["total_visitors"], 5)
        self.assertTrue(frozen["closed"])
        self.assertTrue(frozen["superseded"])
        self.assertEqual(frozen["frozen_version"], 1)
        self.assertEqual(self.lake.snapshots.describe(snapshot["snapshot_id"])["status"], "published")
        actions = [x["action"] for x in self.lake.audit.trail(limit=100)["audits"]]
        self.assertIn("day.closed", actions)
        self.assertIn("day.reopened", actions)
        self.assertIn("events.released_after_reopen", actions)

    def test_reopen_replay_is_idempotent(self):
        self.closed_day()
        first = self.lake.reopen_day("venue", "2026-03-01", approved_by="lead",
                                     reason="backfill", request_key="reopen-1")
        again = self.lake.reopen_day("venue", "2026-03-01", approved_by="lead",
                                     reason="backfill", request_key="reopen-1")
        self.assertEqual(first["version"], again["version"])
        self.assertFalse(again["closed"])

    def test_stale_request_key_does_not_mask_a_newer_closure(self):
        self.closed_day()
        self.lake.reopen_day("venue", "2026-03-01", approved_by="lead",
                             reason="backfill", request_key="reopen-1")
        self.publish(self.lake.snapshots.head("venue", "2026-03-01")["head_id"])
        self.lake.close_day("venue", "2026-03-01", closed_by="ops", reason="again",
                            request_key="close-2")
        with self.assertRaises(Conflict):
            self.lake.reopen_day("venue", "2026-03-01", approved_by="lead",
                                 reason="retry", request_key="reopen-1")
        self.assertTrue(self.lake.closures.state("venue", "2026-03-01")["closed"])

    def test_reopen_then_close_again_increments_version(self):
        self.closed_day()
        self.lake.ingest("counter", "v1", "late", [self.visit("late", count=9)])
        self.lake.reopen_day("venue", "2026-03-01", approved_by="lead", reason="backfill")
        backfill = self.publish(self.lake.snapshots.head("venue", "2026-03-01")["head_id"])
        again = self.lake.close_day("venue", "2026-03-01", closed_by="ops",
                                    reason="corrected-report", request_key="close-2")
        self.assertTrue(again["closed"])
        self.assertEqual(again["closure_version"], 3)
        self.assertEqual(again["head_id"], backfill["snapshot_id"])
        self.assertEqual(len(again["history"]), 2)
        self.assertEqual(again["history"][0]["status"], "reopened")
        self.assertEqual(again["history"][1]["status"], "closed")

    def test_close_abandons_pending_candidates_but_keeps_them_traceable(self):
        self.lake.ingest("counter", "v1", "base", [self.visit("base", count=5)])
        head = self.publish()
        self.lake.ingest("counter", "v1", "more", [self.visit("more", count=2)])
        candidate = self.lake.snapshots.stage("venue", "2026-03-01", head["snapshot_id"])
        self.lake.close_day("venue", "2026-03-01", closed_by="ops", reason="freeze")
        self.assertEqual(self.lake.snapshots.describe(candidate["snapshot_id"])["status"], "abandoned")
        self.assertTrue(self.lake.snapshots.verify(candidate["snapshot_id"])["valid"])
        with self.assertRaises(Conflict):
            self.lake.snapshots.publish(candidate["snapshot_id"], head["snapshot_id"])

    def test_events_arriving_between_reopen_and_publish_join_backfill(self):
        snapshot, _ = self.closed_day()
        self.lake.ingest("counter", "v1", "late", [self.visit("late", count=9)])
        self.lake.reopen_day("venue", "2026-03-01", approved_by="lead", reason="backfill")
        self.lake.ingest("counter", "v1", "later", [self.visit("later", count=2)])
        backfill = self.publish(snapshot["snapshot_id"])
        self.assertEqual(backfill["manifest"]["statistics"]["by_exhibit"]["robot"]["visits"], 16)
        self.assertEqual(self.lake.closures.state("venue", "2026-03-01")["held_events"], 0)

    # ------------------------------------------------------ query coherence
    def test_query_surfaces_never_mix_held_rows_into_published_numbers(self):
        self.closed_day()
        self.lake.ingest("counter", "v1", "late", [self.visit("late", count=9)])
        self.assertEqual(self.lake.coverage.day("venue", "2026-03-01")["held_events"], 1)
        self.assertEqual(self.lake.batch_quality.partition("venue", "2026-03-01")
                         ["sources"][0]["held_after_close"], 1)
        quality = self.lake.quality.partition("venue", "2026-03-01")
        self.assertEqual(quality["held_events"], 1)
        self.assertEqual(quality["pending_events"], 0)
        inventory = self.lake.snapshots.inventory("venue", "2026-03-01")
        self.assertEqual(inventory["held_events"], 1)
        self.assertEqual(inventory["unpublished_events"], 0)

    def test_close_holds_previously_unpublished_events(self):
        self.lake.ingest("counter", "v1", "base", [self.visit("base", count=5)])
        self.publish()
        # A row accepted but never published before the freeze would otherwise
        # vanish from the official number; it must be held instead.
        self.lake.ingest("counter", "v1", "gap", [self.visit("gap", count=3)])
        result = self.lake.close_day("venue", "2026-03-01", closed_by="ops", reason="freeze")
        self.assertTrue(result["closed"])
        state = self.lake.closures.state("venue", "2026-03-01")
        self.assertEqual(state["held"]["total"], 1)
        self.assertEqual(state["held"]["events"][0]["held_reason"], "unpublished_at_close")
        self.assertEqual(self.lake.analytics.day("venue", "2026-03-01")["total_visitors"], 5)

    # ------------------------------------------------------------- timezones
    def test_cross_timezone_sources_land_on_venue_local_day(self):
        self.lake.register_venue("west", "West", "America/Los_Angeles")
        self.lake.register_exhibit("west-robot", "west", "Robot", "Maker", "2026-01-01")
        self.lake.register_source("west-counter", "west", "visit")
        self.lake.register_contract("west-v1", "west-counter",
                                    ["event_id", "exhibit_id", "event_at", "count"],
                                    "2026-01-01T00:00:00+00:00")
        self.lake.ingest("west-counter", "west-v1", "w1", [
            {"event_id": "w", "exhibit_id": "west-robot",
             "event_at": "2026-03-01T07:30:00+08:00", "count": 4}])
        west = self.lake.publish_day("west", "2026-02-28")
        east = self.publish("venue", None) if False else None
        self.assertEqual(west["local_day"], "2026-02-28")
        report = self.lake.analytics.day("west", "2026-02-28")
        self.assertEqual(report["total_visitors"], 4)
        with self.assertRaises(NotFound):
            self.lake.analytics.day("west", "2026-03-01")
        # Freezing one venue's day does not freeze another venue or day.
        self.lake.close_day("west", "2026-02-28", closed_by="ops", reason="sent")
        self.assertFalse(self.lake.closures.state("venue", "2026-03-01")["closed"])

    def test_integrity_flags_held_event_that_reached_a_report(self):
        self.closed_day()
        self.lake.ingest("counter", "v1", "late", [self.visit("late", count=9)])
        self.assertTrue(self.lake.audit.venue_integrity("venue")["valid"])
        with self.lake.store.transaction() as conn:
            conn.execute("INSERT INTO snapshot_members VALUES(?,?,?,?)",
                         (self.lake.snapshots.head("venue", "2026-03-01")["head_id"],
                          "counter", "late", "bad"))
        issues = [x["code"] for x in self.lake.audit.venue_integrity("venue")["issues"]]
        self.assertIn("held_event_published", issues)

    # ------------------------------------------------------------ concurrency
    def test_repeated_late_batches_do_not_double_count(self):
        self.closed_day()
        first = self.lake.ingest("counter", "v1", "late", [self.visit("late", count=9)])
        replay = self.lake.ingest("counter", "v1", "late", [self.visit("late", count=9)])
        self.assertEqual(first["batch_id"], replay["batch_id"])
        self.assertTrue(replay["replay"])
        self.assertEqual(self.lake.closures.state("venue", "2026-03-01")["held"]["total"], 1)

    def test_concurrent_publish_and_close_never_mix_versions(self):
        self.lake.ingest("counter", "v1", "base", [self.visit("base", count=5)])
        head = self.publish()
        self.lake.ingest("counter", "v1", "more", [self.visit("more", count=2)])
        candidate = self.lake.snapshots.stage("venue", "2026-03-01", head["snapshot_id"])
        outcomes = {}
        barrier = threading.Barrier(2)

        def publish():
            barrier.wait()
            try:
                outcomes["publish"] = self.lake.snapshots.publish(candidate["snapshot_id"],
                                                                  head["snapshot_id"])["snapshot_id"]
            except Exception as exc:  # noqa: BLE001 - recorded for the assertion
                outcomes["publish"] = type(exc).__name__

        def close():
            barrier.wait()
            try:
                outcomes["close"] = self.lake.close_day(
                    "venue", "2026-03-01", expected_head=head["snapshot_id"],
                    closed_by="ops", reason="daily")["head_id"]
            except Exception as exc:  # noqa: BLE001
                outcomes["close"] = type(exc).__name__

        threads = [threading.Thread(target=publish), threading.Thread(target=close)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        state = self.lake.closures.state("venue", "2026-03-01")
        report = self.lake.analytics.day("venue", "2026-03-01")
        if outcomes["close"] == "Conflict":
            # Close lost the race: the new head is published but the day stays open.
            self.assertEqual(state["closed"], False)
            self.assertEqual(report["snapshot_id"], candidate["snapshot_id"])
        else:
            # Close won: it froze the old head and the later publish must fail.
            self.assertEqual(state["closed"], True)
            self.assertEqual(state["head_id"], head["snapshot_id"])
            self.assertEqual(report["snapshot_id"], head["snapshot_id"])
            self.assertEqual(outcomes["publish"], "StateError")
        self.assertEqual(state["generation"], 1 if state["closed"] else 2)


class ClosureApiTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.client = TestClient(create_app(str(Path(self.folder.name) / "api.db")))
        self.client.post("/venues", json={"venue_id": "venue", "name": "Venue",
                                         "timezone": "Asia/Shanghai"})
        self.client.post("/exhibits", json={"exhibit_id": "robot", "venue_id": "venue",
                                           "name": "Robot", "maker": "Maker",
                                           "launched_on": "2026-01-01"})
        self.client.post("/sources", json={"source_id": "counter", "venue_id": "venue",
                                          "kind": "visit"})
        self.client.post("/contracts", json={"contract_id": "v1", "source_id": "counter",
                                            "fields": ["event_id", "exhibit_id", "event_at", "count"],
                                            "effective_from": "2026-01-01T00:00:00+00:00"})

    def row(self, identity="a", count=5):
        return {"event_id": identity, "exhibit_id": "robot",
                "event_at": "2026-03-01T10:00:00+08:00", "count": count}

    def publish(self):
        staged = self.client.post("/snapshots/stage",
                                  json={"venue_id": "venue", "local_day": "2026-03-01"}).json()
        return self.client.post(f"/snapshots/{staged['snapshot_id']}/publish",
                                json={"expected_head": None}).json()["snapshot_id"]

    def test_close_then_late_ingest_then_reopen_over_api(self):
        self.client.post("/batches", json={"source_id": "counter", "contract_id": "v1",
                                          "request_key": "base", "events": [self.row()]})
        first = self.publish()
        closed = self.client.post("/venues/venue/days/2026-03-01/close",
                                  json={"closed_by": "ops", "reason": "daily-report-sent",
                                        "request_key": "close-1"})
        self.assertEqual(closed.status_code, 200, closed.text)
        self.assertTrue(closed.json()["closed"])

        late = self.client.post("/batches", json={"source_id": "counter", "contract_id": "v1",
                                                 "request_key": "late",
                                                 "events": [self.row("late", 9)]})
        self.assertEqual(late.json()["held_rows"], 1)
        report = self.client.get("/venues/venue/days/2026-03-01/report").json()
        self.assertEqual(report["snapshot_id"], first)
        self.assertEqual(report["total_visitors"], 5)

        staged = self.client.post("/snapshots/stage",
                                  json={"venue_id": "venue", "local_day": "2026-03-01"})
        self.assertEqual(staged.status_code, 409)
        self.assertEqual(staged.json()["error"]["code"], "invalid_state")

        reopened = self.client.post("/venues/venue/days/2026-03-01/reopen",
                                    json={"approved_by": "venue-lead",
                                          "reason": "late-device-batch"})
        self.assertEqual(reopened.status_code, 200, reopened.text)
        self.assertEqual(reopened.json()["history"][0]["status"], "reopened")

        staged = self.client.post("/snapshots/stage",
                                  json={"venue_id": "venue", "local_day": "2026-03-01",
                                        "expected_head": first}).json()
        published = self.client.post(f"/snapshots/{staged['snapshot_id']}/publish",
                                     json={"expected_head": first}).json()
        self.assertEqual(published["generation"], 2)
        self.assertEqual(self.client.get("/venues/venue/days/2026-03-01/report")
                         .json()["total_visitors"], 14)
        self.assertEqual(self.client.get(f"/venues/venue/days/2026-03-01/report"
                                         f"?snapshot_id={first}").json()["total_visitors"], 5)
        closure = self.client.get("/venues/venue/days/2026-03-01/closure").json()
        self.assertEqual(closure["history"][0]["approved_by"], "venue-lead")
        self.assertEqual(closure["held"]["total"], 0)

    def test_reopen_without_approval_is_invalid(self):
        self.client.post("/batches", json={"source_id": "counter", "contract_id": "v1",
                                          "request_key": "base", "events": [self.row()]})
        self.publish()
        self.client.post("/venues/venue/days/2026-03-01/close",
                         json={"closed_by": "ops", "reason": "daily"})
        result = self.client.post("/venues/venue/days/2026-03-01/reopen",
                                  json={"approved_by": "", "reason": ""})
        self.assertEqual(result.status_code, 400)
        self.assertEqual(result.json()["error"]["code"], "invalid_input")


if __name__ == "__main__":
    unittest.main()
