import datetime as dt
import unittest

from event_router.ingest import Ingester
from event_router.store import EventStore

from tests._helpers import TempWorkspace, make_schema


def fixed_now():
    # 固定“当前时间”：2026-01-30T12:00:00Z，迟到窗口判定才可预测。
    return dt.datetime(2026, 1, 30, 12, 0, 0, tzinfo=dt.timezone.utc)


class IngestBaseTest(unittest.TestCase):
    def setUp(self):
        self.ws = TempWorkspace()
        self.addCleanup(self.ws.cleanup)

    def make_ingester(self, schema=None, retention_days=7):
        store = EventStore(self.ws.db_path)
        self.addCleanup(store.close)
        schema = schema or make_schema()
        store.register_schema(schema)
        return Ingester(store, schema, retention_days=retention_days,
                        now_func=fixed_now), store


class HappyPathTest(IngestBaseTest):
    def test_normalizes_and_partitions_by_event_time(self):
        records = [
            {
                "event_id": "e1",
                "event_time": "2026-01-29T10:00:00Z",
                "user_id": "1",
                "amount": "3.5",
                "unknown_top": {"keep": True},
            },
            {
                "event_id": "e2",
                # 乱序：更早的事件排在后面，仍应进入自己的日期分区。
                "event_time": "2026-01-28T23:59:00Z",
                "user_id": 2,
            },
        ]
        self.ws.write_jsonl(records)
        ingester, store = self.make_ingester()

        stats = ingester.ingest_file(self.ws.jsonl_path)

        self.assertEqual(stats.inserted, 2)
        self.assertEqual(store.count_events(), 2)
        self.assertEqual(store.list_dates(), ["2026-01-28", "2026-01-29"])
        event1 = store.get_event("e1")
        self.assertEqual(event1["event_date"], "2026-01-29")
        self.assertEqual(event1["payload"]["user_id"], 1)
        self.assertEqual(event1["payload"]["amount"], 3.5)
        self.assertEqual(event1["extras"]["unknown_top"], {"keep": True})
        self.assertEqual(event1["is_late"], 0)

    def test_string_numbers_and_stringified_id_are_accepted(self):
        self.ws.write_jsonl([
            {"event_id": 9001, "event_time": 1769738400,
             "user_id": "7", "amount": "1.25"},
        ])
        ingester, store = self.make_ingester()
        stats = ingester.ingest_file(self.ws.jsonl_path)
        self.assertEqual(stats.inserted, 1)
        event = store.get_event("9001")
        self.assertIsNotNone(event)
        self.assertEqual(event["payload"]["user_id"], 7)


class BadDataTest(IngestBaseTest):
    def test_bad_json_and_bad_records_go_to_dead_letter(self):
        self.ws.write_jsonl([
            '{"event_id": "ok1", "event_time": "2026-01-29", "user_id": 1}',
            "{not valid json",
            '{"event_id": "ok2", "event_time": "2026-01-29"}',  # 缺 user_id
            '{"event_id": "ok3", "event_time": "2026-01-29", "user_id": "x"}',
            '["a", "json array"]',
        ])
        ingester, store = self.make_ingester()
        stats = ingester.ingest_file(self.ws.jsonl_path)

        self.assertEqual(stats.inserted, 1)
        self.assertEqual(stats.dead_letters, 4)
        dlq = store.list_dead_letters()
        reasons = sorted(item["reason"] for item in dlq)
        self.assertEqual(
            reasons,
            ["invalid_json", "missing_required", "record_not_object",
             "type_coercion"],
        )
        bad_json = next(item for item in dlq if item["reason"] == "invalid_json")
        self.assertEqual(bad_json["line_no"], 2)
        self.assertIn("position", bad_json["details"])
        # 成功的事件仍然正常落库，坏数据不影响整批。
        self.assertIsNotNone(store.get_event("ok1"))


class DeduplicationTest(IngestBaseTest):
    def _records(self):
        return [
            {"event_id": "dup", "event_time": "2026-01-29", "user_id": 1},
            {"event_id": "dup", "event_time": "2026-01-29", "user_id": 2},
            {"event_id": "uniq", "event_time": "2026-01-29", "user_id": 3},
        ]

    def test_duplicate_event_id_inserted_once(self):
        self.ws.write_jsonl(self._records())
        ingester, store = self.make_ingester()
        stats = ingester.ingest_file(self.ws.jsonl_path)
        self.assertEqual(stats.inserted, 2)
        self.assertEqual(stats.duplicates, 1)
        self.assertEqual(store.count_events(), 2)
        # 首次到达的内容生效，重复送达不覆盖。
        self.assertEqual(store.get_event("dup")["payload"]["user_id"], 1)

    def test_full_rerun_does_not_double_count(self):
        self.ws.write_jsonl(self._records())
        ingester, store = self.make_ingester()
        first = ingester.ingest_file(self.ws.jsonl_path)
        self.assertEqual(first.inserted, 2)

        second = ingester.full_rerun(self.ws.jsonl_path)
        self.assertEqual(second.inserted, 0)
        self.assertEqual(second.duplicates, 3)
        self.assertEqual(store.count_events(), 2)

    def test_rerun_does_not_duplicate_dead_letters(self):
        self.ws.write_jsonl(["{broken"] * 3)
        ingester, store = self.make_ingester()
        ingester.ingest_file(self.ws.jsonl_path)
        self.assertEqual(store.count_dead_letters(), 3)
        ingester.full_rerun(self.ws.jsonl_path)
        self.assertEqual(store.count_dead_letters(), 3)


class LateDataTest(IngestBaseTest):
    def test_events_inside_and_outside_retention_window(self):
        # now = 2026-01-30，窗口 7 天，cutoff = 2026-01-23。
        self.ws.write_jsonl([
            {"event_id": "in-window", "event_time": "2026-01-25",
             "user_id": 1},
            {"event_id": "on-cutoff", "event_time": "2026-01-23",
             "user_id": 2},
            {"event_id": "too-old", "event_time": "2026-01-22T08:00:00Z",
             "user_id": 3},
        ])
        ingester, store = self.make_ingester(retention_days=7)
        stats = ingester.ingest_file(self.ws.jsonl_path)

        self.assertEqual(stats.late, 1)
        self.assertEqual(store.get_event("in-window")["is_late"], 0)
        self.assertEqual(store.get_event("on-cutoff")["is_late"], 0)
        late_event = store.get_event("too-old")
        self.assertEqual(late_event["is_late"], 1)
        self.assertEqual(late_event["event_date"], "2026-01-22")

    def test_late_events_still_land_in_own_partition(self):
        self.ws.write_jsonl([
            {"event_id": "late", "event_time": "2025-12-31T23:59:00Z",
             "user_id": 1},
            {"event_id": "fresh", "event_time": "2026-01-29",
             "user_id": 2},
        ])
        ingester, store = self.make_ingester()
        ingester.ingest_file(self.ws.jsonl_path)
        self.assertEqual(
            store.list_dates(), ["2025-12-31", "2026-01-29"]
        )


class CheckpointTest(IngestBaseTest):
    def test_resume_skips_already_committed_lines(self):
        self.ws.write_jsonl([
            {"event_id": "e1", "event_time": "2026-01-29", "user_id": 1},
            {"event_id": "e2", "event_time": "2026-01-29", "user_id": 2},
            {"event_id": "e3", "event_time": "2026-01-29", "user_id": 3},
        ])
        ingester, store = self.make_ingester()
        first = ingester.ingest_file(self.ws.jsonl_path, chunk_size=2)
        self.assertEqual(first.chunks, 2)

        # 再次运行：checkpoint 已在文件尾，不应重复读任何行。
        second = ingester.ingest_file(self.ws.jsonl_path, chunk_size=2)
        self.assertEqual(second.lines_read, 0)
        self.assertEqual(second.inserted, 0)
        self.assertEqual(store.count_events(), 3)

    def test_append_then_resume_ingests_only_new_lines(self):
        self.ws.write_jsonl([
            {"event_id": "e1", "event_time": "2026-01-29", "user_id": 1},
        ])
        ingester, store = self.make_ingester()
        ingester.ingest_file(self.ws.jsonl_path)
        self.ws.append_jsonl([
            {"event_id": "e2", "event_time": "2026-01-29", "user_id": 2},
        ])
        stats = ingester.ingest_file(self.ws.jsonl_path)
        self.assertEqual(stats.lines_read, 1)
        self.assertEqual(stats.inserted, 1)
        self.assertEqual(store.count_events(), 2)


if __name__ == "__main__":
    unittest.main()
