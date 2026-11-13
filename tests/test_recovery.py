import datetime as dt
import json
import os
import sqlite3
import unittest

from event_router.errors import IncompatibleSchemaError, SimulatedCrash
from event_router.ingest import Ingester, Phase
from event_router.schema import Schema
from event_router.store import EventStore

from tests._helpers import BASE_SCHEMA, TempWorkspace, make_schema


def fixed_now():
    return dt.datetime(2026, 1, 30, 12, 0, 0, tzinfo=dt.timezone.utc)


class TransactionCrashTest(unittest.TestCase):
    def setUp(self):
        self.ws = TempWorkspace()
        self.addCleanup(self.ws.cleanup)
        self.records = [
            {"event_id": "e{}".format(i), "event_time": "2026-01-29",
             "user_id": i}
            for i in range(1, 6)
        ]

    def _open(self):
        store = EventStore(self.ws.db_path)
        schema = make_schema()
        store.register_schema(schema)
        return store, Ingester(store, schema, now_func=fixed_now)

    def test_crash_before_commit_rolls_back_data_and_checkpoint(self):
        self.ws.write_jsonl(self.records)
        store, ingester = self._open()

        def crash(phase, chunk_index, pending):
            self.assertEqual(phase, Phase.BEFORE_COMMIT)
            raise SimulatedCrash("kill -9 during chunk 1")

        with self.assertRaises(SimulatedCrash):
            ingester.ingest_file(
                self.ws.jsonl_path, chunk_size=2,
                crash_hook=crash, crash_chunk=1,
            )

        # 进程被 kill 的等价语义：连接关闭，未 commit 的事务整体回滚。
        store.close()

        # 用一个全新连接核对磁盘状态：没有事件、没有 checkpoint。
        probe = sqlite3.connect(self.ws.db_path)
        try:
            self.assertEqual(probe.execute("SELECT COUNT(*) FROM events").fetchone()[0], 0)
            self.assertEqual(
                probe.execute("SELECT COUNT(*) FROM checkpoints").fetchone()[0], 0
            )
        finally:
            probe.close()

    def test_resume_after_crash_processes_everything_exactly_once(self):
        self.ws.write_jsonl(self.records)
        store, ingester = self._open()

        def crash_once(phase, chunk_index, pending):
            if chunk_index == 2:
                raise SimulatedCrash("kill during chunk 2")

        with self.assertRaises(SimulatedCrash):
            ingester.ingest_file(
                self.ws.jsonl_path, chunk_size=2, crash_hook=crash_once
            )
        store.close()

        # 模拟进程重启：重新打开库，从已提交的 checkpoint 继续。
        store2, ingester2 = self._open()
        stats = ingester2.ingest_file(self.ws.jsonl_path, chunk_size=2)

        # 第一个 chunk（e1,e2）已提交；续跑只会读到 e3,e4,e5。
        self.assertEqual(stats.lines_read, 3)
        self.assertEqual(stats.inserted, 3)
        self.assertEqual(store2.count_events(), 5)
        offset, line_no = store2.get_checkpoint(os.path.basename(self.ws.jsonl_path))
        self.assertEqual(offset, os.path.getsize(self.ws.jsonl_path))
        self.assertEqual(line_no, 5)
        store2.close()

    def test_repeated_crash_and_resume_never_duplicates(self):
        self.ws.write_jsonl(self.records)
        # 每次只让一个 chunk 成功，反复“kill - 重启”。
        survived_chunks = {"n": 0}

        def crash_alternating(phase, chunk_index, pending):
            if chunk_index % 2 == 0:
                raise SimulatedCrash("alternating kill")

        store, ingester = self._open()
        with self.assertRaises(SimulatedCrash):
            ingester.ingest_file(
                self.ws.jsonl_path, chunk_size=2, crash_hook=crash_alternating
            )
        store.close()
        survived_chunks["n"] = 1

        for _ in range(3):
            store, ingester = self._open()
            try:
                ingester.ingest_file(
                    self.ws.jsonl_path, chunk_size=2,
                    crash_hook=crash_alternating,
                )
                break
            except SimulatedCrash:
                store.close()
                survived_chunks["n"] += 1

        self.assertEqual(store.count_events(), 5)
        # 全量重跑再兜底一次，计数仍然不变。
        rerun = ingester.full_rerun(self.ws.jsonl_path, chunk_size=2)
        self.assertEqual(rerun.inserted, 0)
        self.assertEqual(rerun.duplicates, 5)
        self.assertEqual(store.count_events(), 5)
        store.close()

    def test_crash_then_resume_on_same_connection(self):
        self.ws.write_jsonl(self.records)
        store, ingester = self._open()

        def crash_first(phase, chunk_index, pending):
            if chunk_index == 1:
                raise SimulatedCrash("kill first chunk")

        with self.assertRaises(SimulatedCrash):
            ingester.ingest_file(
                self.ws.jsonl_path, chunk_size=2, crash_hook=crash_first
            )
        # 不关闭连接：库内部已 rollback，同连接直接续跑也必须安全。
        stats = ingester.ingest_file(self.ws.jsonl_path, chunk_size=2)
        self.assertEqual(stats.inserted, 5)
        self.assertEqual(store.count_events(), 5)
        self.assertEqual(
            store.get_checkpoint(os.path.basename(self.ws.jsonl_path))[0],
            os.path.getsize(self.ws.jsonl_path),
        )
        store.close()


class SchemaUpgradeTest(unittest.TestCase):
    def setUp(self):
        self.ws = TempWorkspace()
        self.addCleanup(self.ws.cleanup)
        self.store = EventStore(self.ws.db_path)
        self.addCleanup(self.store.close)

    def test_old_data_readable_after_registering_new_version(self):
        v1 = make_schema()
        self.assertTrue(self.store.register_schema(v1))
        ingester_v1 = Ingester(self.store, v1, now_func=fixed_now)
        self.ws.write_jsonl([
            {"event_id": "old-1", "event_time": "2026-01-29",
             "user_id": 1, "upstream_new_field": "raw"},
        ])
        ingester_v1.ingest_file(self.ws.jsonl_path)

        v2_dict = {
            "name": "user_action",
            "version": 2,
            "fields": BASE_SCHEMA["fields"] + [
                {"name": "channel", "type": "string", "required": False},
            ],
        }
        v2 = Schema.from_dict(v2_dict)
        self.assertTrue(self.store.register_schema(v2))

        # 旧记录原样可读，不需要任何重写；新字段对旧记录只是不存在。
        old_event = self.store.get_event("old-1")
        self.assertEqual(old_event["schema_ver"], 1)
        self.assertEqual(old_event["extras"]["upstream_new_field"], "raw")
        self.assertNotIn("channel", old_event["payload"])
        self.assertEqual(
            self.store.registered_versions("user_action"), [1, 2]
        )

        # 新版本写入带新字段的数据，与旧记录共存。
        self.ws.append_jsonl([
            {"event_id": "new-1", "event_time": "2026-01-29",
             "user_id": 2, "channel": "mobile"},
        ])
        stats = Ingester(self.store, v2, now_func=fixed_now).ingest_file(
            self.ws.jsonl_path
        )
        self.assertEqual(stats.inserted, 1)
        new_event = self.store.get_event("new-1")
        self.assertEqual(new_event["schema_ver"], 2)
        self.assertEqual(new_event["payload"]["channel"], "mobile")
        self.assertEqual(self.store.count_events(), 2)

    def test_incompatible_upgrade_rejected_at_registration(self):
        v1 = make_schema()
        self.store.register_schema(v1)
        bad_fields = [
            field for field in BASE_SCHEMA["fields"] if field["name"] != "note"
        ]
        v_bad = Schema.from_dict({
            "name": "user_action",
            "version": 2,
            "fields": bad_fields,  # 删除了可选字段 note
        })
        with self.assertRaises(IncompatibleSchemaError):
            self.store.register_schema(v_bad)
        # 注册失败不能留下半截版本记录。
        self.assertEqual(
            self.store.registered_versions("user_action"), [1]
        )

    def test_same_version_different_definition_rejected(self):
        v1 = make_schema()
        self.store.register_schema(v1)
        changed_fields = [
            dict(field, type="string") if field["name"] == "user_id" else field
            for field in BASE_SCHEMA["fields"]
        ]
        v1_again = Schema.from_dict({
            "name": "user_action",
            "version": 1,
            "fields": changed_fields,
        })
        with self.assertRaises(IncompatibleSchemaError):
            self.store.register_schema(v1_again)

    def test_idempotent_registration_of_identical_version(self):
        v1 = make_schema()
        self.store.register_schema(v1)
        # 完全相同的定义重复登记是 no-op。
        self.assertFalse(self.store.register_schema(make_schema()))


class FullRerunTest(unittest.TestCase):
    def setUp(self):
        self.ws = TempWorkspace()
        self.addCleanup(self.ws.cleanup)

    def test_rerun_after_schema_change_keeps_single_copy(self):
        records = [
            {"event_id": "e1", "event_time": "2026-01-29", "user_id": 1},
            "{broken line",
            {"event_id": "e2", "event_time": "2026-01-29", "user_id": 2},
        ]
        self.ws.write_jsonl(records)
        store = EventStore(self.ws.db_path)
        self.addCleanup(store.close)
        v1 = make_schema()
        store.register_schema(v1)
        ingester = Ingester(store, v1, now_func=fixed_now)
        first = ingester.ingest_file(self.ws.jsonl_path, chunk_size=1)
        self.assertEqual((first.inserted, first.dead_letters), (2, 1))

        # 全量重跑：事件与死信都靠唯一键保持单份。
        second = ingester.full_rerun(self.ws.jsonl_path, chunk_size=1)
        self.assertEqual(second.inserted, 0)
        self.assertEqual(second.duplicates, 2)
        self.assertEqual(second.dead_letters, 0)
        self.assertEqual(store.count_events(), 2)
        self.assertEqual(store.count_dead_letters(), 1)


if __name__ == "__main__":
    unittest.main()
