"""测试公共工具：临时目录、schema 与 JSONL 文件构造。"""

import json
import os
import tempfile

from event_router.schema import Schema


BASE_SCHEMA = {
    "name": "user_action",
    "version": 1,
    "fields": [
        {"name": "event_id", "type": "string", "required": True},
        {"name": "event_time", "type": "datetime", "required": True},
        {"name": "user_id", "type": "int", "required": True},
        {"name": "amount", "type": "float", "required": False},
        {"name": "note", "type": "string", "required": False},
        {"name": "tags", "type": "json", "required": False},
    ],
}


def make_schema(raw=None):
    return Schema.from_dict(raw or BASE_SCHEMA)


class TempWorkspace(object):
    def __init__(self):
        self.dir = tempfile.mkdtemp(prefix="event_router_test_")
        self.db_path = os.path.join(self.dir, "events.db")
        self.jsonl_path = os.path.join(self.dir, "events.jsonl")

    def cleanup(self):
        import shutil

        shutil.rmtree(self.dir, ignore_errors=True)

    def write_jsonl(self, records, path=None):
        """records 中字符串原样写入（用于构造坏 JSON），dict 自动序列化。"""
        target = path or self.jsonl_path
        with open(target, "w", encoding="utf-8") as handle:
            for record in records:
                if isinstance(record, str):
                    handle.write(record + "\n")
                else:
                    handle.write(json.dumps(record, ensure_ascii=False) + "\n")
        return target

    def append_jsonl(self, records, path=None):
        target = path or self.jsonl_path
        with open(target, "a", encoding="utf-8") as handle:
            for record in records:
                if isinstance(record, str):
                    handle.write(record + "\n")
                else:
                    handle.write(json.dumps(record, ensure_ascii=False) + "\n")
        return target
