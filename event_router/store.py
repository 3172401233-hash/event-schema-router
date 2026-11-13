"""SQLite 存储层：事件表、死信表、checkpoint 与 schema 版本登记。

所有写操作都在调用方提供的单个事务里完成，ingest 层保证
“数据写入 + checkpoint 推进”在同一个事务内提交，从而满足崩溃一致性。
"""

import json
import sqlite3

from .errors import IncompatibleSchemaError

SCHEMA_DDL = """
CREATE TABLE IF NOT EXISTS schema_versions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL,
    version     INTEGER NOT NULL,
    definition  TEXT NOT NULL,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    UNIQUE (name, version)
);

CREATE TABLE IF NOT EXISTS events (
    event_id     TEXT NOT NULL,
    event_time   TEXT NOT NULL,              -- UTC ISO8601
    event_date   TEXT NOT NULL,              -- 日期分区键 YYYY-MM-DD
    schema_name  TEXT NOT NULL,
    schema_ver   INTEGER NOT NULL,
    payload      TEXT NOT NULL,              -- 规范化后的全部字段（JSON）
    extras       TEXT NOT NULL,              -- 未知可选字段原始内容（JSON）
    is_late      INTEGER NOT NULL DEFAULT 0, -- 超过保留窗口的迟到数据
    source_file  TEXT NOT NULL,
    line_no      INTEGER NOT NULL,
    created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (event_id)
);

CREATE INDEX IF NOT EXISTS idx_events_date ON events(event_date);
CREATE INDEX IF NOT EXISTS idx_events_late ON events(is_late);

CREATE TABLE IF NOT EXISTS dead_letters (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id     TEXT,                       -- 尽力解析，解析不到则为 NULL
    source_file  TEXT NOT NULL,
    line_no      INTEGER NOT NULL,
    offset       INTEGER NOT NULL,
    reason       TEXT NOT NULL,
    details      TEXT NOT NULL,
    raw_line     TEXT NOT NULL,
    created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    UNIQUE (source_file, line_no)
);

CREATE INDEX IF NOT EXISTS idx_dlq_reason ON dead_letters(reason);

CREATE TABLE IF NOT EXISTS checkpoints (
    source_file  TEXT PRIMARY KEY,
    offset       INTEGER NOT NULL,           -- 下一条待读字节偏移
    line_no      INTEGER NOT NULL,           -- 下一条待读物理行号
    updated_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
"""


class EventStore(object):
    """封装 SQLite 连接与所有持久化操作。"""

    def __init__(self, db_path):
        self.db_path = db_path
        # check_same_thread=False 让测试里跨线程/重开更灵活；单库单写者。
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        # WAL + NORMAL 在保证事务原子性的同时避免 fsync 放大。
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.executescript(SCHEMA_DDL)
        self.conn.commit()

    # -- 生命周期 ----------------------------------------------------------

    def close(self):
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    # -- schema 版本登记 ---------------------------------------------------

    def registered_versions(self, name):
        rows = self.conn.execute(
            "SELECT version FROM schema_versions WHERE name = ? ORDER BY version",
            (name,),
        ).fetchall()
        return [row["version"] for row in rows]

    def register_schema(self, schema):
        """登记一个 schema 版本，做持久化层面的演进校验。

        重复登记完全相同的版本视为幂等 no-op。
        """
        existing = self.conn.execute(
            "SELECT definition FROM schema_versions WHERE name = ? AND version = ?",
            (schema.name, schema.version),
        ).fetchone()
        if existing is not None:
            if json.loads(existing["definition"]) != schema.to_dict():
                raise IncompatibleSchemaError(
                    "schema {} v{} already registered with a different definition".format(
                        schema.name, schema.version
                    )
                )
            return False
        latest_row = self.conn.execute(
            "SELECT definition FROM schema_versions WHERE name = ? "
            "ORDER BY version DESC LIMIT 1",
            (schema.name,),
        ).fetchone()
        if latest_row is not None:
            from .schema import Schema  # 避免循环导入

            old_schema = Schema.from_dict(json.loads(latest_row["definition"]))
            schema.check_compatible_with(old_schema)
        self.conn.execute(
            "INSERT INTO schema_versions (name, version, definition) VALUES (?, ?, ?)",
            (schema.name, schema.version, json.dumps(schema.to_dict(), ensure_ascii=False)),
        )
        self.conn.commit()
        return True

    # -- checkpoint --------------------------------------------------------

    def get_checkpoint(self, source_file):
        row = self.conn.execute(
            "SELECT offset, line_no FROM checkpoints WHERE source_file = ?",
            (source_file,),
        ).fetchone()
        if row is None:
            return 0, 0
        return row["offset"], row["line_no"]

    def save_checkpoint(self, source_file, offset, line_no):
        self.conn.execute(
            "INSERT INTO checkpoints (source_file, offset, line_no, updated_at) "
            "VALUES (?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) "
            "ON CONFLICT(source_file) DO UPDATE SET "
            "offset = excluded.offset, line_no = excluded.line_no, "
            "updated_at = excluded.updated_at",
            (source_file, offset, line_no),
        )

    def reset_checkpoint(self, source_file):
        """全量重跑入口：清掉进度（事件去重仍由主键保证）。"""
        self.conn.execute(
            "DELETE FROM checkpoints WHERE source_file = ?", (source_file,)
        )
        self.conn.commit()

    # -- 事件 / 死信 -------------------------------------------------------

    def insert_event(self, event):
        """按 event_id 去重插入，返回 True 表示新入库、False 表示重复送达。"""
        cursor = self.conn.execute(
            "INSERT OR IGNORE INTO events "
            "(event_id, event_time, event_date, schema_name, schema_ver, "
            " payload, extras, is_late, source_file, line_no) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                event["event_id"],
                event["event_time"],
                event["event_date"],
                event["schema_name"],
                event["schema_ver"],
                json.dumps(event["payload"], ensure_ascii=False),
                json.dumps(event["extras"], ensure_ascii=False),
                1 if event["is_late"] else 0,
                event["source_file"],
                event["line_no"],
            ),
        )
        return cursor.rowcount == 1

    def insert_dead_letter(self, dead_letter):
        """同一源文件同一行只保留一条死信，返回 True 表示新写入。"""
        cursor = self.conn.execute(
            "INSERT OR IGNORE INTO dead_letters "
            "(event_id, source_file, line_no, offset, reason, details, raw_line) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                dead_letter.get("event_id"),
                dead_letter["source_file"],
                dead_letter["line_no"],
                dead_letter["offset"],
                dead_letter["reason"],
                json.dumps(dead_letter.get("details", {}), ensure_ascii=False),
                dead_letter["raw_line"],
            ),
        )
        return cursor.rowcount == 1

    # -- 查询辅助（供测试与上层使用）--------------------------------------

    def count_events(self):
        return self.conn.execute("SELECT COUNT(*) AS n FROM events").fetchone()["n"]

    def count_dead_letters(self):
        return self.conn.execute("SELECT COUNT(*) AS n FROM dead_letters").fetchone()["n"]

    def get_event(self, event_id):
        row = self.conn.execute(
            "SELECT * FROM events WHERE event_id = ?", (event_id,)
        ).fetchone()
        if row is None:
            return None
        result = dict(row)
        result["payload"] = json.loads(result["payload"])
        result["extras"] = json.loads(result["extras"])
        return result

    def list_dead_letters(self):
        rows = self.conn.execute(
            "SELECT * FROM dead_letters ORDER BY id"
        ).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["details"] = json.loads(item["details"])
            result.append(item)
        return result

    def list_dates(self):
        return [
            row["event_date"]
            for row in self.conn.execute(
                "SELECT DISTINCT event_date FROM events ORDER BY event_date"
            ).fetchall()
        ]
