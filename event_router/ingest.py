"""分块 JSONL 摄入管道。

关键保证：

- 按字节偏移读取，checkpoint 记录“下一条待读字节”，进程重启后精确续跑。
- 每个 chunk 的事件/死信写入与 checkpoint 推进在**同一个 SQLite 事务**里
  提交；事务未提交进程就死掉时，数据和 checkpoint 一起回滚。
- ``event_id`` 是事件主键，重复送达靠 ``INSERT OR IGNORE`` 幂等去重，
  全量重跑也不会重复计数。
- 按 ``event_time`` 计算 UTC 日期分区；超过保留窗口的迟到数据照常入库，
  仅以 ``is_late=1`` 单独标记。
"""

import datetime as _dt
import json
import os
from dataclasses import dataclass, field
from enum import Enum

from .errors import NormalizationError, SimulatedCrash


class Phase(Enum):
    """crash_hook 的触发时机。"""

    AFTER_BEGIN = "after_begin"      # 事务已 BEGIN、数据已写入、尚未 commit
    BEFORE_COMMIT = "before_commit"  # 与 AFTER_BEGIN 等价语义，语义化别名


@dataclass
class IngestStats:
    """一次（或多次累计）摄入的计数。"""

    chunks: int = 0
    lines_read: int = 0
    inserted: int = 0
    duplicates: int = 0
    dead_letters: int = 0
    late: int = 0
    # 明细：每个文件最后推进到的字节偏移，便于测试断言。
    files: dict = field(default_factory=dict)

    def add(self, other):
        self.chunks += other.chunks
        self.lines_read += other.lines_read
        self.inserted += other.inserted
        self.duplicates += other.duplicates
        self.dead_letters += other.dead_letters
        self.late += other.late
        self.files.update(other.files)


def _safe_event_id(raw_record):
    """死信场景下尽力提取 event_id，提不到就返回 None。"""
    if isinstance(raw_record, dict):
        value = raw_record.get("event_id")
        if isinstance(value, str) and value:
            return value
        if isinstance(value, int) and not isinstance(value, bool):
            return str(value)
    return None


class Ingester(object):
    def __init__(self, store, schema, retention_days=7, now_func=None):
        self.store = store
        self.schema = schema
        self.retention_days = retention_days
        # now_func 可注入固定时钟，保证迟到窗口相关测试稳定。
        self._now_func = now_func or _dt.datetime.now

    def _today(self):
        now = self._now_func()
        if now.tzinfo is not None:
            now = now.astimezone(_dt.timezone.utc)
        return now.date()

    def _is_late(self, event_date):
        if self.retention_days is None:
            return False
        cutoff = self._today() - _dt.timedelta(days=self.retention_days)
        return event_date < cutoff

    def _write_dead_letter(self, source_file, line_no, offset, reason,
                           details, raw_line, event_id=None):
        return self.store.insert_dead_letter(
            {
                "event_id": event_id,
                "source_file": source_file,
                "line_no": line_no,
                "offset": offset,
                "reason": reason,
                "details": details,
                "raw_line": raw_line,
            }
        )

    def ingest_file(self, path, chunk_size=500, reprocess=False,
                    crash_hook=None, crash_chunk=None):
        """摄入单个 JSONL 文件，返回本次调用的 IngestStats。

        参数：
        - chunk_size: 每个 SQLite 事务处理的物理行数（含坏行/空行）。
        - reprocess: True 时先清 checkpoint 从文件头全量重跑；
          事件主键去重保证不会重复入库。
        - crash_hook: 可选回调 hook(phase, chunk_index, pending_count)，
          抛 SimulatedCrash 可模拟事务提交前进程被 kill。
        - crash_chunk: 仅在该 chunk 序号（从 1 开始）触发 hook，None 表示
          每个 chunk 都触发。
        """
        source_file = os.path.basename(path)
        if reprocess:
            self.store.reset_checkpoint(source_file)

        start_offset, start_line = self.store.get_checkpoint(source_file)
        stats = IngestStats()

        try:
            with open(path, "rb") as handle:
                handle.seek(start_offset)
                offset = start_offset
                line_no = start_line
                pending = 0
                chunk_index = 0

                while True:
                    line_bytes = handle.readline()
                    if not line_bytes:
                        break
                    line_start = offset
                    offset += len(line_bytes)
                    line_no += 1

                    stripped = line_bytes.strip()
                    if not stripped:
                        # 空行不参与事务，但偏移照常推进。
                        continue
                    stats.lines_read += 1

                    try:
                        raw_text = stripped.decode("utf-8")
                    except UnicodeDecodeError as exc:
                        if self._write_dead_letter(
                            source_file, line_no, line_start,
                            "invalid_encoding", {"error": str(ex)},
                            stripped.decode("utf-8", errors="replace"),
                        ):
                            stats.dead_letters += 1
                        pending += 1
                    else:
                        pending += self._consume_line(
                            raw_text, source_file, line_no, line_start, stats
                        )

                    if pending >= chunk_size:
                        chunk_index += 1
                        self._commit_chunk(
                            source_file, offset, line_no, chunk_index,
                            pending, crash_hook, crash_chunk,
                        )
                        stats.chunks += 1
                        pending = 0

                # 收尾：不足一个 chunk 的残留行与 checkpoint 一起提交。
                if pending > 0:
                    chunk_index += 1
                    self._commit_chunk(
                        source_file, offset, line_no, chunk_index,
                        pending, crash_hook, crash_chunk,
                    )
                    stats.chunks += 1
                else:
                    # 没有残留数据也要把 checkpoint 落到文件尾（含空行场景）。
                    self.store.save_checkpoint(source_file, offset, line_no)
                    self.store.conn.commit()
        except BaseException:
            # 模拟进程被 kill：未提交的事务整体回滚，数据与 checkpoint
            # 都保持在上一个已提交点，重启后从该点精确续跑。
            self.store.conn.rollback()
            raise

        stats.files[source_file] = offset
        return stats

    def _consume_line(self, raw_text, source_file, line_no, offset, stats):
        """处理一行已解码文本，返回该事务内新增的写入数（0 或 1）。"""
        try:
            raw_record = json.loads(raw_text)
        except json.JSONDecodeError as exc:
            if self._write_dead_letter(
                source_file, line_no, offset,
                "invalid_json", {"error": exc.msg, "position": exc.pos},
                raw_text,
            ):
                stats.dead_letters += 1
            return 1

        try:
            normalized, extras = self.schema.normalize(raw_record)
        except NormalizationError as exc:
            if self._write_dead_letter(
                source_file, line_no, offset,
                exc.reason, exc.details, raw_text,
                event_id=_safe_event_id(raw_record),
            ):
                stats.dead_letters += 1
            return 1

        event_time = parse_timestamp(normalized["event_time"])
        event_date = event_time.date().isoformat()
        is_late = self._is_late(event_time.date())
        event = {
            "event_id": str(normalized["event_id"]),
            "event_time": event_time.isoformat(),
            "event_date": event_date,
            "schema_name": self.schema.name,
            "schema_ver": self.schema.version,
            "payload": normalized,
            "extras": extras,
            "is_late": is_late,
            "source_file": source_file,
            "line_no": line_no,
        }
        if self.store.insert_event(event):
            stats.inserted += 1
            if is_late:
                stats.late += 1
        else:
            stats.duplicates += 1
        return 1

    def _commit_chunk(self, source_file, offset, line_no, chunk_index,
                      pending, crash_hook, crash_chunk):
        conn = self.store.conn
        # 数据与 checkpoint 在同一事务：BEGIN 延迟到第一条 DML，
        # save_checkpoint 只是同事务内的又一条 DML。
        self.store.save_checkpoint(source_file, offset, line_no)
        if crash_hook is not None and (crash_chunk is None or crash_chunk == chunk_index):
            crash_hook(Phase.BEFORE_COMMIT, chunk_index, pending)
        conn.commit()

    def full_rerun(self, path, chunk_size=500):
        """全量重跑：重置 checkpoint 后从头摄入，依靠主键去重避免重复。"""
        return self.ingest_file(path, chunk_size=chunk_size, reprocess=True)
from .schema import parse_timestamp
