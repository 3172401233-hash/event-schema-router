# event-schema-router

Python 3.8 标准库实现的事件接入库。输入分块 JSONL 文件与本地 schema 定义，
输出规范化事件、死信记录和可断点续跑的 checkpoint，全部落在一个 SQLite
数据库中，不依赖任何第三方包或外部服务。

## 能力概览

- **容错解析**：坏 JSON、非法编码、顶层不是对象、必填缺失、类型无法转换
  的记录进入死信并标注原因，不再让整批任务失败。
- **宽松类型转换**：上游把数字写成字符串（`"42"`、`"12.5"`）等常见漂移
  会按 schema 自动转换；`event_time` 支持 epoch 数字、纯数字字符串、
  `YYYY-MM-DD` 与 ISO 8601（含结尾 `Z`）。
- **未知字段保留**：schema 未声明的字段原样保存在 `events.extras`
  （JSON 文本），上游加字段不需要改库。
- **幂等去重**：`event_id` 是事件主键，重复送达（含全量重跑）只入库一次。
- **日期分区**：按 `event_time` 的 UTC 日期写入 `event_date` 分区键；
  乱序、晚到的数据都进入各自日期分区。
- **迟到窗口**：超过保留窗口（默认 7 天）的事件照常入库，仅以
  `is_late=1` 单独标记。
- **崩溃一致性**：每个 chunk 的数据写入与 checkpoint 推进在同一个
  SQLite 事务内提交，模拟 kill 后重启不丢数据、不重复提交。

## 目录结构

```
event_router/
  errors.py     # 异常类型（含死信 reason 约定、SimulatedCrash）
  schema.py     # Schema/FieldDef、类型转换、时间解析、版本兼容规则
  store.py      # SQLite 表结构与所有持久化操作
  ingest.py     # 分块 JSONL 读取、事务边界、迟到判定、checkpoint
tests/          # unittest 用例，全部使用临时文件与临时 SQLite
```

## 快速开始

```python
from event_router import EventStore, Ingester, Schema

schema = Schema.from_file("schema/user_action.v1.json")
store = EventStore("data/events.db")
store.register_schema(schema)          # 登记版本，重复登记幂等

ingester = Ingester(store, schema, retention_days=7)
stats = ingester.ingest_file("data/events.jsonl", chunk_size=500)
print(stats)                           # inserted / duplicates / dead_letters / late

# 全量重跑：清 checkpoint 从头读，事件主键保证不重复计数
ingester.full_rerun("data/events.jsonl")
store.close()
```

## Schema 规则

schema 是一个本地 JSON 文档：

```json
{
  "name": "user_action",
  "version": 1,
  "fields": [
    {"name": "event_id",   "type": "string",   "required": true},
    {"name": "event_time", "type": "datetime", "required": true},
    {"name": "user_id",    "type": "int",      "required": true},
    {"name": "amount",     "type": "float",    "required": false},
    {"name": "note",       "type": "string",   "required": false},
    {"name": "tags",       "type": "json",     "required": false}
  ]
}
```

### 字段类型

| 类型 | 转换规则 |
| --- | --- |
| `string` | 字符串原样；数字/布尔转稳定文本（`42` -> `"42"`） |
| `int` | 整型、整值浮点、纯数字字符串（含 `"12.0"`） |
| `float` | 数字与数字字符串 |
| `bool` | 布尔；`"true"/"false"/"1"/"0"/"yes"/"no"/"on"/"off"` |
| `datetime` | epoch（数字或字符串）、ISO 8601、`YYYY-MM-DD`，统一输出 UTC ISO 字符串 |
| `json` | 任意可 JSON 序列化结构，原样保留 |

显式 `null` 与可选字段缺失等价：归一化结果中不出现该键。`event_id`、
`event_time` 为系统字段，必须声明且必填。

### 演进规则（新版本不得要求重写旧数据）

1. `name` 不可变，`version` 必须严格递增。
2. 已有字段不可删除、不可改类型、不可改 `required` 标记。
3. 只允许**新增可选字段**；新增必填字段会在 `register_schema` 时抛
   `IncompatibleSchemaError`。
4. 每个事件在 `events` 中保留写入时的 `schema_ver` 与完整 payload，
   旧记录永远按旧版本原样可读，升级后不需要数据迁移。
5. 相同版本号只能注册相同定义，否则同样报 `IncompatibleSchemaError`。

### 死信原因（`dead_letters.reason`）

| reason | 含义 |
| --- | --- |
| `invalid_encoding` | 字节序列不是合法 UTF-8 |
| `invalid_json` | JSON 解析失败，details 含解析器报错与位置 |
| `record_not_object` | 顶层 JSON 不是对象（如数组、标量） |
| `missing_required` | 必填字段缺失或为 null，details.fields 列出字段名 |
| `type_coercion` | 类型转换失败，details.fields 给出逐字段原因 |

## SQLite 表结构

库开启 `journal_mode=WAL` 与 `synchronous=NORMAL`。

### `events`（规范化事件，按 event_id 幂等）

| 列 | 说明 |
| --- | --- |
| `event_id` (PK) | 事件唯一标识，重复送达靠 `INSERT OR IGNORE` 去重 |
| `event_time` | UTC ISO 8601 时间 |
| `event_date` | 日期分区键 `YYYY-MM-DD`（UTC），查询分区走 `idx_events_date` |
| `schema_name` / `schema_ver` | 写入时使用的 schema 版本 |
| `payload` | 规范化后的全部声明字段（JSON 文本） |
| `extras` | 未知可选字段的原始内容（JSON 文本） |
| `is_late` | 1 表示事件日期早于 `today - retention_days` |
| `source_file` / `line_no` | 溯源：来源文件与物理行号 |
| `created_at` | 入库时间（UTC） |

### `dead_letters`（失败记录）

列为 `id`、`event_id`（尽力提取，可能为空）、`source_file`、`line_no`、
`offset`、`reason`、`details`（JSON）、`raw_line`（原文）、`created_at`。
`UNIQUE(source_file, line_no)` 保证全量重跑不会堆积重复死信。

### `checkpoints`（断点）

| 列 | 说明 |
| --- | --- |
| `source_file` (PK) | 来源文件名 |
| `offset` | 下一条待读的**字节偏移**（二进制读取，多字节 UTF-8 安全） |
| `line_no` | 下一条待读物理行号 |
| `updated_at` | 最近一次推进时间 |

### `schema_versions`

`name` + `version` 唯一，`definition` 保存注册时的完整 schema JSON，
用于持久化演进校验与审计。

## 事务与恢复方式

摄入以 `chunk_size`（默认 500 物理行）为单位提交：

1. 打开文件后 `seek` 到 checkpoint 的字节偏移，跳过已提交内容。
2. chunk 内每条记录的 `INSERT`（事件或死信）与最后的 checkpoint
   `UPSERT` 在**同一个事务**里执行，然后一次 `commit`。
3. 进程在 commit 前被 kill：SQLite 回滚整个事务，数据和 checkpoint
   都不落盘；重启后从上一个已提交偏移重新读取，而事件主键去重保证
   重新处理也不会重复入库。
4. commit 成功后崩溃：checkpoint 与数据同时持久化，重启从新偏移继续。
5. 文件追加新数据后直接再次运行即可，只读取新增字节。
6. 全量重跑（`full_rerun` / `reprocess=True`）先删除该文件的
   checkpoint 再从头扫描；`events` 主键与 `dead_letters` 唯一键保证
   计数不翻倍。

测试中的 `crash_hook(phase, chunk_index, pending)` 会在 commit 前抛出
`SimulatedCrash`，用于确定性地复现“事务中断 -> 关闭连接 -> 重开续跑”。

## 运行测试

```bash
python -m unittest discover -s tests -v
```

若环境里只有 `python3`（如本机），用 `python3 -m unittest discover -s tests -v`
等价。测试只使用 `tempfile` 下的临时 JSONL 与临时 SQLite，结束自动清理，
覆盖：schema 升级（兼容/不兼容/幂等注册）、坏 JSON 与坏记录、重复事件、
乱序日期分区、窗口内外迟到数据、事务中断与重启续跑、反复崩溃、全量重跑。
