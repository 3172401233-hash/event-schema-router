"""本地 schema 定义、字段类型转换与 schema 演进规则。

一个 schema 是一个 JSON/字典文档，例如::

    {
      "name": "user_action",
      "version": 1,
      "fields": [
        {"name": "event_id",   "type": "string", "required": true},
        {"name": "event_time", "type": "datetime", "required": true},
        {"name": "user_id",    "type": "int", "required": true},
        {"name": "amount",     "type": "float", "required": false},
        {"name": "tags",       "type": "json", "required": false}
      ]
    }

演进规则（新版本只允许向后兼容的变更，旧数据永远不需要重写）：

1. ``name`` 不可改变；``version`` 必须严格递增。
2. 已存在字段不可删除、不可改类型、不可重命名。
3. 必填字段不可以改成可选，可选字段也不可以改成必填
   （两种改法都会让旧版本写入的数据在新版本下含义发生变化）。
4. 只允许新增可选字段；新增字段若标记为必填会抛 IncompatibleSchemaError。
5. schema 中没有声明的字段属于“未知可选字段”，原样保留在 extras 中。
"""

import datetime as _dt
import json

from .errors import IncompatibleSchemaError, NormalizationError, SchemaError

SUPPORTED_TYPES = ("string", "int", "float", "bool", "datetime", "json")

# 事件接入库内部强依赖的两个字段，schema 中必须声明为必填。
SYSTEM_FIELDS = ("event_id", "event_time")


def parse_timestamp(value):
    """把 event_time 的各种写法统一转成带时区的 UTC datetime。

  支持：
    - 数字（秒级 epoch，int/float/纯数字字符串）
    - ``YYYY-MM-DD`` 日期字符串
    - ISO 8601 字符串（结尾的 ``Z`` 会被当作 UTC）
    - 原生 datetime（naive datetime 视为 UTC）
    """
    if isinstance(value, bool):
        # bool 是 int 的子类，时间戳不接受布尔值。
        raise ValueError("boolean is not a valid timestamp")
    if isinstance(value, (int, float)):
        return _dt.datetime.fromtimestamp(float(value), tz=_dt.timezone.utc)
    if isinstance(value, _dt.datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=_dt.timezone.utc)
        return value.astimezone(_dt.timezone.utc)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValueError("empty timestamp string")
        # 纯数字字符串按 epoch 处理，避免依赖上游是否加引号。
        try:
            return _dt.datetime.fromtimestamp(float(text), tz=_dt.timezone.utc)
        except ValueError:
            pass
        iso_text = text[:-1] + "+00:00" if text.endswith("Z") else text
        try:
            parsed = _dt.datetime.fromisoformat(iso_text)
        except ValueError:
            # 再退一步尝试仅日期格式。
            try:
                parsed = _dt.datetime.strptime(text, "%Y-%m-%d")
            except ValueError as exc:
                raise ValueError("unrecognized timestamp: {!r}".format(value)) from exc
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=_dt.timezone.utc)
        return parsed.astimezone(_dt.timezone.utc)
    raise ValueError("unrecognized timestamp type: {!r}".format(type(value).__name__))


def _coerce_string(value):
    if isinstance(value, str):
        return value
    if value is None:
        raise ValueError("None is not a string")
    # 数字/布尔被写成字符串或反过来时，用稳定的文本表示兼容。
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    raise ValueError("cannot convert {} to string".format(type(value).__name__))


def _coerce_int(value):
    if isinstance(value, bool):
        raise ValueError("boolean is not an int")
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if value.is_integer():
            return int(value)
        raise ValueError("float {} is not integral".format(value))
    if isinstance(value, str):
        text = value.strip()
        try:
            return int(text)
        except ValueError:
            return int(float(text))  # "12.0" 这种写法也兼容
    raise ValueError("cannot convert {} to int".format(type(value).__name__))


def _coerce_float(value):
    if isinstance(value, bool):
        raise ValueError("boolean is not a float")
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        return float(value.strip())
    raise ValueError("cannot convert {} to float".format(type(value).__name__))


def _coerce_bool(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        text = value.strip().lower()
        if text in ("true", "1", "yes", "on"):
            return True
        if text in ("false", "0", "no", "off"):
            return False
        raise ValueError("cannot convert string {!r} to bool".format(value))
    if isinstance(value, int) and value in (0, 1):
        return bool(value)
    raise ValueError("cannot convert {} to bool".format(type(value).__name__))


def _coerce_json(value):
    # 能被 JSON 序列化即可，原样保留结构（dict/list/数字/字符串等）。
    json.dumps(value, ensure_ascii=False)
    return value


def _coerce_datetime(value):
    # 归一化结果必须能 JSON 落库，datetime 统一转成带时区的 ISO8601 字符串。
    return parse_timestamp(value).isoformat()


_COERCERS = {
    "string": _coerce_string,
    "int": _coerce_int,
    "float": _coerce_float,
    "bool": _coerce_bool,
    "datetime": _coerce_datetime,
    "json": _coerce_json,
}


class FieldDef(object):
    """单个字段的声明。"""

    __slots__ = ("name", "type", "required")

    def __init__(self, name, field_type, required):
        self.name = name
        self.type = field_type
        self.required = bool(required)

    @classmethod
    def from_dict(cls, raw):
        if not isinstance(raw, dict):
            raise SchemaError("field definition must be an object: {!r}".format(raw))
        name = raw.get("name")
        field_type = raw.get("type")
        if not isinstance(name, str) or not name:
            raise SchemaError("field 'name' must be a non-empty string: {!r}".format(raw))
        if field_type not in SUPPORTED_TYPES:
            raise SchemaError(
                "field {!r} has unsupported type {!r}, allowed: {}".format(
                    name, field_type, ", ".join(SUPPORTED_TYPES)
                )
            )
        required = raw.get("required", False)
        if not isinstance(required, bool):
            raise SchemaError("field {!r} 'required' must be a boolean".format(name))
        return cls(name, field_type, required)

    def to_dict(self):
        return {"name": self.name, "type": self.type, "required": self.required}

    def __repr__(self):
        return "FieldDef(name={!r}, type={!r}, required={!r})".format(
            self.name, self.type, self.required
        )


class Schema(object):
    """一份不可变的本地 schema 定义。"""

    def __init__(self, name, version, fields):
        self.name = name
        self.version = int(version)
        self.fields = tuple(fields)
        self._field_map = {field.name: field for field in self.fields}
        self._validate()

    @classmethod
    def from_dict(cls, raw):
        if not isinstance(raw, dict):
            raise SchemaError("schema must be a JSON object")
        name = raw.get("name")
        version = raw.get("version")
        raw_fields = raw.get("fields")
        if not isinstance(name, str) or not name:
            raise SchemaError("schema 'name' must be a non-empty string")
        if not isinstance(version, int) or isinstance(version, bool) or version < 1:
            raise SchemaError("schema 'version' must be a positive integer")
        if not isinstance(raw_fields, list) or not raw_fields:
            raise SchemaError("schema 'fields' must be a non-empty list")
        fields = [FieldDef.from_dict(item) for item in raw_fields]
        names = [field.name for field in fields]
        if len(set(names)) != len(names):
            raise SchemaError("schema {} v{} has duplicate field names".format(name, version))
        return cls(name, version, fields)

    @classmethod
    def from_file(cls, path):
        with open(path, "r", encoding="utf-8") as handle:
            return cls.from_dict(json.load(handle))

    def to_dict(self):
        return {
            "name": self.name,
            "version": self.version,
            "fields": [field.to_dict() for field in self.fields],
        }

    def _validate(self):
        for system_field in SYSTEM_FIELDS:
            field = self._field_map.get(system_field)
            if field is None:
                raise SchemaError(
                    "schema {} v{} must declare field {!r}".format(
                        self.name, self.version, system_field
                    )
                )
            if not field.required:
                raise SchemaError(
                    "schema {} v{} field {!r} must be required".format(
                        self.name, self.version, system_field
                    )
                )

    def get_field(self, name):
        return self._field_map.get(name)

    def check_compatible_with(self, old):
        """校验 self（新版本）相对 old（旧版本）是否向后兼容。"""
        if old.name != self.name:
            raise IncompatibleSchemaError(
                "schema name cannot change: {!r} -> {!r}".format(old.name, self.name)
            )
        if self.version <= old.version:
            raise IncompatibleSchemaError(
                "schema version must increase: v{} -> v{}".format(old.version, self.version)
            )
        old_names = set(old._field_map)
        new_names = set(self._field_map)
        removed = old_names - new_names
        if removed:
            raise IncompatibleSchemaError(
                "v{} removes fields which would invalidate old data: {}".format(
                    self.version, sorted(removed)
                )
            )
        for name in old_names:
            old_field = old._field_map[name]
            new_field = self._field_map[name]
            if old_field.type != new_field.type:
                raise IncompatibleSchemaError(
                    "field {!r} type cannot change: {} -> {}".format(
                        name, old_field.type, new_field.type
                    )
                )
            if old_field.required != new_field.required:
                raise IncompatibleSchemaError(
                    "field {!r} required flag cannot change between versions".format(name)
                )
        added = new_names - old_names
        for name in added:
            if self._field_map[name].required:
                raise IncompatibleSchemaError(
                    "new field {!r} in v{} must be optional so old rows stay valid".format(
                        name, self.version
                    )
                )

    def normalize(self, record):
        """把一条原始 JSON 记录转成 (规范化字段 dict, extras dict)。

        未知可选字段原样进入 extras；任何必填缺失或类型转换失败都会抛
        NormalizationError，由上层写入死信。
        """
        if not isinstance(record, dict):
            raise NormalizationError(
                "record_not_object",
                {"actual_type": type(record).__name__},
            )
        normalized = {}
        coercion_errors = {}
        missing = []
        for field in self.fields:
            present = field.name in record and record[field.name] is not None
            if not present:
                if field.required:
                    missing.append(field.name)
                # 可选字段缺失或显式为 null：归一化结果里不放该键。
                continue
            try:
                normalized[field.name] = _COERCERS[field.type](record[field.name])
            except (ValueError, TypeError) as exc:
                coercion_errors[field.name] = str(exc)
        if missing:
            raise NormalizationError("missing_required", {"fields": sorted(missing)})
        if coercion_errors:
            raise NormalizationError("type_coercion", {"fields": coercion_errors})
        extras = {
            key: value
            for key, value in record.items()
            if key not in self._field_map
        }
        return normalized, extras

    def __repr__(self):
        return "Schema(name={!r}, version={!r}, fields={})".format(
            self.name, self.version, len(self.fields)
        )
