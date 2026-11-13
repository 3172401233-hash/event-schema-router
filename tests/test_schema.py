import datetime as dt
import unittest

from event_router.errors import (
    IncompatibleSchemaError,
    NormalizationError,
    SchemaError,
)
from event_router.schema import Schema, parse_timestamp

from tests._helpers import BASE_SCHEMA, make_schema


class SchemaDefinitionTest(unittest.TestCase):
    def test_rejects_missing_system_fields(self):
        raw = {
            "name": "x",
            "version": 1,
            "fields": [{"name": "event_id", "type": "string", "required": True}],
        }
        with self.assertRaises(SchemaError):
            Schema.from_dict(raw)

    def test_rejects_optional_system_field(self):
        raw = {
            "name": "x",
            "version": 1,
            "fields": [
                {"name": "event_id", "type": "string", "required": True},
                {"name": "event_time", "type": "datetime", "required": False},
            ],
        }
        with self.assertRaises(SchemaError):
            Schema.from_dict(raw)

    def test_rejects_unknown_type_and_duplicate_fields(self):
        raw = {
            "name": "x",
            "version": 1,
            "fields": [
                {"name": "event_id", "type": "string", "required": True},
                {"name": "event_time", "type": "datetime", "required": True},
                {"name": "event_time", "type": "datetime", "required": True},
            ],
        }
        with self.assertRaises(SchemaError):
            Schema.from_dict(raw)


class CoercionTest(unittest.TestCase):
    def test_timestamp_formats(self):
        expected = dt.datetime(2026, 1, 2, 3, 4, 5, tzinfo=dt.timezone.utc)
        self.assertEqual(parse_timestamp("2026-01-02T03:04:05Z"), expected)
        self.assertEqual(
            parse_timestamp("2026-01-02T11:04:05+08:00"), expected
        )
        self.assertEqual(parse_timestamp("1767323045"), expected)
        self.assertEqual(parse_timestamp(1767323045), expected)
        self.assertEqual(
            parse_timestamp("2026-01-02"),
            dt.datetime(2026, 1, 2, tzinfo=dt.timezone.utc),
        )

    def test_numbers_written_as_strings_are_coerced(self):
        schema = make_schema()
        normalized, extras = schema.normalize(
            {
                "event_id": 1001,  # 数字 id 转字符串
                "event_time": "2026-01-02T03:04:05Z",
                "user_id": "42",
                "amount": "12.5",
            }
        )
        self.assertEqual(normalized["event_id"], "1001")
        self.assertEqual(normalized["user_id"], 42)
        self.assertEqual(normalized["amount"], 12.5)

    def test_unknown_optional_fields_kept_raw_in_extras(self):
        schema = make_schema()
        record = {
            "event_id": "e1",
            "event_time": "2026-01-02T03:04:05Z",
            "user_id": 1,
            "new_upstream_field": {"nested": [1, 2, 3]},
            "trace_id": "abc",
        }
        normalized, extras = schema.normalize(record)
        self.assertNotIn("new_upstream_field", normalized)
        self.assertEqual(
            extras["new_upstream_field"], {"nested": [1, 2, 3]}
        )
        self.assertEqual(extras["trace_id"], "abc")

    def test_missing_required_goes_to_error_with_reason(self):
        schema = make_schema()
        with self.assertRaises(NormalizationError) as ctx:
            schema.normalize({"event_id": "e1", "event_time": "2026-01-02"})
        self.assertEqual(ctx.exception.reason, "missing_required")
        self.assertEqual(ctx.exception.details["fields"], ["user_id"])

    def test_uncoercible_type_reports_field(self):
        schema = make_schema()
        with self.assertRaises(NormalizationError) as ctx:
            schema.normalize(
                {
                    "event_id": "e1",
                    "event_time": "2026-01-02",
                    "user_id": "not-a-number",
                }
            )
        self.assertEqual(ctx.exception.reason, "type_coercion")
        self.assertIn("user_id", ctx.exception.details["fields"])

    def test_non_object_record_rejected(self):
        schema = make_schema()
        with self.assertRaises(NormalizationError) as ctx:
            schema.normalize(["not", "an", "object"])
        self.assertEqual(ctx.exception.reason, "record_not_object")


class SchemaEvolutionTest(unittest.TestCase):
    def _v2(self, extra_field):
        raw = {
            "name": "user_action",
            "version": 2,
            "fields": BASE_SCHEMA["fields"] + [extra_field],
        }
        return Schema.from_dict(raw)

    def test_add_optional_field_is_compatible(self):
        v1 = make_schema()
        v2 = self._v2({"name": "channel", "type": "string", "required": False})
        v2.check_compatible_with(v1)  # 不抛异常即通过

    def test_add_required_field_rejected(self):
        v1 = make_schema()
        v2 = self._v2({"name": "channel", "type": "string", "required": True})
        with self.assertRaises(IncompatibleSchemaError):
            v2.check_compatible_with(v1)

    def test_remove_field_rejected(self):
        v1 = make_schema()
        raw = {
            "name": "user_action",
            "version": 2,
            "fields": BASE_SCHEMA["fields"][:-1],
        }
        v2 = Schema.from_dict(raw)
        with self.assertRaises(IncompatibleSchemaError):
            v2.check_compatible_with(v1)

    def test_type_change_rejected(self):
        v1 = make_schema()
        changed = [
            dict(field, type="string") if field["name"] == "user_id" else field
            for field in BASE_SCHEMA["fields"]
        ]
        v2 = Schema.from_dict(
            {"name": "user_action", "version": 2, "fields": changed}
        )
        with self.assertRaises(IncompatibleSchemaError):
            v2.check_compatible_with(v1)

    def test_name_and_version_change_rejected(self):
        v1 = make_schema()
        renamed = Schema.from_dict(dict(BASE_SCHEMA, name="other", version=2))
        with self.assertRaises(IncompatibleSchemaError):
            renamed.check_compatible_with(v1)
        same_version = make_schema()
        with self.assertRaises(IncompatibleSchemaError):
            same_version.check_compatible_with(v1)


if __name__ == "__main__":
    unittest.main()
