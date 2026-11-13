"""事件接入库：JSONL 解析、schema 演进、去重、迟到分区与断点续跑。"""

from .errors import (
    EventRouterError,
    SchemaError,
    IncompatibleSchemaError,
    NormalizationError,
    SimulatedCrash,
)
from .schema import FieldDef, Schema, parse_timestamp
from .store import EventStore
from .ingest import Ingester, IngestStats, Phase

__all__ = [
    "EventRouterError",
    "SchemaError",
    "IncompatibleSchemaError",
    "NormalizationError",
    "SimulatedCrash",
    "FieldDef",
    "Schema",
    "parse_timestamp",
    "EventStore",
    "Ingester",
    "IngestStats",
    "Phase",
]

__version__ = "1.0.0"
