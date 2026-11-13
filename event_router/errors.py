"""库内统一的异常类型。"""


class EventRouterError(Exception):
    """所有 event_router 异常的基类。"""


class SchemaError(EventRouterError):
    """schema 定义本身非法。"""


class IncompatibleSchemaError(SchemaError):
    """新版本 schema 与已注册的旧版本不兼容。"""


class NormalizationError(EventRouterError):
    """单条记录无法归一化，reason 用于写入死信原因。

    reason 取值：
    - record_not_object: 顶层不是 JSON 对象
    - missing_required: 必填字段缺失（details.fields 列出字段名）
    - type_coercion: 字段类型无法转换（details.fields 给出逐字段原因）
    - serialization_error: json 类型字段无法被 JSON 序列化
    """

    def __init__(self, reason, details=None):
        self.reason = reason
        self.details = details or {}
        message = reason
        if details:
            message = "{}: {}".format(reason, details)
        super().__init__(message)


class SimulatedCrash(EventRouterError):
    """测试用：通过 crash_hook 模拟进程在事务提交前被 kill。"""
