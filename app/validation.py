"""事件信封与负载的结构校验：非法事件在入队时即被拒绝（REJECTED）。

校验分两层，结论可区分：
    * 信封校验 INVALID_ENVELOPE：外层字段缺失/类型错误；
    * 负载校验 INVALID_PAYLOAD：payload 非对象，或已知事件类型的字段约束不满足。
"""
from __future__ import annotations

from typing import Any

from app.models import EnqueueRequest


class ValidationError(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


def _require_nonempty_str(raw: Any, field: str) -> str:
    if not isinstance(raw, str) or not raw.strip():
        raise ValidationError(f"field '{field}' must be a non-empty string")
    return raw


def validate_envelope(raw: Any) -> EnqueueRequest:
    """校验外层字段（event_id/aggregate_key/event_type/payload）。"""
    if not isinstance(raw, dict):
        raise ValidationError("request body must be a JSON object")
    event_id = _require_nonempty_str(raw.get("event_id"), "event_id")
    aggregate_key = _require_nonempty_str(raw.get("aggregate_key"), "aggregate_key")
    event_type = _require_nonempty_str(raw.get("event_type"), "event_type")
    if "payload" not in raw:
        raise ValidationError("field 'payload' is required")
    payload = raw["payload"]
    if not isinstance(payload, dict):
        raise ValidationError("field 'payload' must be a JSON object")
    return EnqueueRequest(
        event_id=event_id,
        aggregate_key=aggregate_key,
        event_type=event_type,
        payload=payload,
    )


# 已知事件类型的负载字段约束（未知类型仅要求 payload 为对象，保持开放）。
_PAYLOAD_RULES: dict[str, dict[str, type]] = {
    "account.deposit": {"account": str, "amount": (int, float)},  # type: ignore[dict-item]
    "service.flaky": {"fail_times": int},
}


def validate_payload(req: EnqueueRequest) -> None:
    """校验已知事件类型的 payload 字段；失败抛 ValidationError。"""
    rules = _PAYLOAD_RULES.get(req.event_type)
    if rules is None:
        return
    for name, expected in rules.items():
        if name not in req.payload:
            raise ValidationError(f"payload field '{name}' is required for {req.event_type}")
        value = req.payload[name]
        if not isinstance(value, expected) or isinstance(value, bool):
            raise ValidationError(
                f"payload field '{name}' for {req.event_type} has wrong type"
            )
        if name == "amount" and value <= 0:
            raise ValidationError("payload field 'amount' must be > 0")
        if name == "fail_times" and value < 0:
            raise ValidationError("payload field 'fail_times' must be >= 0")
