"""入队语义：重复投递识别(DUPLICATE) vs 结构非法(REJECTED)，结论彼此可区分。"""
from __future__ import annotations

from app.models import EventStatus, RejectReason


def body(event_id="e1", **over):
    b = {
        "event_id": event_id,
        "aggregate_key": "acct:1",
        "event_type": "account.deposit",
        "payload": {"account": "alice", "amount": 100},
    }
    b.update(over)
    return b


def test_duplicate_is_distinct_from_invalid(pipeline, caplog):
    first = pipeline.enqueue(body())
    assert first.accepted is True
    assert first.reason is None
    assert first.status == EventStatus.PENDING

    dup = pipeline.enqueue(body())
    assert dup.accepted is False
    assert dup.reason == RejectReason.DUPLICATE          # 重放识别
    assert dup.status == EventStatus.PENDING             # 原事件状态不受影响
    assert "replay" in dup.detail.lower()

    invalid = pipeline.enqueue(body("e2", payload={"account": "alice", "amount": -1}))
    assert invalid.accepted is False
    assert invalid.reason == RejectReason.INVALID_PAYLOAD  # 结构非法
    assert invalid.status == EventStatus.REJECTED

    assert dup.reason != invalid.reason                  # 两类结论必须可区分

    logs = caplog.text
    assert "event=e1" in logs and "DUPLICATE" in logs
    assert "INVALID_PAYLOAD" in logs


def test_invalid_envelope_variants_are_rejected(pipeline):
    cases = [
        None,
        "not-an-object",
        {},
        {"event_id": "", "aggregate_key": "k", "event_type": "t", "payload": {}},
        {"event_id": "x", "aggregate_key": "k", "event_type": "t"},  # 缺 payload
        {"event_id": "x", "aggregate_key": "k", "event_type": "t", "payload": [1, 2]},
    ]
    for raw in cases:
        res = pipeline.enqueue(raw)
        assert res.accepted is False, raw
        assert res.reason == RejectReason.INVALID_ENVELOPE, raw
        assert res.status == EventStatus.REJECTED


def test_duplicate_does_not_overwrite_original(pipeline):
    pipeline.enqueue(body("e1"))
    # 即使重复提交内容不同，也按重放处理、不覆盖原记录
    pipeline.enqueue(body("e1", aggregate_key="acct:OTHER",
                          event_type="service.always_bad", payload={}))
    view = pipeline.get_status("e1")
    assert view.aggregate_key == "acct:1"
    assert view.event_type == "account.deposit"
