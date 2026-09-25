"""至少一次投递 + 副作用仅一次：重复投递、重放识别、非法拒绝。"""

import asyncio
import logging

from event_pipeline.models import EventStatus, FailureKind, IngestOutcome
from event_pipeline.worker import EffectContext, EventRecord


def _recording_handler(calls: list[str]):
    async def handler(event: EventRecord, ctx: EffectContext):
        calls.append(event.event_id)
        await ctx.applied()  # 外部副作用在此生效
    return handler


def test_duplicate_publish_and_invalid_have_distinct_outcomes(pipeline_factory):
    calls: list[str] = []
    p = pipeline_factory(_recording_handler(calls))

    accepted = p.publish("e1", "A", {"v": 1})
    duplicate = p.publish("e1", "A", {"v": 2})
    invalid = p.publish("", "A", {"v": 3})

    assert accepted.outcome is IngestOutcome.ACCEPTED
    assert duplicate.outcome is IngestOutcome.DUPLICATE
    assert invalid.outcome is IngestOutcome.INVALID
    assert duplicate.reason != invalid.reason
    assert "重放" in duplicate.reason
    assert "event_id" in invalid.reason


def test_effect_applied_exactly_once_under_repeated_delivery(pipeline_factory):
    """同一 event_id 被重复投递（模拟重投），业务副作用只生效一次。"""
    calls: list[str] = []
    p = pipeline_factory(_recording_handler(calls))
    p.publish("e1", "A", {})

    # 模拟上游至少一次语义下的并发重复投递：两个协程同时驱动同一事件。
    async def scenario():
        await asyncio.gather(
            p.run_once(),
            p.run_once(),
        )
    asyncio.run(scenario())

    assert calls == ["e1"]
    event = p.get_event("e1")
    assert event.status is EventStatus.SUCCEEDED
    assert event.failure_kind is FailureKind.NONE
    assert p.effect_applied("e1") is True


def test_replay_after_success_does_not_reapply_side_effect(pipeline_factory):
    """事件成功后，再次出现同一 event_id 的重放（手动重建待办），直接命中幂等。"""
    calls: list[str] = []
    p = pipeline_factory(_recording_handler(calls))
    p.publish("e1", "A", {})
    asyncio.run(p.run_once())
    assert calls == ["e1"]

    # 模拟重放：存储层强行把同一事件重新置为可派发（等同重复投递进入系统）
    store = p.store
    import time
    now = time.time()
    store._conn  # ensure open
    with store._lock:
        store._conn.execute(
            "UPDATE events SET status='pending', attempts=9, available_at=?,"
            " lease_owner=NULL, lease_expires_at=NULL WHERE event_id='e1'",
            (now,),
        )
    asyncio.run(p.run_once())

    # 副作用没有第二次执行；结论为重放命中，状态仍成功
    assert calls == ["e1"]
    assert p.get_event("e1").status is EventStatus.SUCCEEDED
    assert p.effect_applied("e1") is True


def test_logs_contain_event_id_attempts_and_decision(pipeline_factory, caplog):
    calls: list[str] = []
    p = pipeline_factory(_recording_handler(calls))
    p.publish("evt-log", "A", {})
    with caplog.at_level(logging.INFO, logger="event_pipeline.worker"):
        asyncio.run(p.run_once())
    joined = "\n".join(r.getMessage() for r in caplog.records)
    assert "evt-log" in joined          # 事件标识
    assert "attempts=1/3" in joined     # 尝试次数
    assert "判定依据" in joined          # 判定依据
    assert "结论=成功" in joined
