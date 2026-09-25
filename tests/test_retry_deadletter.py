"""失败重试与死信：可解释退避、次数上限、失败原因留存且彼此可区分。"""

import asyncio
import logging

from event_pipeline.errors import NonRetryableError, RetryableError
from event_pipeline.models import EventStatus, FailureKind
from event_pipeline.worker import EffectContext, EventRecord


def test_retryable_failures_succeed_after_backoff(pipeline_factory, caplog):
    attempts = {"n": 0}

    async def handler(event: EventRecord, ctx: EffectContext):
        attempts["n"] += 1
        if event.attempts < 3:
            raise RetryableError("temporary downstream error")
        await ctx.applied()

    p = pipeline_factory(handler)
    p.publish("e1", "A", {})

    async def scenario():
        with caplog.at_level(logging.WARNING, logger="event_pipeline.worker"):
            await p.start()
            drained = await p.wait_drained(3.0)
            await p.stop()
        return drained

    assert asyncio.run(scenario()) is True
    assert attempts["n"] == 3
    e = p.get_event("e1")
    assert e.status is EventStatus.SUCCEEDED
    assert e.attempts == 3
    retry_logs = [r.getMessage() for r in caplog.records if "等待重试" in r.getMessage()]
    assert len(retry_logs) == 2
    assert all("指数退避" in m and "temporary downstream error" in m for m in retry_logs)


def test_non_retryable_goes_dead_immediately_with_reason(pipeline_factory):
    async def handler(event: EventRecord, ctx: EffectContext):
        raise NonRetryableError("payload 缺少必填字段 order_id")

    p = pipeline_factory(handler)
    p.publish("e1", "A", {})
    asyncio.run(p.run_once())

    e = p.get_event("e1")
    assert e.status is EventStatus.DEAD
    assert e.attempts == 1
    assert e.failure_kind is FailureKind.NON_RETRYABLE
    assert "order_id" in e.last_error
    assert [x.event_id for x in p.list_dead_letters()] == ["e1"]


def test_exhausting_attempts_dead_with_distinct_kind(pipeline_factory, fast_policy):
    async def handler(event: EventRecord, ctx: EffectContext):
        raise RetryableError("always broken")

    p = pipeline_factory(handler)
    p.publish("e1", "A", {})

    async def scenario():
        await p.start()
        await p.wait_drained(3.0)
        await p.stop()

    asyncio.run(scenario())
    e = p.get_event("e1")
    assert e.status is EventStatus.DEAD
    assert e.failure_kind is FailureKind.MAX_ATTEMPTS
    assert e.attempts == fast_policy.max_attempts
    assert "重试次数耗尽" in e.last_error
    assert FailureKind.MAX_ATTEMPTS != FailureKind.NON_RETRYABLE


def test_unexpected_error_is_treated_as_retryable(pipeline_factory):
    async def handler(event: EventRecord, ctx: EffectContext):
        raise RuntimeError("boom")

    p = pipeline_factory(handler)
    p.publish("e1", "A", {})
    asyncio.run(p.run_once())
    e = p.get_event("e1")
    assert e.status is EventStatus.FAILED
    assert e.failure_kind is FailureKind.RETRYABLE
    assert "boom" in e.last_error


def test_failed_event_does_not_block_other_aggregates(pipeline_factory):
    """单个维度长时间失败，不得阻塞其他维度推进。"""
    async def broken(event: EventRecord, ctx: EffectContext):
        raise RetryableError("A is stuck")

    done: list[str] = []

    async def healthy(event: EventRecord, ctx: EffectContext):
        done.append(event.event_id)
        await ctx.applied()

    async def route(event, ctx):
        if event.aggregate_key == "A":
            await broken(event, ctx)
        else:
            await healthy(event, ctx)

    p = pipeline_factory(route)
    p.publish("a1", "A", {}, max_attempts=10_000)  # 长时间失败，不会很快进死信
    p.publish("a2", "A", {})
    p.publish("b1", "B", {})

    async def scenario():
        await p.start()
        # 只等待当前到点批次安定（a1 处于退避中即视为本批安定）
        settled = await p.wait_drained(2.0, include_failed=False)
        await p.stop(drain=True)
        return settled

    assert asyncio.run(scenario()) is True
    assert done == ["b1"]
    assert p.get_event("a1").status is EventStatus.FAILED
    # a2 因同聚合顺序约束等待 a1，符合"同维度串行"
    assert p.get_event("a2").status is EventStatus.PENDING
    assert p.get_event("b1").status is EventStatus.SUCCEEDED