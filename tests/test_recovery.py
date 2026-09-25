"""处理中断与进程重启恢复：未完成事件回到确定状态，不丢失、不重复产生副作用。"""

import asyncio
import time

from event_pipeline.models import EventStatus, FailureKind
from event_pipeline.storage import EventStore
from event_pipeline.models import RetryPolicy
from event_pipeline.worker import EventWorker
from event_pipeline.worker import EffectContext, EventRecord


def _open(db_path, handler, lease: float = 0.05):
    """每个"进程"用独立 EventWorker（独立 owner/事件循环），共享同一磁盘数据库。"""
    store = EventStore(db_path)
    policy = RetryPolicy(max_attempts=5, base_delay=0.01, backoff_factor=2.0,
                         max_delay=0.05, jitter=0.0)
    worker = EventWorker(store, handler, policy,
                         lease_seconds=lease, poll_interval=0.01)
    return store, worker


def test_crash_before_effect_event_retried_and_completed(db_path):
    """进程在副作用生效前崩溃 -> 租约恢复为 failed(crash) -> 重试至成功，不丢事件。"""
    attempts = {"n": 0}

    async def crashing(event: EventRecord, ctx: EffectContext):
        attempts["n"] += 1
        # 第一次：副作用尚未 applied，进程直接"死掉"（不标记任何状态）
        if event.attempts == 1:
            raise SystemExit  # 模拟进程退出，RUNNING 租约残留

    async def completing(event: EventRecord, ctx: EffectContext):
        attempts["n"] += 1
        await ctx.applied()

    # 进程 1：认领后崩溃
    store1, w1 = _open(db_path, crashing)
    store1.enqueue("e1", "A", {}, 5)

    async def run1():
        await w1.run_once()

    try:
        asyncio.run(run1())
    except SystemExit:
        pass
    assert store1.get("e1").status is EventStatus.RUNNING
    store1.close()

    # 磁盘上残留 RUNNING；等待租约过期
    time.sleep(0.08)

    # 进程 2：重新打开，先恢复再处理，事件被重试完成
    effects: list[str] = []

    async def completing2(event: EventRecord, ctx: EffectContext):
        attempts["n"] += 1
        effects.append(event.event_id)
        await ctx.applied()

    store2, w2 = _open(db_path, completing2)
    asyncio.run(w2.run_once())  # 本轮：恢复 running->failed(crash)
    asyncio.run(w2.run_once())  # 下一轮：重试处理

    e = store2.get("e1")
    assert e.status is EventStatus.SUCCEEDED
    assert effects == ["e1"]
    assert store2.effect_applied("e1") is True
    assert e.attempts == 2
    store2.close()


def test_crash_after_effect_no_duplicate_side_effect_on_recovery(db_path):
    """进程在副作用生效后、标记成功前崩溃 -> 恢复重试时命中幂等，副作用不重复。"""
    store1, w1 = _open(db_path, _handler_applied_then_die)
    store1.enqueue("e1", "A", {}, 5)

    try:
        asyncio.run(w1.run_once())
    except SystemExit:
        pass

    e = store1.get("e1")
    assert e.status is EventStatus.RUNNING
    assert store1.effect_applied("e1") is True  # 副作用已落盘
    store1.close()

    time.sleep(0.08)

    calls: list[str] = []

    async def replay_handler(event: EventRecord, ctx: EffectContext):
        calls.append(event.event_id)
        await ctx.applied()  # 应检测到已 applied 而跳过业务

    store2, w2 = _open(db_path, replay_handler)
    asyncio.run(w2.run_once())
    asyncio.run(w2.run_once())

    e2 = store2.get("e1")
    assert e2.status is EventStatus.SUCCEEDED
    assert calls == []  # 业务体因重放未执行
    assert store2.effect_applied("e1") is True
    store2.close()


async def _handler_applied_then_die(event: EventRecord, ctx: EffectContext):
    await ctx.applied()  # 副作用已持久登记
    raise SystemExit     # 但还没来得及 mark_succeeded 进程就崩溃


def test_recovery_marks_crash_kind_and_logs_reason(db_path, caplog):
    import logging

    async def h(event, ctx):
        await ctx.applied()

    store, w = _open(db_path, h, lease=0.02)
    store.enqueue("e1", "A", {}, 5)
    now = time.time()
    store.claim("e1", "dead-worker", 0.02, now)
    expiry = now + 0.03  # 租约已过期但早于下一次到点，本轮只做恢复

    with caplog.at_level(logging.WARNING, logger="event_pipeline.worker"):
        recovered = w.recover_stuck(expiry)
    assert recovered == 1

    e = store.get("e1")
    assert e.status is EventStatus.FAILED
    assert e.failure_kind is FailureKind.CRASH
    joined = "\n".join(r.getMessage() for r in caplog.records)
    assert "e1" in joined and "租约超时" in joined
    store.close()
