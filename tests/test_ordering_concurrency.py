"""同聚合串行保序、跨聚合并发、单维度失败不阻塞其他维度。"""
from __future__ import annotations

import asyncio
import itertools
import time

from app.errors import RetryableError
from app.handlers import HandlerRegistry
from app.models import EventStatus
from app.pipeline import Pipeline
from tests.conftest import make_config, wait_until


# 记录每个事件的“完成顺序”与完成时刻
_finish_log: list[str] = []
_start_log: list[str] = []
_seq = itertools.count()


async def ordered_handler(payload, ctx):
    n = next(_seq)
    _start_log.append(ctx.event_id)
    await asyncio.sleep(payload.get("sleep", 0.02))  # 让出循环，制造交错机会
    _finish_log.append(ctx.event_id)


async def flaky_ordered_handler(payload, ctx):
    # 第一次慢且失败（退避），第二次很快成功：用于检验它会阻塞同维度后续事件
    if ctx.attempt == 1:
        await asyncio.sleep(0.05)
        raise RetryableError("boom on first try")
    _finish_log.append(ctx.event_id)


def build_pipeline(store):
    reg = HandlerRegistry()
    reg.register("ordered", ordered_handler)
    reg.register("flaky.ordered", flaky_ordered_handler)
    return Pipeline(store, reg, make_config(max_attempts=3))


def setup_function(_):
    _finish_log.clear()
    _start_log.clear()


def enq(p, eid, key, etype="ordered", **payload):
    p.enqueue({"event_id": eid, "aggregate_key": key,
               "event_type": etype, "payload": payload})


def test_same_aggregate_processed_in_enqueue_order(store, aloop):
    p = build_pipeline(store)
    aloop.run_until_complete(p.start())
    try:
        for i in range(5):
            enq(p, f"A{i}", "KEY-A", sleep=0.01)

        async def done():
            await wait_until(
                lambda: len(_finish_log) == 5
                and all(store.status_view(f"A{i}").status == EventStatus.COMPLETED
                        for i in range(5)),
                timeout=3.0,
            )

        aloop.run_until_complete(done())
    finally:
        aloop.run_until_complete(p.stop())
    # 严格按入队顺序完成（即使每个处理器都 sleep 让出循环）
    assert _finish_log == [f"A{i}" for i in range(5)]


def test_different_aggregates_progress_concurrently(store, aloop):
    p = build_pipeline(store)
    aloop.run_until_complete(p.start())
    try:
        enq(p, "B0", "KEY-B", sleep=0.1)
        enq(p, "C0", "KEY-C", sleep=0.1)
        enq(p, "D0", "KEY-D", sleep=0.1)
        t0 = time.monotonic()

        async def done():
            await wait_until(lambda: len(_finish_log) == 3, timeout=3.0)

        aloop.run_until_complete(done())
        elapsed = time.monotonic() - t0
    finally:
        aloop.run_until_complete(p.stop())
    # 三个维度并发：总耗时接近单个 sleep，而非 3 倍串行
    assert elapsed < 0.25, elapsed
    assert set(_finish_log) == {"B0", "C0", "D0"}


def test_failing_aggregate_does_not_block_others(store, aloop):
    p = build_pipeline(store)
    aloop.run_until_complete(p.start())
    try:
        # KEY-X 队头事件先失败后退避：阻塞 X 的后续，但不影响 KEY-Y
        enq(p, "X0", "KEY-X", etype="flaky.ordered")
        enq(p, "X1", "KEY-X", sleep=0.0)
        enq(p, "Y0", "KEY-Y", sleep=0.01)

        async def y_done():
            await wait_until(
                lambda: store.status_view("Y0").status == EventStatus.COMPLETED,
                timeout=3.0,
            )

        aloop.run_until_complete(y_done())
        # Y0 已完成时，X0 仍在重试、X1 因队头阻塞尚未完成
        assert store.status_view("X0").status in (
            EventStatus.IN_PROGRESS, EventStatus.RETRY_WAIT)
        assert store.status_view("X1").status in (
            EventStatus.PENDING, EventStatus.IN_PROGRESS)
        assert _finish_log == ["Y0"]

        async def x_done():
            await wait_until(
                lambda: all(store.status_view(e).status == EventStatus.COMPLETED
                            for e in ("X0", "X1")),
                timeout=3.0,
            )

        aloop.run_until_complete(x_done())
        # 退避结束后，X 维度仍严格按序：X0 先于 X1
        assert _finish_log.index("X0") < _finish_log.index("X1")
    finally:
        aloop.run_until_complete(p.stop())
