"""并发与顺序：同聚合按入队顺序串行完成，不同聚合并发推进。"""

import asyncio

from event_pipeline.models import EventStatus
from event_pipeline.worker import EffectContext, EventRecord


def test_same_aggregate_processed_in_enqueue_order(pipeline_factory):
    order: list[str] = []
    gate = asyncio.Event()

    async def handler(event: EventRecord, ctx: EffectContext):
        # 第一条先挂住；若没有串行约束，后续事件会抢先完成
        if event.event_id == "a1":
            await gate.wait()
        order.append(event.event_id)
        await ctx.applied()

    p = pipeline_factory(handler, concurrency=4)
    for i in range(1, 6):
        p.publish(f"a{i}", "A", {})

    async def scenario():
        await p.start()
        await asyncio.sleep(0.15)  # 等 a1 被认领并挂起
        assert order == []
        gate.set()
        settled = await p.wait_drained(2.0)
        await p.stop()
        return settled

    assert asyncio.run(scenario()) is True
    assert order == ["a1", "a2", "a3", "a4", "a5"]
    assert [e.seq for e in p.list_by_aggregate("A")] == [1, 2, 3, 4, 5]
    assert all(
        e.status is EventStatus.SUCCEEDED for e in p.list_by_aggregate("A")
    )


def test_different_aggregates_advance_concurrently(pipeline_factory):
    active: set[str] = set()
    max_concurrent = {"n": 0}

    async def handler(event: EventRecord, ctx: EffectContext):
        active.add(event.aggregate_key)
        max_concurrent["n"] = max(max_concurrent["n"], len(active))
        await asyncio.sleep(0.1)
        active.discard(event.aggregate_key)
        await ctx.applied()

    p = pipeline_factory(handler, concurrency=8)
    for key in ["A", "B", "C", "D"]:
        p.publish(f"{key.lower()}1", key, {})

    async def scenario():
        await p.start()
        settled = await p.wait_drained(2.0)
        await p.stop()
        return settled

    assert asyncio.run(scenario()) is True
    # 4 个不同聚合必须能同时在途（若被串行化则最大并发为 1）
    assert max_concurrent["n"] == 4


def test_slow_aggregate_does_not_block_other_aggregates(pipeline_factory):
    async def slow(event: EventRecord, ctx: EffectContext):
        await asyncio.sleep(0.3)
        await ctx.applied()

    fast_done: list[str] = []

    async def fast(event: EventRecord, ctx: EffectContext):
        fast_done.append(event.event_id)
        await ctx.applied()

    async def route(event, ctx):
        await (slow if event.aggregate_key == "SLOW" else fast)(event, ctx)

    p = pipeline_factory(route, concurrency=4)
    p.publish("s1", "SLOW", {})
    p.publish("s2", "SLOW", {})
    p.publish("f1", "FAST", {})

    async def scenario():
        await p.start()
        # 快速聚合远早于慢聚合完成
        await asyncio.sleep(0.15)
        fast_finished = p.get_event("f1").status is EventStatus.SUCCEEDED
        await p.wait_drained(2.0)
        await p.stop()
        return fast_finished

    assert asyncio.run(scenario()) is True
    assert fast_done == ["f1"]


def test_states_are_queryable_during_processing(pipeline_factory):
    """处理过程中全部状态流转可查询，且不破坏接口可用性。"""
    seen: set[str] = set()

    async def handler(event: EventRecord, ctx: EffectContext):
        seen.add(event.status.value)  # 被认领时应为 running
        await ctx.applied()

    p = pipeline_factory(handler)
    p.publish("a1", "A", {})
    asyncio.run(p.run_once())
    assert "running" in seen
    assert set(p.stats().keys()) == {s.value for s in EventStatus}
    assert p.stats()["succeeded"] == 1
