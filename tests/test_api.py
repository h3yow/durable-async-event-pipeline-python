"""FastAPI 对外接口：保留健康检查；入队、重复/非法区分、状态可查询。"""

import asyncio
from contextlib import asynccontextmanager

from httpx import ASGITransport, AsyncClient

from event_pipeline.api import create_app
from event_pipeline.errors import NonRetryableError
from event_pipeline.models import RetryPolicy
from event_pipeline.pipeline import EventPipeline
from event_pipeline.worker import EffectContext, EventRecord


fast = RetryPolicy(max_attempts=2, base_delay=0.01, max_delay=0.02)


async def reject_handler(event: EventRecord, ctx: EffectContext):
    raise NonRetryableError("bad payload from api")


async def ok_handler(event: EventRecord, ctx: EffectContext):
    await ctx.applied()


@asynccontextmanager
async def serving(app):
    """手动进入 FastAPI lifespan（启动/关停 worker）并给出 httpx 客户端。"""
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://t") as client:
            yield client


def _build(tmp_path, name, handler):
    pipe = EventPipeline(tmp_path / name, handler=handler, retry_policy=fast,
                         lease_seconds=5, poll_interval=0.01)
    return create_app(pipe), pipe


def test_root_health_and_stats(tmp_path):
    app, pipe = _build(tmp_path, "api.db", ok_handler)

    async def scenario():
        async with serving(app) as c:
            r = await c.get("/")
            assert r.status_code == 200
            assert r.json()["Hello"] == "World"
            h = await c.get("/health")
            assert h.status_code == 200 and h.json()["status"] == "ok"
            s = await c.get("/stats")
            assert s.status_code == 200 and "states" in s.json()

    asyncio.run(scenario())
    pipe.close()


def test_publish_accepted_then_processed(tmp_path):
    app, pipe = _build(tmp_path, "api.db", ok_handler)

    async def scenario():
        async with serving(app) as c:
            r = await c.post("/events", json={
                "event_id": "e1", "aggregate_key": "A", "payload": {"v": 1}})
            assert r.status_code == 200
            body = r.json()
            assert body["outcome"] == "accepted"
            assert body["event"]["status"] == "pending"
            await pipe.wait_drained(2.0)
            g = await c.get("/events/e1")
            assert g.json()["status"] == "succeeded"
            assert g.json()["effect_applied"] is True

    asyncio.run(scenario())
    pipe.close()


def test_duplicate_409_distinct_from_invalid_422(tmp_path):
    app, pipe = _build(tmp_path, "api.db", ok_handler)

    async def scenario():
        async with serving(app) as c:
            await c.post("/events", json={
                "event_id": "dup", "aggregate_key": "A", "payload": {}})
            dup = await c.post("/events", json={
                "event_id": "dup", "aggregate_key": "A", "payload": {}})
            bad = await c.post("/events", json={
                "event_id": "", "aggregate_key": "A", "payload": {}})
            assert dup.status_code == 409
            assert dup.json()["detail"]["outcome"] == "duplicate"
            assert dup.json()["detail"]["existing_status"] == "pending"
            assert bad.status_code == 422

    asyncio.run(scenario())
    pipe.close()


def test_dead_letters_endpoint_lists_failure_reason(tmp_path):
    app, pipe = _build(tmp_path, "api2.db", reject_handler)

    async def scenario():
        async with serving(app) as c:
            await c.post("/events", json={
                "event_id": "d1", "aggregate_key": "A", "payload": {}})
            await pipe.wait_drained(2.0)
            r = await c.get("/dead-letters")
            assert r.status_code == 200
            items = r.json()["events"]
            assert len(items) == 1
            assert items[0]["event_id"] == "d1"
            assert items[0]["failure_kind"] == "non_retryable"
            assert "bad payload" in items[0]["last_error"]

    asyncio.run(scenario())
    pipe.close()


def test_state_filter_and_aggregate_query(tmp_path):
    app, pipe = _build(tmp_path, "api3.db", reject_handler)

    async def scenario():
        async with serving(app) as c:
            await c.post("/events", json={
                "event_id": "a1", "aggregate_key": "A", "payload": {}})
            await c.post("/events", json={
                "event_id": "a2", "aggregate_key": "A", "payload": {}})
            await pipe.wait_drained(2.0)
            r = await c.get("/events", params={"status": "dead"})
            ids = {e["event_id"] for e in r.json()["events"]}
            # a1 死信（终态）后 a2 越过它继续推进，二者都被处理
            assert ids == {"a1", "a2"}
            agg = await c.get("/aggregates/A/events")
            seq = [(e["event_id"], e["seq"], e["status"])
                   for e in agg.json()["events"]]
            assert seq == [("a1", 1, "dead"), ("a2", 2, "dead")]

    asyncio.run(scenario())
    pipe.close()
