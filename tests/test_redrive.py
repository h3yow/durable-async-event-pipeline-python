"""死信重新驱动（redrive）：复活跑完、幂等去重、同维度保序、副作用不翻倍、
批量上限与审计可查、三种重新驱动结果可区分。

日志仍可看到 event=... / attempt=N / decision=...（含新增的 REDRIVE/SKIP/DUPLICATE）。
"""
from __future__ import annotations

import asyncio
import threading
import time
from dataclasses import replace
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from app.api import create_app
from app.errors import RetryableError
from app.handlers import EFF_ACCOUNT_DELTA
from app.models import EventStatus, RedriveFilter, RedriveOutcome
from app.pipeline import Pipeline
from tests.conftest import make_config, wait_until


class Downstream:
    """可开关的下游模拟：up=False 时抛可重试失败（下游故障），
    up=True 时记录完成顺序并登记副作用（下游已修复）。"""

    def __init__(self, effect_kind: str = "generic") -> None:
        self.up = True
        self.effect_kind = effect_kind
        self.completed: list[str] = []

    async def handle(self, payload, ctx):
        if not self.up:
            raise RetryableError(f"downstream unavailable for {ctx.event_id}")
        self.completed.append(ctx.event_id)
        if self.effect_kind == EFF_ACCOUNT_DELTA:
            ctx.record_effect(
                "credit",
                {"account": payload["account"], "amount": payload["amount"]},
                kind=EFF_ACCOUNT_DELTA,
            )
        else:
            ctx.record_effect("ds-done", {"event_id": ctx.event_id})


def enq(p, eid, key, etype="test.ds", payload=None):
    r = p.enqueue({
        "event_id": eid, "aggregate_key": key,
        "event_type": etype, "payload": payload or {},
    })
    assert r.accepted, r.detail
    return r


@pytest.fixture
def ds(registry):
    d = Downstream()
    registry.register("test.ds", d.handle)
    return d


@pytest.fixture
def rp(aloop, store, registry):
    """max_attempts=2：快速进入死信，便于测试。"""
    p = Pipeline(store, registry, make_config(max_attempts=2))
    aloop.run_until_complete(p.start())
    yield p
    aloop.run_until_complete(p.stop())


def wait_status(store, eid, status, aloop, timeout=3.0):
    ok = aloop.run_until_complete(
        wait_until(lambda: store.status_view(eid).status == status, timeout=timeout)
    )
    assert ok, f"{eid} 未在超时内进入 {status}"


# ---- 1. 成功复活并跑完 -------------------------------------------------------
def test_redrive_success_revives_dead_letter(rp, store, ds, aloop, caplog):
    ds.up = False
    enq(rp, "d1", "k1")
    wait_status(store, "d1", EventStatus.DEAD_LETTER, aloop)
    assert store.count_effects("d1") == 0

    ds.up = True  # 下游修复，运维发起重新驱动
    result = aloop.run_until_complete(
        rp.redrive(RedriveFilter(event_ids=("d1",)), batch_id="b1")
    )
    assert result.redriven == 1 and result.matched == 1 and not result.duplicate
    assert result.events[0].event_id == "d1" and result.events[0].redriven

    wait_status(store, "d1", EventStatus.COMPLETED, aloop)
    st = rp.redrive_status("d1")
    assert st is not None and st.outcome == RedriveOutcome.SUCCEEDED
    assert st.batch_id == "b1"
    assert store.count_effects("d1") == 1            # 副作用恰好一次

    recs = rp.list_redrive_records(event_id="d1")    # 审计可查
    assert len(recs) == 1 and recs[0].batch_id == "b1"
    assert "downstream unavailable" in recs[0].prior_error
    assert recs[0].redriven_at
    assert "decision=REDRIVE" in caplog.text         # 日志含判定依据
    assert "event=d1" in caplog.text


# ---- 2. 同一批次重复/并发发起只生效一次 --------------------------------------
def test_redrive_same_batch_id_replayed_only_once(rp, store, ds, aloop, caplog):
    ds.up = False
    enq(rp, "d2", "k2")
    wait_status(store, "d2", EventStatus.DEAD_LETTER, aloop)
    ds.up = True

    f = RedriveFilter(event_ids=("d2",))
    # 并发发起同一 batch_id（模拟多个入口同时操作）
    r1, r2 = aloop.run_until_complete(asyncio.gather(
        rp.redrive(f, batch_id="dup"), rp.redrive(f, batch_id="dup"),
    ))
    first, replay = sorted([r1, r2], key=lambda r: r.duplicate)
    assert first.duplicate is False and first.redriven == 1
    assert replay.duplicate is True and replay.redriven == 1   # 重放首次结果
    # 顺序再调用一次同批次：仍是重放，不再次驱动
    r3 = aloop.run_until_complete(rp.redrive(f, batch_id="dup"))
    assert r3.duplicate is True

    assert len(store.list_redrive_records(event_id="d2")) == 1  # 只被驱动一次
    wait_status(store, "d2", EventStatus.COMPLETED, aloop)
    assert store.count_effects("d2") == 1
    assert "decision=DUPLICATE" in caplog.text


def test_redrive_concurrent_batches_never_double_drive(aloop, store, registry, ds):
    # chunk=1 强制分批让出事件循环，最大化两个批次的交错
    cfg = replace(make_config(max_attempts=1), redrive_chunk_size=1)
    p = Pipeline(store, registry, cfg)
    aloop.run_until_complete(p.start())
    try:
        ds.up = False
        for i in range(6):
            enq(p, f"c{i}", "kc")
        aloop.run_until_complete(wait_until(
            lambda: len(store.list_dead_letters()) == 6, timeout=4.0))
        ds.up = True

        f = RedriveFilter(aggregate_key="kc")
        ra, rb = aloop.run_until_complete(asyncio.gather(
            p.redrive(f, batch_id="ba"), p.redrive(f, batch_id="bb"),
        ))
        assert ra.redriven + rb.redriven == 6        # 每个事件恰好被驱动一次
        for i in range(6):
            assert len(store.list_redrive_records(event_id=f"c{i}")) == 1
        aloop.run_until_complete(wait_until(
            lambda: store.count_effects() == 6, timeout=4.0))
        assert store.count_effects() == 6            # 副作用不翻倍
    finally:
        aloop.run_until_complete(p.stop())


# ---- 3. 同维度顺序不乱 -------------------------------------------------------
def test_redrive_returns_to_original_position(rp, store, ds, aloop):
    ds.up = False
    enq(rp, "old", "ko")
    wait_status(store, "old", EventStatus.DEAD_LETTER, aloop)

    aloop.run_until_complete(rp.stop())              # 停调度，制造“仍在排队”的事件
    enq(rp, "new1", "ko")
    enq(rp, "new2", "ko")
    seq_old = store.status_view("old").seq
    assert seq_old < store.status_view("new1").seq < store.status_view("new2").seq

    ds.up = True
    aloop.run_until_complete(
        rp.redrive(RedriveFilter(event_ids=("old",)), batch_id="ord"))
    aloop.run_until_complete(rp.start())

    wait_status(store, "new2", EventStatus.COMPLETED, aloop)
    # 重新驱动保留原 seq：old 回到队首，不排到后入队事件之后，也不被越过
    assert ds.completed == ["old", "new1", "new2"]
    assert store.status_view("old").seq == seq_old   # seq 不变 = 原处理次序


# ---- 4. 业务副作用不翻倍 -----------------------------------------------------
def test_redrive_side_effect_applied_exactly_once(aloop, store, registry):
    bank = Downstream(effect_kind=EFF_ACCOUNT_DELTA)
    bank.up = False
    registry.register("test.bank", bank.handle)
    p = Pipeline(store, registry, make_config(max_attempts=2))
    aloop.run_until_complete(p.start())
    try:
        enq(p, "pay1", "kb", etype="test.bank",
            payload={"account": "alice", "amount": 50})
        wait_status(store, "pay1", EventStatus.DEAD_LETTER, aloop)
        assert store.account_balance("alice") == 0

        bank.up = True
        r1 = aloop.run_until_complete(
            p.redrive(RedriveFilter(event_ids=("pay1",)), batch_id="pay-b"))
        r2 = aloop.run_until_complete(
            p.redrive(RedriveFilter(event_ids=("pay1",)), batch_id="pay-b"))
        assert r1.redriven == 1 and r2.duplicate

        wait_status(store, "pay1", EventStatus.COMPLETED, aloop)
        assert store.account_balance("alice") == 50   # 加款恰好一次
        assert store.count_effects("pay1", "credit") == 1
    finally:
        aloop.run_until_complete(p.stop())


# ---- 5. 批量上限、分批与审计/汇总 ---------------------------------------------
def test_redrive_batch_limit_truncation_and_audit(aloop, store, registry, ds):
    cfg = replace(make_config(max_attempts=1),
                  redrive_max_batch=3, redrive_chunk_size=2)
    p = Pipeline(store, registry, cfg)
    aloop.run_until_complete(p.start())
    try:
        ds.up = False
        for i in range(5):
            enq(p, f"b{i}", f"kb{i}")                # 不同维度，互不影响
        aloop.run_until_complete(wait_until(
            lambda: len(store.list_dead_letters()) == 5, timeout=4.0))
        ds.up = True

        r = aloop.run_until_complete(p.redrive(RedriveFilter(), batch_id="bulk"))
        assert r.matched == 3 and r.redriven == 3 and r.truncated  # 上限截断
        assert len(store.list_dead_letters()) == 2   # 剩余留待下一批

        r2 = aloop.run_until_complete(
            p.redrive(RedriveFilter(limit=1), batch_id="bulk-2"))
        assert r2.redriven == 1 and r2.truncated     # 显式 limit 也受约束
        r3 = aloop.run_until_complete(p.redrive(RedriveFilter(), batch_id="bulk-3"))
        assert r3.redriven == 1 and not r3.truncated

        assert len(store.list_redrive_records()) == 5   # 审计覆盖全部 5 条
        aloop.run_until_complete(wait_until(
            lambda: store.count_effects() == 5, timeout=4.0))
        s = p.redrive_summary()                          # 汇总可查
        assert s.total_batches == 3 and s.total_redriven == 5
        assert s.succeeded == 5 and s.failed_again == 0 and s.in_progress == 0
    finally:
        aloop.run_until_complete(p.stop())


def test_redrive_does_not_block_other_dimensions(aloop, store, registry, ds):
    cfg = replace(make_config(max_attempts=1), redrive_chunk_size=1)
    p = Pipeline(store, registry, cfg)
    aloop.run_until_complete(p.start())
    try:
        ds.up = False
        for i in range(10):
            enq(p, f"a{i}", "ka")
        aloop.run_until_complete(wait_until(
            lambda: len(store.list_dead_letters()) == 10, timeout=4.0))
        ds.up = True
        enq(p, "dep", "kb", etype="account.deposit",
            payload={"account": "x", "amount": 5})

        async def both():
            res = await p.redrive(RedriveFilter(aggregate_key="ka"), batch_id="big")
            # 大批量重新驱动分批让出事件循环：其它维度照常推进
            await wait_until(
                lambda: store.status_view("dep").status == EventStatus.COMPLETED,
                timeout=4.0)
            return res

        res = aloop.run_until_complete(both())
        assert res.redriven == 10
        assert store.account_balance("x") == 5
        aloop.run_until_complete(wait_until(
            lambda: store.count_effects() == 11, timeout=4.0))
    finally:
        aloop.run_until_complete(p.stop())


# ---- 6. 重新驱动结果三态可区分 -------------------------------------------------
def test_redrive_outcome_states_distinguishable(rp, store, ds, aloop):
    ds.up = False
    enq(rp, "s1", "ks")
    wait_status(store, "s1", EventStatus.DEAD_LETTER, aloop)

    aloop.run_until_complete(rp.stop())
    aloop.run_until_complete(rp.redrive(RedriveFilter(event_ids=("s1",)), batch_id="o1"))
    # 调度未运行：事件在 PENDING → “重新驱动后还在重试/排队”
    st = rp.redrive_status("s1")
    assert st.outcome == RedriveOutcome.IN_PROGRESS
    assert rp.redrive_status("never-driven") is None

    aloop.run_until_complete(rp.start())             # 下游仍故障 → 再次失败
    wait_status(store, "s1", EventStatus.DEAD_LETTER, aloop)
    st = rp.redrive_status("s1")
    assert st.outcome == RedriveOutcome.FAILED_AGAIN
    assert st.attempts == 2                          # 重新驱动后重新消耗重试预算

    ds.up = True                                     # 修复后再次驱动 → 成功
    aloop.run_until_complete(rp.redrive(RedriveFilter(event_ids=("s1",)), batch_id="o2"))
    wait_status(store, "s1", EventStatus.COMPLETED, aloop)
    st = rp.redrive_status("s1")
    assert st.outcome == RedriveOutcome.SUCCEEDED and st.batch_id == "o2"

    recs = rp.list_redrive_records(event_id="s1")    # 两次驱动都有审计
    assert [r.batch_id for r in recs] == ["o1", "o2"]
    s = rp.redrive_summary()
    assert s.total_batches == 2 and s.total_redriven == 2 and s.succeeded == 1


# ---- 7. HTTP 接口 -------------------------------------------------------------
@pytest.fixture
def api_env(store, registry):
    ds = Downstream()
    ds.up = False
    registry.register("test.ds", ds.handle)
    pipeline = Pipeline(store, registry, make_config(max_attempts=2))
    app = create_app(pipeline)

    loop = asyncio.new_event_loop()
    ready = threading.Event()

    def loop_thread():
        asyncio.set_event_loop(loop)
        loop.run_until_complete(pipeline.start())
        ready.set()
        loop.run_forever()

    t = threading.Thread(target=loop_thread, daemon=True)
    t.start()
    ready.wait(timeout=2)
    with TestClient(app) as c:
        try:
            yield SimpleNamespace(client=c, pipeline=pipeline, ds=ds)
        finally:
            fut = asyncio.run_coroutine_threadsafe(pipeline.stop(), loop)
            fut.result(timeout=3)
            loop.call_soon_threadsafe(loop.stop)
            t.join(timeout=2)
            loop.close()


def _poll(fn, timeout=3.0, interval=0.01):
    deadline = time.time() + timeout
    while time.time() < deadline:
        val = fn()
        if val:
            return val
        time.sleep(interval)
    return fn()


def test_redrive_api_flow(api_env):
    c, ds = api_env.client, api_env.ds
    r = c.post("/events", json={
        "event_id": "a1", "aggregate_key": "ka",
        "event_type": "test.ds", "payload": {},
    })
    assert r.status_code == 202
    assert _poll(lambda: c.get("/events/a1").json()["status"] == "DEAD_LETTER")

    ds.up = True                                     # 下游修复后发起重新驱动
    r = c.post("/dead-letters/redrive",
               json={"batch_id": "api-b1", "event_ids": ["a1"]})
    assert r.status_code == 202
    body = r.json()
    assert body["redriven"] == 1 and body["duplicate"] is False

    r = c.post("/dead-letters/redrive",              # 重复提交同批次 → 幂等重放
               json={"batch_id": "api-b1", "event_ids": ["a1"]})
    assert r.status_code == 200 and r.json()["duplicate"] is True

    out = _poll(lambda: (lambda b: b if b["outcome"] == "SUCCEEDED" else None)(
        c.get("/dead-letters/redrive/a1").json()))
    assert out["batch_id"] == "api-b1" and out["status"] == "COMPLETED"

    audit = c.get("/redrive-audit", params={"event_id": "a1"}).json()
    assert audit["count"] == 1
    assert audit["records"][0]["batch_id"] == "api-b1"
    assert audit["records"][0]["redriven_at"]

    summary = c.get("/redrive-summary").json()
    assert summary["total_batches"] == 1
    assert summary["total_redriven"] == 1
    assert summary["succeeded"] == 1

    assert c.get("/dead-letters/redrive/never-driven").status_code == 404
    assert c.get("/").json() == {"Hello": "World"}   # 既有接口不变


def test_redrive_api_validation(api_env):
    c = api_env.client
    assert c.post("/dead-letters/redrive", json={"limit": -1}).status_code == 422
    assert c.post("/dead-letters/redrive",
                  json={"event_ids": "not-a-list"}).status_code == 422
    assert c.post("/dead-letters/redrive",
                  json={"aggregate_key": 123}).status_code == 422
