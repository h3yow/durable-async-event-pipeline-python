"""死信重新驱动：复活跑完、重复/并发只生效一次、顺序不乱、副作用不翻倍、
批量上限与审计可查、结果分类（成功/重试中/再次失败）。"""
from __future__ import annotations

import dataclasses
import threading

import pytest

from app.errors import RetryableError
from app.handlers import HandlerRegistry
from app.models import EventStatus, RedriveOutcome
from app.pipeline import Pipeline
from tests.conftest import make_config, wait_until


# ---- 测试专用处理器 ----------------------------------------------------------
_flags = {"fail": True}
_completed: list[str] = []


async def sometimes_handler(payload, ctx):
    """_flags['fail'] 为真时可重试失败（模拟下游故障），修复后成功。"""
    if _flags["fail"]:
        raise RetryableError("downstream still broken")
    ctx.record_effect("recovered", {"event": ctx.event_id})
    _completed.append(ctx.event_id)


async def always_retryable_handler(payload, ctx):
    """永远可重试失败（修复前重新驱动会再次进死信）。"""
    raise RetryableError("still broken")


async def deposit_flaky_handler(payload, ctx):
    """故障期可重试失败；恢复后按 (event_id,'deposit') 幂等键加款。"""
    if _flags["fail"]:
        raise RetryableError("bank downstream broken")
    ctx.record_effect(
        "deposit",
        {"account": payload["account"], "amount": payload["amount"]},
        kind="account.delta",
    )


async def recorder_handler(payload, ctx):
    _completed.append(ctx.event_id)
    ctx.record_effect("seen", {"event": ctx.event_id})


def build_pipeline(store, **cfg_overrides):
    reg = HandlerRegistry()
    reg.register("test.sometimes", sometimes_handler)
    reg.register("test.always_retryable", always_retryable_handler)
    reg.register("test.deposit_flaky", deposit_flaky_handler)
    reg.register("test.recorder", recorder_handler)
    cfg = make_config(max_attempts=2)
    if cfg_overrides:
        cfg = dataclasses.replace(cfg, **cfg_overrides)
    return Pipeline(store, reg, cfg)


def setup_function(_):
    _flags["fail"] = True
    _completed.clear()


def enq(p, eid, etype, payload=None, key="acct:rd"):
    p.enqueue({
        "event_id": eid,
        "aggregate_key": key,
        "event_type": etype,
        "payload": payload or {},
    })


def wait_status(aloop, store, eid, status, timeout=4.0):
    async def go():
        ok = await wait_until(
            lambda: store.status_view(eid).status == status, timeout=timeout)
        assert ok, f"{eid} 未在超时内到达 {status}，当前 {store.status_view(eid).status}"
    aloop.run_until_complete(go())


def test_redrive_revives_dead_letter_and_completes(store, aloop, caplog):
    """修复下游后重新驱动：死信复活并跑完，结果可查且归类为 SUCCEEDED。"""
    p = build_pipeline(store)
    aloop.run_until_complete(p.start())
    try:
        enq(p, "d1", "test.sometimes")
        wait_status(aloop, store, "d1", EventStatus.DEAD_LETTER)
        assert store.count_effects("d1") == 0

        _flags["fail"] = False  # 下游已修复
        res = p.redrive_dead_letters(redrive_id="rd-1", aggregate_key="acct:rd")
        assert res.redrive_id == "rd-1"
        assert res.accepted == 1 and res.matched == 1 and not res.truncated
        assert res.items[0].outcome == RedriveOutcome.REDRIVEN

        wait_status(aloop, store, "d1", EventStatus.COMPLETED)
        assert store.count_effects("d1") == 1  # 副作用只生效一次

        report = p.redrive_report("rd-1")
        assert report is not None
        assert report.total == 1 and report.succeeded == 1
        assert report.in_flight == 0 and report.failed_again == 0
        assert report.items[0].outcome == RedriveOutcome.SUCCEEDED
        assert report.items[0].redriven_at  # 审计留痕：何时被驱动
        # 日志可辨识：事件标识 + 判定依据
        assert "redrive=rd-1 event=d1" in caplog.text
        assert "decision=REDRIVEN" in caplog.text
    finally:
        aloop.run_until_complete(p.stop())


def test_redrive_same_id_repeated_only_drives_once(store, aloop):
    """同一 redrive_id 重复发起：第二次不重复驱动，审计仍只有一条。"""
    p = build_pipeline(store)
    aloop.run_until_complete(p.start())
    try:
        enq(p, "d2", "test.sometimes")
        wait_status(aloop, store, "d2", EventStatus.DEAD_LETTER)
        aloop.run_until_complete(p.stop())  # 停调度，隔离“再次死亡”的干扰

        r1 = p.redrive_dead_letters(redrive_id="rd-dup", event_ids=["d2"])
        assert r1.accepted == 1
        assert store.status_view("d2").status == EventStatus.RETRY_WAIT

        # 同一 redrive_id 重复调用：事件已不在死信 → 跳过
        r2 = p.redrive_dead_letters(redrive_id="rd-dup", event_ids=["d2"])
        assert r2.accepted == 0 and r2.skipped == 1
        assert r2.items[0].outcome == RedriveOutcome.SKIPPED_NOT_DEAD
        assert "already driven" in r2.items[0].detail

        # 极端情形：事件被驱动后再次死亡，同一 redrive_id 重放也不得再驱动
        with store.write_lock:
            store.conn.execute(
                "UPDATE events SET status=? WHERE event_id='d2'",
                (EventStatus.DEAD_LETTER.value,))
            store.conn.commit()
        r3 = p.redrive_dead_letters(redrive_id="rd-dup", event_ids=["d2"])
        assert r3.accepted == 0 and "already driven" in r3.items[0].detail

        n = store.conn.execute(
            "SELECT COUNT(*) c FROM redrive_audit WHERE redrive_id='rd-dup'"
        ).fetchone()["c"]
        assert n == 1  # 审计仍只有一条
    finally:
        aloop.run_until_complete(p.stop())


def test_redrive_concurrent_calls_drive_each_event_once(store, aloop):
    """多个入口同时发起（不同 redrive_id）：同一事件只被驱动一次。"""
    p = build_pipeline(store)
    aloop.run_until_complete(p.start())
    try:
        for i in range(6):
            enq(p, f"c{i}", "test.sometimes", key=f"k{i % 3}")
        for i in range(6):
            wait_status(aloop, store, f"c{i}", EventStatus.DEAD_LETTER)
        aloop.run_until_complete(p.stop())

        results: list = [None, None]

        def call(idx, rid):
            results[idx] = p.redrive_dead_letters(redrive_id=rid)

        t1 = threading.Thread(target=call, args=(0, "rd-A"))
        t2 = threading.Thread(target=call, args=(1, "rd-B"))
        t1.start(); t2.start(); t1.join(); t2.join()

        total_accepted = results[0].accepted + results[1].accepted
        assert total_accepted == 6          # 每条事件恰好被一个调用驱动
        n = store.conn.execute(
            "SELECT COUNT(*) c FROM redrive_audit").fetchone()["c"]
        assert n == 6                        # 审计一一对应，无重复
        for i in range(6):
            assert store.status_view(f"c{i}").status == EventStatus.RETRY_WAIT
    finally:
        aloop.run_until_complete(p.stop())


def test_redrive_preserves_aggregate_order(store, aloop):
    """重新驱动的事件回到原 seq 位置：同维度后续事件不得越过它。"""
    p = build_pipeline(store)
    aloop.run_until_complete(p.start())
    try:
        enq(p, "ord1", "test.sometimes", key="acct:ord")
        wait_status(aloop, store, "ord1", EventStatus.DEAD_LETTER)
        aloop.run_until_complete(p.stop())

        # 停机期间入队同维度后续事件（保持 PENDING）
        enq(p, "ord2", "test.recorder", key="acct:ord")
        enq(p, "ord3", "test.recorder", key="acct:ord")

        _flags["fail"] = False
        res = p.redrive_dead_letters(redrive_id="rd-ord", event_ids=["ord1"])
        assert res.accepted == 1

        aloop.run_until_complete(p.start())
        wait_status(aloop, store, "ord3", EventStatus.COMPLETED)
        # 完成顺序 = 原始入队顺序：被驱动的 ord1 不插队也不被越过
        assert _completed == ["ord1", "ord2", "ord3"]
    finally:
        aloop.run_until_complete(p.stop())


def test_redrive_side_effect_not_doubled(store, aloop):
    """复活后业务副作用（账户加款）恰好生效一次；重复驱动不翻倍。"""
    p = build_pipeline(store)
    aloop.run_until_complete(p.start())
    try:
        enq(p, "pay1", "test.deposit_flaky",
            {"account": "alice", "amount": 50}, key="acct:alice")
        wait_status(aloop, store, "pay1", EventStatus.DEAD_LETTER)
        assert store.account_balance("alice") == 0

        _flags["fail"] = False
        res = p.redrive_dead_letters(redrive_id="rd-pay", event_ids=["pay1"])
        assert res.accepted == 1
        wait_status(aloop, store, "pay1", EventStatus.COMPLETED)

        assert store.account_balance("alice") == 50   # 只加一次款
        assert store.count_effects("pay1", "deposit") == 1

        # 同一 redrive_id 重放 + 新 redrive_id 对已完成事件：都不再驱动
        r2 = p.redrive_dead_letters(redrive_id="rd-pay", event_ids=["pay1"])
        r3 = p.redrive_dead_letters(redrive_id="rd-pay2", event_ids=["pay1"])
        assert r2.accepted == 0 and r3.accepted == 0
        assert r3.items[0].outcome == RedriveOutcome.SKIPPED_NOT_DEAD
        assert store.account_balance("alice") == 50
    finally:
        aloop.run_until_complete(p.stop())


def test_redrive_batch_limit_and_audit(store, aloop):
    """批量上限截断 + 审计/汇总可查；大批量驱动不阻塞其他维度推进。"""
    p = build_pipeline(store, redrive_batch_limit=2)
    aloop.run_until_complete(p.start())
    try:
        for i in range(5):
            enq(p, f"b{i}", "test.sometimes", key=f"kb{i}")
        for i in range(5):
            wait_status(aloop, store, f"b{i}", EventStatus.DEAD_LETTER)

        # 显式 limit=2：只受理 2 条，truncated 标记
        r1 = p.redrive_dead_letters(redrive_id="rd-b1", limit=2)
        assert r1.accepted == 2 and r1.matched == 5 and r1.truncated
        # 不指定 limit：被 redrive_batch_limit=2 截断，仍需再分批
        r2 = p.redrive_dead_letters(redrive_id="rd-b2")
        assert r2.accepted == 2 and r2.matched == 3 and r2.truncated
        # 最后一批：全部受理，不再截断
        r3 = p.redrive_dead_letters(redrive_id="rd-b3")
        assert r3.accepted == 1 and r3.matched == 1 and not r3.truncated

        summary = p.redrive_summary()
        assert summary.total_redrives == 3
        assert summary.total_events == 5
        assert summary.in_flight == 5  # 尚未修复下游，全部在重试/排队中

        rep1 = p.redrive_report("rd-b1")
        assert rep1 is not None and rep1.total == 2
        assert p.redrive_report("rd-unknown") is None

        # 大批量驱动期间，其他维度照常推进（不卡调度）
        enq(p, "live1", "test.recorder", key="other-key")
        wait_status(aloop, store, "live1", EventStatus.COMPLETED)
    finally:
        aloop.run_until_complete(p.stop())


def test_redrive_outcome_classification(store, aloop):
    """三种结果可区分：成功后跑完 / 仍在重试 / 再次失败回死信。"""
    p = build_pipeline(store)
    aloop.run_until_complete(p.start())
    try:
        enq(p, "s1", "test.sometimes", key="k-s")
        enq(p, "f1", "test.always_retryable", key="k-f")
        enq(p, "w1", "test.sometimes", key="k-w")
        for eid in ("s1", "f1", "w1"):
            wait_status(aloop, store, eid, EventStatus.DEAD_LETTER)
        aloop.run_until_complete(p.stop())

        _flags["fail"] = False  # 只修复 test.sometimes 的下游
        res = p.redrive_dead_letters(redrive_id="rd-cls")
        assert res.accepted == 3

        # 调度未启动：三条都还在 RETRY_WAIT → 归类 IN_FLIGHT（重试中）
        rep = p.redrive_report("rd-cls")
        assert rep.in_flight == 3 and rep.succeeded == 0 and rep.failed_again == 0

        aloop.run_until_complete(p.start())
        wait_status(aloop, store, "s1", EventStatus.COMPLETED)
        wait_status(aloop, store, "w1", EventStatus.COMPLETED)
        wait_status(aloop, store, "f1", EventStatus.DEAD_LETTER)  # 再次失败

        rep = p.redrive_report("rd-cls")
        outcomes = {i.event_id: i.outcome for i in rep.items}
        assert outcomes["s1"] == RedriveOutcome.SUCCEEDED
        assert outcomes["w1"] == RedriveOutcome.SUCCEEDED
        assert outcomes["f1"] == RedriveOutcome.FAILED_AGAIN
        assert rep.succeeded == 2 and rep.failed_again == 1 and rep.in_flight == 0

        summary = p.redrive_summary()
        assert summary.succeeded == 2 and summary.failed_again == 1
    finally:
        aloop.run_until_complete(p.stop())


def test_redrive_selection_by_type_and_ids(store, aloop):
    """按事件类型 / 显式 event_ids 选择；不存在与不在死信的条目可区分。"""
    p = build_pipeline(store)
    aloop.run_until_complete(p.start())
    try:
        enq(p, "t1", "test.sometimes", key="k-t1")
        enq(p, "t2", "test.always_retryable", key="k-t2")
        wait_status(aloop, store, "t1", EventStatus.DEAD_LETTER)
        wait_status(aloop, store, "t2", EventStatus.DEAD_LETTER)
        aloop.run_until_complete(p.stop())

        # 按类型只驱动 test.sometimes
        r1 = p.redrive_dead_letters(redrive_id="rd-t", event_type="test.sometimes")
        assert r1.accepted == 1 and r1.items[0].event_id == "t1"

        # 显式 ids：不存在的与不在死信的都要明确报告
        r2 = p.redrive_dead_letters(
            redrive_id="rd-ids", event_ids=["t2", "ghost", "t1"])
        by_id = {i.event_id: i for i in r2.items}
        assert by_id["t2"].outcome == RedriveOutcome.REDRIVEN
        assert by_id["ghost"].outcome == RedriveOutcome.SKIPPED_NOT_FOUND
        assert by_id["t1"].outcome == RedriveOutcome.SKIPPED_NOT_DEAD
    finally:
        aloop.run_until_complete(p.stop())
