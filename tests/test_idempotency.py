"""至少一次投递 + 幂等：重复投递/并发处理时业务副作用只生效一次。"""
from __future__ import annotations

import asyncio
import sqlite3
import threading

import pytest

from app.models import EventStatus, RejectReason
from tests.conftest import wait_until


def body(event_id, account="alice", amount=100, key=None, etype="account.deposit"):
    return {
        "event_id": event_id,
        "aggregate_key": key or f"acct:{account}",
        "event_type": etype,
        "payload": {"account": account, "amount": amount} if etype == "account.deposit" else {},
    }


def test_successful_deposit_applies_effect_once(running_pipeline, store, aloop, caplog):
    p = running_pipeline
    p.enqueue(body("dep-1", account="alice", amount=50))

    aloop.run_until_complete(
        wait_until(lambda: store.status_view("dep-1").status == EventStatus.COMPLETED)
    )
    assert store.account_balance("alice") == 50.0
    assert store.count_effects("dep-1") == 1
    v = store.status_view("dep-1")
    assert v.attempts == 1
    assert "event=dep-1" in caplog.text
    assert "attempt=1" in caplog.text
    assert "decision=OK" in caplog.text


def test_duplicate_redelivery_no_second_effect(running_pipeline, store, aloop):
    p = running_pipeline

    async def scenario():
        r1 = p.enqueue(body("dep-2", account="bob", amount=30))
        assert r1.accepted
        await wait_until(lambda: store.status_view("dep-2").status == EventStatus.COMPLETED)
        r2 = p.enqueue(body("dep-2", account="bob", amount=30))
        assert r2.accepted is False and r2.reason == RejectReason.DUPLICATE
        await asyncio.sleep(0.05)

    aloop.run_until_complete(scenario())
    assert store.account_balance("bob") == 30.0
    assert store.count_effects("dep-2") == 1


def test_concurrent_duplicate_enqueue_single_acceptance(pipeline, store):
    """并发重复提交同一 event_id：只有一个被接受，库里只有一条。"""
    payload = body("dep-3", account="c", amount=1, key="k")
    results: list = []

    def enq():
        results.append(pipeline.enqueue(payload))

    threads = [threading.Thread(target=enq) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    accepted = [r for r in results if r.accepted]
    duplicates = [r for r in results if r.reason == RejectReason.DUPLICATE]
    assert len(accepted) == 1
    assert len(duplicates) == 7
    assert store.count_effects("dep-3") == 0
    assert store.status_view("dep-3").status == EventStatus.PENDING


def test_concurrent_claim_only_one_worker_wins(store):
    """同一事件并发领取：条件 UPDATE 保证只有一个领取者。"""
    now = "2026-09-26T00:00:00.000Z"
    store.insert_pending("dep-4", "k", "account.deposit", "{}", 5, now)
    wins: list = []

    def claim():
        wins.append(store.claim_next_for_key("k", "2026-09-27T00:00:00.000Z"))

    threads = [threading.Thread(target=claim) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    records = [w for w in wins if w is not None]
    assert len(records) == 1
    assert records[0].event_id == "dep-4"
    assert records[0].status == EventStatus.IN_PROGRESS


def test_effect_unique_constraint_is_final_guard(store):
    """即使上层被绕过，数据库唯一约束也阻止同一副作用落第二次。"""
    now = "2026-09-26T00:00:00.000Z"
    store.insert_pending("dep-5", "k", "t", "{}", 5, now)
    store.claim_next_for_key("k", "2026-09-27T00:00:00.000Z")
    effects = [{"name": "deposit", "kind": "generic", "detail": {}}]
    store.mark_completed("dep-5", 1, effects)
    # 手工把状态拨回 IN_PROGRESS 模拟“重复执行”，再次提交必须被唯一约束拒绝
    store.conn.execute(
        "UPDATE events SET status='IN_PROGRESS' WHERE event_id='dep-5'"
    )
    store.conn.commit()
    with pytest.raises(sqlite3.IntegrityError):
        store.mark_completed("dep-5", 2, effects)
    assert store.count_effects("dep-5") == 1
