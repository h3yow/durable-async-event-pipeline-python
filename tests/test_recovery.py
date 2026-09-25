"""处理中断/进程重启恢复：IN_PROGRESS 事件恢复到确定状态，副作用不丢不重。"""
from __future__ import annotations

from app.models import EventStatus
from tests.conftest import make_config, wait_until


def deposit(eid, key, account, amount):
    return {
        "event_id": eid,
        "aggregate_key": key,
        "event_type": "account.deposit",
        "payload": {"account": account, "amount": amount},
    }


def test_crash_before_commit_then_restart_completes_once(store, registry, aloop, caplog):
    from app.pipeline import Pipeline

    db_path = store.db_path
    cfg = make_config()
    p1 = Pipeline(store, registry, cfg)
    aloop.run_until_complete(p1.start())
    try:
        p1.crash_after_effects.add("c1")  # 在副作用登记后、提交前“杀死进程”
        p1.enqueue(deposit("c1", "acct:bob", "bob", 42))

        async def crashed():
            await wait_until(
                lambda: store.status_view("c1").status == EventStatus.IN_PROGRESS
            )
            # 给一点时间确认它停在 IN_PROGRESS 而不是被继续流转
            await __import__("asyncio").sleep(0.05)

        aloop.run_until_complete(crashed())
        assert store.status_view("c1").status == EventStatus.IN_PROGRESS
        assert store.account_balance("bob") == 0.0      # 事务未提交：无副作用
        assert store.count_effects("c1") == 0
    finally:
        aloop.run_until_complete(p1.stop())
    store.close()

    # —— 模拟进程重启：新的 store/pipeline，复用同一个 SQLite 文件 ——
    from app.handlers import build_default_registry
    from app.handlers import reset_flaky_attempts

    reset_flaky_attempts()
    store2 = type(store)(db_path)
    store2.connect()
    p2 = Pipeline(store2, build_default_registry(), make_config())
    aloop.run_until_complete(p2.start())
    try:
        assert "c1" in caplog.text and "SIMULATED_CRASH" in caplog.text

        async def recovered():
            await wait_until(
                lambda: store2.status_view("c1").status == EventStatus.COMPLETED,
                timeout=3.0,
            )

        aloop.run_until_complete(recovered())
    finally:
        aloop.run_until_complete(p2.stop())

    # 重开一个连接做最终核验（模拟另起进程检查落盘结果）
    verify = type(store)(db_path)
    verify.connect()
    try:
        assert verify.account_balance("bob") == 42.0
        assert verify.count_effects("c1") == 1
        assert verify.status_view("c1").attempts == 2
    finally:
        verify.close()
    assert "recovery:" in caplog.text                    # 恢复动作有日志


def test_orphan_in_progress_recovered_on_start(store, registry):
    from app.pipeline import Pipeline

    # 不经过处理器，直接制造一条“上次进程遗留的 IN_PROGRESS”
    now = "2020-01-01T00:00:00.000Z"
    store.insert_pending("c2", "acct:x", "account.deposit",
                         '{"account":"x","amount":7}', 5, now)
    store.claim_next_for_key("acct:x", "2020-01-02T00:00:00.000Z")
    assert store.status_view("c2").status == EventStatus.IN_PROGRESS

    p = Pipeline(store, registry, make_config())
    recovered = p.recover()                            # 显式恢复
    assert recovered == ["c2"]
    v = store.status_view("c2")
    assert v.status == EventStatus.RETRY_WAIT          # 确定状态
    # 恢复幂等：再次恢复没有事件
    assert p.recover() == []


def test_all_in_progress_events_are_recovered(store, registry):
    from app.pipeline import Pipeline

    for i in range(4):
        store.insert_pending(f"m{i}", f"k{i}", "account.deposit",
                             '{"account":"a","amount":1}', 5,
                             "2020-01-01T00:00:00.000Z")
        store.claim_next_for_key(f"k{i}", "2020-01-02T00:00:00.000Z")
    p = Pipeline(store, registry, make_config())
    recovered = sorted(p.recover())
    assert recovered == [f"m{i}" for i in range(4)]
    for i in range(4):
        assert store.status_view(f"m{i}").status == EventStatus.RETRY_WAIT


def test_no_loss_or_duplicate_after_restart_full_scan(store, registry, aloop):
    """多条事件混合崩溃，重启后全部到达终态，副作用计数精确。"""
    from app.handlers import reset_flaky_attempts
    from app.pipeline import Pipeline

    db_path = store.db_path
    reset_flaky_attempts()
    p1 = Pipeline(store, registry, make_config())
    aloop.run_until_complete(p1.start())
    try:
        p1.enqueue(deposit("g0", "acct:g", "g", 10))
        p1.crash_after_effects.add("g1")
        p1.enqueue(deposit("g1", "acct:g", "g", 20))
        p1.crash_after_effects.add("g2")
        p1.enqueue(deposit("g2", "acct:g", "g", 30))

        async def settle():
            import asyncio
            await wait_until(
                lambda: store.status_view("g0").status == EventStatus.COMPLETED
                and store.status_view("g2").status == EventStatus.IN_PROGRESS
            )
            await asyncio.sleep(0.05)

        aloop.run_until_complete(settle())
    finally:
        aloop.run_until_complete(p1.stop())
    store.close()

    reset_flaky_attempts()
    store2 = type(store)(db_path)
    store2.connect()
    from app.handlers import build_default_registry
    p2 = Pipeline(store2, build_default_registry(), make_config())
    aloop.run_until_complete(p2.start())
    try:
        async def all_done():
            await wait_until(
                lambda: all(store2.status_view(e).status == EventStatus.COMPLETED
                            for e in ("g0", "g1", "g2")),
                timeout=3.0,
            )

        aloop.run_until_complete(all_done())
    finally:
        aloop.run_until_complete(p2.stop())

    verify = type(store)(db_path)
    verify.connect()
    try:
        assert verify.account_balance("g") == 60.0        # 10+20+30，无重复
        for e in ("g0", "g1", "g2"):
            assert verify.count_effects(e) == 1
    finally:
        verify.close()
