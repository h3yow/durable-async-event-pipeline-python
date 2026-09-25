"""退避重试、次数上限、不可重试失败与死信原因保留。"""
from __future__ import annotations

from app.models import EventStatus
from tests.conftest import wait_until


def enq(p, eid, etype, payload=None, key=None):
    return p.enqueue({
        "event_id": eid,
        "aggregate_key": key or etype,
        "event_type": etype,
        "payload": payload or {},
    })


def test_retry_then_success(running_pipeline, store, aloop, caplog):
    p = running_pipeline
    enq(p, "r1", "service.flaky", {"fail_times": 2}, key="k1")

    async def go():
        await wait_until(lambda: store.status_view("r1").status == EventStatus.COMPLETED,
                         timeout=3.0)

    aloop.run_until_complete(go())
    v = store.status_view("r1")
    assert v.status == EventStatus.COMPLETED
    assert v.attempts == 3                       # 2 次失败 + 1 次成功
    assert store.count_effects("r1") == 1       # 成功副作用只有一次
    assert "decision=RETRYABLE" in caplog.text
    assert "backoff" in caplog.text             # 退避依据被记录
    assert "decision=OK" in caplog.text


def test_retry_exhaustion_goes_dead(running_pipeline, store, aloop, caplog):
    p = running_pipeline
    enq(p, "r2", "service.flaky", {"fail_times": 99}, key="k2")  # 永远失败

    async def go():
        await wait_until(lambda: store.status_view("r2").status == EventStatus.DEAD_LETTER,
                         timeout=4.0)

    aloop.run_until_complete(go())
    v = store.status_view("r2")
    assert v.status == EventStatus.DEAD_LETTER
    assert v.attempts == v.max_attempts          # 达到次数上限
    assert "retries exhausted" in v.last_error   # 保留可解释的失败原因
    assert "transient failure" in v.last_error
    dl = store.list_dead_letters()
    assert [d.event_id for d in dl] == ["r2"]
    assert "decision=DEAD_LETTER" in caplog.text
    assert store.count_effects("r2") == 0


def test_non_retryable_goes_dead_immediately(running_pipeline, store, aloop, caplog):
    p = running_pipeline
    enq(p, "r3", "service.always_bad", {}, key="k3")

    async def go():
        await wait_until(lambda: store.status_view("r3").status == EventStatus.DEAD_LETTER)

    aloop.run_until_complete(go())
    v = store.status_view("r3")
    assert v.status == EventStatus.DEAD_LETTER
    assert v.attempts == 1                       # 不浪费重试次数
    assert "non-retryable" in v.last_error
    assert "permanent business rejection" in v.last_error


def test_unknown_event_type_is_non_retryable(running_pipeline, store, aloop):
    p = running_pipeline
    enq(p, "r4", "totally.unknown", {}, key="k4")

    async def go():
        await wait_until(lambda: store.status_view("r4").status == EventStatus.DEAD_LETTER)

    aloop.run_until_complete(go())
    v = store.status_view("r4")
    assert v.status == EventStatus.DEAD_LETTER
    assert v.attempts == 1
    assert "no handler registered" in v.last_error


def test_backoff_delays_are_interpretable_and_capped():
    from app.backoff import BackoffPolicy

    pol = BackoffPolicy(base_delay=0.1, factor=2.0, max_delay=1.0, jitter=0.0)
    seq = [pol.delay_for(i) for i in range(1, 6)]
    assert seq == [0.1, 0.2, 0.4, 0.8, 1.0]      # 指数增长且被上限截断
    text = pol.describe(3)
    assert "attempt#3" in text and "0.400s" in text
