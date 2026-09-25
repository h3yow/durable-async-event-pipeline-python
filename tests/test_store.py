"""存储层：去重入队、结构校验、顺序约束、租约与副作用仅一次登记。"""

import time

from event_pipeline.models import EventStatus, FailureKind, IngestOutcome
from event_pipeline.storage import EventStore


def _store(path):
    return EventStore(path)


def test_enqueue_accepted_with_monotonic_seq_per_aggregate(db_path):
    s = _store(db_path)
    assert s.enqueue("e1", "A", {}, 3).outcome is IngestOutcome.ACCEPTED
    assert s.enqueue("e2", "A", {}, 3).outcome is IngestOutcome.ACCEPTED
    assert s.enqueue("x1", "B", {}, 3).outcome is IngestOutcome.ACCEPTED
    assert [e.seq for e in s.list_by_aggregate("A")] == [1, 2]
    assert [e.seq for e in s.list_by_aggregate("B")] == [1]


def test_duplicate_event_id_rejected_as_replay(db_path):
    s = _store(db_path)
    s.enqueue("dup", "A", {"v": 1}, 3)
    result = s.enqueue("dup", "A", {"v": 2}, 3)
    assert result.outcome is IngestOutcome.DUPLICATE
    assert "重放" in result.reason
    # 原数据不被覆盖
    assert s.get("dup").payload == {"v": 1}
    assert s.stats()["pending"] == 1


def test_invalid_envelopes_are_rejected_with_distinguishable_reason(db_path):
    s = _store(db_path)
    assert s.enqueue("", "A", {}, 3).outcome is IngestOutcome.INVALID
    assert s.enqueue("e", "", {}, 3).outcome is IngestOutcome.INVALID
    assert s.enqueue("e", "A", "not-an-object", 3).outcome is IngestOutcome.INVALID
    assert s.enqueue("e", "A", {}, 0).outcome is IngestOutcome.INVALID
    assert s.enqueue("e", "A", {}, True).outcome is IngestOutcome.INVALID
    assert s.stats()["pending"] == 0
    assert s.stats()["rejected"] == 0  # 非法事件根本不入库


def test_due_fetch_returns_only_head_of_each_aggregate(db_path):
    s = _store(db_path)
    s.enqueue("a1", "A", {}, 3)
    s.enqueue("a2", "A", {}, 3)
    s.enqueue("b1", "B", {}, 3)
    now = time.time()
    heads = {(e.event_id, e.aggregate_key) for e in s.fetch_due(now, 10)}
    assert heads == {("a1", "A"), ("b1", "B")}
    # a1 终结后 a2 才可派发
    s.mark_succeeded("a1", now)
    heads = {e.event_id for e in s.fetch_due(now, 10)}
    assert heads == {"a2", "b1"}


def test_claim_is_optimistic_and_mutually_exclusive(db_path):
    s = _store(db_path)
    s.enqueue("a1", "A", {}, 3)
    now = time.time()
    first = s.claim("a1", "w1", 30, now)
    second = s.claim("a1", "w2", 30, now)
    assert first is not None and first.status is EventStatus.RUNNING
    assert first.attempts == 1
    assert second is None


def test_lease_expiry_enables_crash_recovery(db_path):
    s = _store(db_path)
    s.enqueue("a1", "A", {}, 3)
    now = time.time()
    s.claim("a1", "w1", 0.02, now)
    assert s.fetch_recoverable(now) == []
    later = now + 1
    stuck = s.fetch_recoverable(later)
    assert [e.event_id for e in stuck] == ["a1"]
    s.mark_recovered(stuck[0], later)
    recovered = s.get("a1")
    assert recovered.status is EventStatus.FAILED
    assert recovered.failure_kind is FailureKind.CRASH
    assert recovered.lease_owner is None
    # 恢复后可以重新认领，attempts 继续累计
    again = s.claim("a1", "w2", 30, later)
    assert again is not None and again.attempts == 2


def test_effect_registry_enforces_single_application(db_path):
    s = _store(db_path)
    s.enqueue("a1", "A", {}, 3)
    assert s.begin_effect("a1", time.time()) is True
    s.commit_effect("a1", time.time())
    # 重放：副作用已生效
    assert s.begin_effect("a1", time.time()) is False
    assert s.effect_applied("a1") is True


def test_pending_effect_after_crash_is_retryable(db_path):
    s = _store(db_path)
    s.enqueue("a1", "A", {}, 3)
    assert s.begin_effect("a1", time.time()) is True
    # 未 commit 即崩溃 -> 显式回滚/重新 begin 允许重试
    s.rollback_effect("a1")
    assert s.effect_applied("a1") is False
    assert s.begin_effect("a1", time.time()) is True


def test_state_survives_reopen(db_path):
    s = _store(db_path)
    s.enqueue("a1", "A", {"k": "v"}, 3)
    s.claim("a1", "w1", 30, time.time())
    s.mark_dead("a1", FailureKind.NON_RETRYABLE, "bad", time.time())
    s.close()

    s2 = EventStore(db_path)
    e = s2.get("a1")
    assert e.status is EventStatus.DEAD
    assert e.failure_kind is FailureKind.NON_RETRYABLE
    assert e.last_error == "bad"
    assert e.payload == {"k": "v"}
