"""重试退避策略：指数退避、上限、可解释说明。"""

from event_pipeline.models import RetryPolicy


def test_exponential_backoff_sequence():
    p = RetryPolicy(max_attempts=5, base_delay=0.1, backoff_factor=2.0,
                    max_delay=10.0, jitter=0.0)
    assert [p.delay_for(n) for n in range(1, 6)] == [0.1, 0.2, 0.4, 0.8, 1.6]


def test_backoff_capped_by_max_delay():
    p = RetryPolicy(base_delay=1.0, backoff_factor=10.0, max_delay=5.0, jitter=0.0)
    assert p.delay_for(3) == 5.0


def test_describe_distinguishes_retry_and_dead():
    p = RetryPolicy(max_attempts=3, base_delay=0.2)
    retry_note = p.describe(2)
    dead_note = p.describe(3)
    assert "等待 0.400s 后重试" in retry_note
    assert "指数退避" in retry_note
    assert "重试次数耗尽，进入死信" in dead_note


def test_zero_attempts_no_delay():
    assert RetryPolicy().delay_for(0) == 0.0
