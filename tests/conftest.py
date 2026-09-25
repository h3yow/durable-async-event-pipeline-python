"""测试公共夹具：临时文件数据库、可控重试策略、日志捕获。"""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from event_pipeline.models import RetryPolicy
from event_pipeline.pipeline import EventPipeline

# 单测日志：必须包含事件标识、尝试次数与判定依据。
LOG_FORMAT = "%(levelname)s %(name)s event=%(message)s"


@pytest.fixture(autouse=True)
def _pipeline_logs(caplog):
    caplog.set_level(logging.DEBUG, logger="event_pipeline")
    yield


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "events-test.db"


@pytest.fixture
def fast_policy() -> RetryPolicy:
    return RetryPolicy(
        max_attempts=3,
        base_delay=0.01,
        backoff_factor=2.0,
        max_delay=0.05,
        jitter=0.0,
    )


def make_pipeline(db_path, handler, fast_policy, **kwargs) -> EventPipeline:
    kwargs.setdefault("lease_seconds", 5.0)
    kwargs.setdefault("poll_interval", 0.01)
    return EventPipeline(db_path, handler=handler, retry_policy=fast_policy, **kwargs)


@pytest.fixture
def pipeline_factory(db_path, fast_policy):
    created: list[EventPipeline] = []

    def _factory(handler, **kwargs):
        p = make_pipeline(db_path, handler, fast_policy, **kwargs)
        created.append(p)
        return p

    yield _factory

    for p in created:
        try:
            p.close()
        except Exception:
            pass


async def drain(pipeline: EventPipeline, timeout: float = 3.0) -> bool:
    """等待管线处理完全部到点事件。"""
    return await pipeline.wait_drained(timeout=timeout)
