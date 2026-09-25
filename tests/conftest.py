"""测试夹具与辅助。

单测日志统一打印：事件标识(event=...)、尝试次数(attempt=N)、判定依据
(decision=...)，见 app.pipeline 中 logger 的输出。
"""
from __future__ import annotations

import asyncio
import logging

import pytest

from app.backoff import BackoffPolicy
from app.handlers import build_default_registry, reset_flaky_attempts
from app.pipeline import Pipeline, PipelineConfig
from app.store import EventStore


def make_config(max_attempts=5):
    # 快速且确定性的退避（无抖动），测试从头可复现
    return PipelineConfig(
        max_attempts=max_attempts,
        poll_interval=0.005,
        backoff=BackoffPolicy(base_delay=0.01, factor=2.0, max_delay=0.05, jitter=0.0),
        handler_timeout=5.0,
    )


@pytest.fixture
def store(tmp_path):
    s = EventStore(str(tmp_path / "events.db"))
    s.connect()
    yield s
    s.close()


@pytest.fixture
def registry():
    reset_flaky_attempts()
    yield build_default_registry()
    reset_flaky_attempts()


@pytest.fixture
def pipeline(store, registry):
    p = Pipeline(store, registry, make_config())
    return p


@pytest.fixture
def started_pipeline(pipeline):
    async def _run():
        await pipeline.start()

    asyncio.run(_run())
    yield pipeline
    asyncio.run(pipeline.stop())


async def wait_until(predicate, timeout=3.0, interval=0.01):
    """轮询直到 predicate 为真或超时。"""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return predicate()


def run(coro):
    return asyncio.run(coro)


@pytest.fixture(autouse=True)
def _pipeline_logs(caplog):
    """捕获并在失败时展示 pipeline 日志（事件标识/尝试次数/判定依据）。"""
    caplog.set_level(logging.INFO, logger="pipeline")
    yield


@pytest.fixture
def aloop():
    """为单个测试提供一个贯穿全用例的事件循环。

    用法：测试体内直接 `await ...` 不可行（同步函数），因此提供
    aloop.run(coro)；pipeline 的 start/stop 与业务协程都在该循环内。
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        yield loop
    finally:
        loop.close()
        asyncio.set_event_loop(None)


@pytest.fixture
def running_pipeline(aloop, store, registry):
    """在 aloop 内启动并最终停止的流水线（dispatcher 始终存活于同一循环）。"""
    p = Pipeline(store, registry, make_config())
    aloop.run_until_complete(p.start())
    try:
        yield p
    finally:
        aloop.run_until_complete(p.stop())
