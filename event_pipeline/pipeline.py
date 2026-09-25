"""EventPipeline：对外门面，组合 EventStore + RetryPolicy + EventWorker。"""

from __future__ import annotations

import logging
from pathlib import Path

from event_pipeline.models import (
    EventRecord,
    EventStatus,
    IngestResult,
    RetryPolicy,
)
from event_pipeline.storage import EventStore
from event_pipeline.worker import EventWorker, Handler


class EventPipeline:
    """本地一体化事件管线（持久化 + 异步 worker），不依赖任何外部中间件。"""

    def __init__(
        self,
        path: str | Path = "events.db",
        handler: Handler | None = None,
        retry_policy: RetryPolicy | None = None,
        *,
        concurrency: int = 8,
        lease_seconds: float = 30.0,
        poll_interval: float = 0.05,
        batch_size: int = 32,
        logger: logging.Logger | None = None,
    ) -> None:
        self._logger = logger or logging.getLogger("event_pipeline")
        self._store = EventStore(path)
        self._policy = retry_policy or RetryPolicy()
        self._default_max = self._policy.max_attempts
        self._worker = EventWorker(
            self._store,
            handler if handler is not None else _null_handler,
            self._policy,
            concurrency=concurrency,
            lease_seconds=lease_seconds,
            poll_interval=poll_interval,
            batch_size=batch_size,
            logger=logging.getLogger("event_pipeline.worker"),
        )

    @property
    def store(self) -> EventStore:
        return self._store

    @property
    def worker(self) -> EventWorker:
        return self._worker

    def publish(
        self,
        event_id: str,
        aggregate_key: str,
        payload: dict,
        max_attempts: int | None = None,
    ) -> IngestResult:
        """持久化入队（结构非法拒绝 / 重复识别为重放），worker 异步拾取。"""
        return self._store.enqueue(
            event_id,
            aggregate_key,
            payload,
            max_attempts if max_attempts is not None else self._default_max,
        )

    def get_event(self, event_id: str) -> EventRecord | None:
        return self._store.get(event_id)

    def list_status(self, status: EventStatus) -> list[EventRecord]:
        return self._store.list_status(status)

    def list_dead_letters(self) -> list[EventRecord]:
        return self._store.list_status(EventStatus.DEAD)

    def list_by_aggregate(self, aggregate_key: str) -> list[EventRecord]:
        return self._store.list_by_aggregate(aggregate_key)

    def stats(self) -> dict[str, int]:
        return self._store.stats()

    def effect_applied(self, event_id: str) -> bool:
        return self._store.effect_applied(event_id)

    async def start(self) -> None:
        await self._worker.start()

    async def stop(self, drain: bool = True) -> None:
        await self._worker.stop(drain=drain)

    async def run_once(self) -> int:
        """同步驱动一轮处理（测试/脚本使用，无需 start）。"""
        return await self._worker.run_once()

    def recover_stuck(self, now: float | None = None) -> int:
        """进程重启后调用：将租约超时的 RUNNING 事件恢复为 failed(crash)。"""
        return self._worker.recover_stuck(now)

    async def wait_drained(
        self, timeout: float = 5.0, include_failed: bool = True
    ) -> bool:
        return await self._worker.wait_drained(
            timeout=timeout, include_failed=include_failed
        )

    def close(self) -> None:
        self._store.close()

    async def __aenter__(self) -> "EventPipeline":
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.stop()
        self.close()


async def _null_handler(event: EventRecord, ctx) -> None:
    """未注册业务处理器时的安全默认：不执行副作用，直接成功。"""
    await ctx.applied()
