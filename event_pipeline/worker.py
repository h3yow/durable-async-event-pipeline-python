"""异步事件处理 worker：租约认领、按聚合维度串行、维度间并发、退避重试与死信。"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections.abc import Awaitable, Callable

from event_pipeline.errors import EventError, NonRetryableError
from event_pipeline.models import EventRecord, FailureKind, RetryPolicy
from event_pipeline.storage import EventStore

# 处理器签名：接收事件与副作用上下文，执行业务。
Handler = Callable[[EventRecord, "EffectContext"], Awaitable[None]]

logger = logging.getLogger("event_pipeline.worker")


class EffectAlreadyApplied(Exception):
    """重放事件：副作用此前已生效，本次尝试不再执行业务，直接判成功。"""


class EffectContext:
    """传给处理器的上下文：处理器必须在副作用真正持久生效处调用 ``applied()``。

    采用幂等登记表保证：同一 event_id 的副作用在重试/重复派发下只生效一次。
    """

    def __init__(self, store: EventStore, event_id: str) -> None:
        self._store = store
        self._event_id = event_id
        self._applied = False

    @property
    def applied_flag(self) -> bool:
        return self._applied

    async def applied(self) -> None:
        """声明"外部可观测副作用已生效"（pending -> applied）。"""
        now = time.time()
        if self._store.effect_applied(self._event_id):
            self._applied = True
            raise EffectAlreadyApplied(self._event_id)
        self._store.commit_effect(self._event_id, now)
        self._applied = True
        logger.info(
            "event_id=%s 副作用已确认生效（effects 登记 pending->applied）",
            self._event_id,
        )


class EventWorker:
    """后台异步 worker。

    - 同一 aggregate_key 的事件按入队 seq 串行完成（存储层队首约束 + 每键锁）；
    - 不同 aggregate_key 之间通过信号量限并发推进；
    - 单键长期失败只延后该键后续事件，不阻塞其他键；
    - 租约超时的 RUNNING 事件会被恢复为 FAILED（crash）重试，重启不丢不重。
    """

    def __init__(
        self,
        store: EventStore,
        handler: Handler,
        retry_policy: RetryPolicy | None = None,
        *,
        concurrency: int = 8,
        lease_seconds: float = 30.0,
        poll_interval: float = 0.05,
        batch_size: int = 32,
        logger: logging.Logger | None = None,
    ) -> None:
        self._store = store
        self._handler = handler
        self._policy = retry_policy or RetryPolicy()
        self._concurrency = int(concurrency)
        self._sem = asyncio.Semaphore(self._concurrency)
        self._lease = float(lease_seconds)
        self._poll = float(poll_interval)
        self._batch = int(batch_size)
        self._log = logger or logging.getLogger("event_pipeline.worker")
        self._owner = f"worker-{uuid.uuid4().hex[:8]}"
        self._key_locks: dict[str, asyncio.Lock] = {}
        self._inflight: set[asyncio.Task[None]] = set()
        self._poll_task: asyncio.Task[None] | None = None
        self._stopped = asyncio.Event()

    # -------------------------------------------------------------- 生命周期
    async def start(self) -> None:
        """启动后台轮询任务（幂等）。"""
        if self._poll_task is None:
            self._stopped.clear()
            self._poll_task = asyncio.create_task(self._poll_loop())
            self._log.info("worker %s 已启动（并发上限=%s 租约=%ss）",
                           self._owner, self._concurrency, self._lease)

    async def stop(self, drain: bool = True) -> None:
        """停止轮询；drain=True 时等待所有在途事件处理完，保证关停状态确定。"""
        self._stopped.set()
        if self._poll_task is not None:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
            self._poll_task = None
        if drain and self._inflight:
            self._log.info("worker 关停中，等待 %s 个在途事件完成", len(self._inflight))
            await asyncio.gather(*self._inflight, return_exceptions=True)
        self._log.info("worker %s 已停止", self._owner)

    async def run_once(self, now: float | None = None) -> int:
        """执行一轮"崩溃恢复 -> 到点认领 -> 处理"，返回本轮认领条数。"""
        now = time.time() if now is None else now
        self.recover_stuck(now)
        return await self._dispatch_due(now)

    def recover_stuck(self, now: float | None = None) -> int:
        """租约超时恢复：把中断/崩溃遗留的 RUNNING 事件置回 failed(crash)。

        返回恢复条数。纯存储操作，进程重启后、派发前调用可保证状态确定。
        """
        now = time.time() if now is None else now
        stuck = self._store.fetch_recoverable(now)
        for event in stuck:
            self._store.mark_recovered(event, now)
            self._log.warning(
                "event_id=%s 恢复：上次尝试 attempts=%s 租约超时未完成，"
                "判定依据=running 且 lease_expires_at<=%s，状态 running->failed(crash)",
                event.event_id, event.attempts, now,
            )
        return len(stuck)

    async def _dispatch_due(self, now: float) -> int:
        """认领各聚合到点队首事件（存储层保证同键不会越过前序事件）并处理。"""
        due = self._store.fetch_due(now, self._batch)
        spawned = 0
        for cand in due:
            event = self._store.claim(cand.event_id, self._owner, self._lease, now)
            if event is None:
                continue  # 被其他 worker/轮次并发认领
            spawned += 1
            task = asyncio.create_task(self._guarded(event))
            self._inflight.add(task)
            task.add_done_callback(self._inflight.discard)
        if self._inflight:
            await asyncio.gather(*list(self._inflight), return_exceptions=True)
        return spawned

    async def wait_drained(
        self,
        timeout: float = 5.0,
        poll: float = 0.01,
        include_failed: bool = True,
    ) -> bool:
        """等待处理安定。

        include_failed=True（默认）：等待所有事件到达终态（含退避重试直至成功/死信）；
        include_failed=False：仅等待当前到点且在途的事件处理完
                             （退避等待中的失败事件视为暂时安定）。
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            in_flight = bool(self._inflight)
            if include_failed:
                unfinished = self._store.count_unfinished() > 0
            else:
                unfinished = bool(self._store.list_due(time.time()))
            if not in_flight and not unfinished:
                return True
            await asyncio.sleep(poll)
        return False

    # ------------------------------------------------------------------ 内部
    async def _poll_loop(self) -> None:
        while not self._stopped.is_set():
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                self._log.exception("轮询周期异常，%ss 后继续", self._poll)
            try:
                await asyncio.wait_for(self._stopped.wait(), timeout=self._poll)
            except asyncio.TimeoutError:
                pass

    def _key_lock(self, key: str) -> asyncio.Lock:
        lock = self._key_locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._key_locks[key] = lock
        return lock

    async def _guarded(self, event: EventRecord) -> None:
        """并发闸门：总并发受信号量限制；同聚合受键锁串行化。"""
        async with self._sem:
            async with self._key_lock(event.aggregate_key):
                await self._process(event)

    async def _process(self, event: EventRecord) -> None:
        now = time.time()
        self._log.info(
            "派发 event_id=%s aggregate=%s seq=%s 尝试 attempts=%s/%s "
            "判定依据=该事件为聚合队首且已到点",
            event.event_id, event.aggregate_key, event.seq,
            event.attempts, event.max_attempts,
        )
        # 幂等闸门：副作用已生效（重放）则跳过业务，直接成功。
        if not self._store.begin_effect(event.event_id, now):
            self._store.mark_succeeded(event.event_id, now)
            self._log.warning(
                "event_id=%s 结论=重放命中 attempts=%s 判定依据=effects 已存在 applied，"
                "业务副作用不重复执行，状态 ->succeeded",
                event.event_id, event.attempts,
            )
            return

        ctx = EffectContext(self._store, event.event_id)
        try:
            await self._handler(event, ctx)
        except NonRetryableError as exc:
            self._store.rollback_effect(event.event_id)
            self._store.mark_dead(
                event.event_id, FailureKind.NON_RETRYABLE, str(exc), time.time()
            )
            self._log.error(
                "event_id=%s 结论=死信 attempts=%s/%s 判定依据=不可重试错误：%s",
                event.event_id, event.attempts, event.max_attempts, exc,
            )
            return
        except EventError as exc:
            self._store.rollback_effect(event.event_id)
            self._schedule_retry_or_dead(event, str(exc))
            return
        except Exception as exc:  # 未预期异常：保守地按可重试处理
            self._store.rollback_effect(event.event_id)
            self._schedule_retry_or_dead(
                event, f"未预期异常（按可重试处理）：{exc!r}"
            )
            return

        now = time.time()
        # 处理器未显式声明 applied：视为副作用随处理完成而生效（纯计算型 handler）。
        if not ctx.applied_flag:
            self._store.commit_effect(event.event_id, now)
        self._store.mark_succeeded(event.event_id, now)
        self._log.info(
            "event_id=%s 结论=成功 attempts=%s/%s 判定依据=handler 正常返回",
            event.event_id, event.attempts, event.max_attempts,
        )

    def _schedule_retry_or_dead(self, event: EventRecord, error: str) -> None:
        now = time.time()
        if event.attempts >= event.max_attempts:
            self._store.mark_dead(
                event.event_id,
                FailureKind.MAX_ATTEMPTS,
                f"{error}；{self._policy.describe(event.attempts)}",
                now,
            )
            self._log.error(
                "event_id=%s 结论=死信 attempts=%s/%s 判定依据=重试次数耗尽；最后原因：%s",
                event.event_id, event.attempts, event.max_attempts, error,
            )
            return
        delay = self._policy.delay_for(event.attempts)
        self._store.mark_retry(event.event_id, error, now + delay, now)
        self._log.warning(
            "event_id=%s 结论=等待重试 attempts=%s/%s 判定依据=%s；原因：%s",
            event.event_id, event.attempts, event.max_attempts,
            self._policy.describe(event.attempts), error,
        )
