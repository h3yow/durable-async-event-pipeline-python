"""持久化异步事件流水线：入队、按聚合维度串行调度、退避重试、死信与恢复。

并发模型（asyncio 单进程，无需外部中间件）：
    * 每个 aggregate_key 至多一个 worker 协程 → 同维度严格按入队顺序串行；
      worker 每次只领取该维度“最靠前且到期”的事件（队头阻塞保序）；
    * 不同维度的 worker 彼此独立并发 → 单维度长时间失败不阻塞其他维度；
    * 状态全部落 SQLite，任意时刻可查询；dispatcher 周期扫描到期事件。

投递语义：至少一次投递（at-least-once）+ 事务性副作用登记，使业务副作用
在重复投递/并发处理/崩溃重放下恰好生效一次（effectively exactly-once）。
"""
from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

from app.backoff import BackoffPolicy
from app.errors import HandlerError, NonRetryableError, RetryableError
from app.handlers import EFF_ACCOUNT_DELTA, HandlerRegistry
from app.models import (
    DeadLetterView,
    EnqueueResult,
    EventRecord,
    EventStatus,
    HandleContext,
    HandleDecision,
    RedriveItemResult,
    RedriveOutcome,
    RedriveReport,
    RedriveReportItem,
    RedriveResult,
    RedriveSummary,
    RejectReason,
    StatusView,
)
from app.store import EventStore
from app.validation import ValidationError, validate_envelope, validate_payload

logger = logging.getLogger("pipeline")


def utc_now() -> datetime:
    return datetime.now(UTC)


def iso(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


@dataclass(frozen=True, slots=True)
class PipelineConfig:
    max_attempts: int = 5
    poll_interval: float = 0.02
    backoff: BackoffPolicy = field(default_factory=BackoffPolicy)
    handler_timeout: float = 10.0
    # 单次重新驱动调用受理的事件数量上限：防止一次大批量驱动长时间占用
    # 写事务、卡住正在运行的调度；超出部分由调用方分批再次发起。
    redrive_batch_limit: int = 200


class SimulatedCrash(BaseException):
    """测试专用：模拟处理器“执行到一半进程被杀”，事件停留在 IN_PROGRESS。

    继承 BaseException 以免被业务异常分支（Exception）当作不可重试失败。
    """


class _RuntimeHandleContext(HandleContext):
    """处理器侧上下文：收集本次尝试声明的副作用，并支持重放识别。"""

    def __init__(
        self,
        pipeline: "Pipeline",
        record: EventRecord,
        attempt: int,
    ) -> None:
        super().__init__(
            event_id=record.event_id,
            aggregate_key=record.aggregate_key,
            attempt=attempt,
        )
        self._pipeline = pipeline
        self.effects: list[dict[str, Any]] = []

    def record_effect(self, name: str, detail: dict[str, Any], kind: str = "generic") -> None:
        if self.already_applied(name):
            # 同一处理器内重复声明同一副作用：幂等忽略，不计两次。
            logger.info(
                "event=%s attempt=%d decision=REPLAY effect=%s already applied in prior try",
                self.event_id, self.attempt, name,
            )
            return
        if any(e["name"] == name for e in self.effects):
            return
        self.effects.append({"name": name, "detail": detail, "kind": kind})

    def already_applied(self, name: str) -> bool:
        return self._pipeline.store.effect_exists(self.event_id, name)


class Pipeline:
    def __init__(
        self,
        store: EventStore,
        registry: HandlerRegistry,
        config: PipelineConfig | None = None,
    ) -> None:
        self.store = store
        self.registry = registry
        self.config = config or PipelineConfig()
        self._running = False
        self._dispatcher: asyncio.Task[None] | None = None
        self._key_workers: dict[str, asyncio.Task[None]] = {}
        # 测试钩子：在副作用登记之后、事务提交之前抛 SimulatedCrash。
        self.crash_after_effects: set[str] = set()
        # 本进程内已发生模拟崩溃的维度：worker 退出后不再被 dispatcher 重启，
        # 直到 stop()/重启（真实进程死亡后只有重启才会恢复这些事件）。
        self._crashed_keys: set[str] = set()

    # ---- 生命周期 ----------------------------------------------------------
    async def start(self) -> None:
        self.store.connect()
        recovered = self.recover()
        if recovered:
            logger.warning("recovery: %d in-progress event(s) -> RETRY_WAIT: %s",
                           len(recovered), recovered)
        self._crashed_keys.clear()
        self._running = True
        self._dispatcher = asyncio.create_task(self._dispatch_loop(), name="dispatcher")

    async def stop(self) -> None:
        self._running = False
        if self._dispatcher is not None:
            self._dispatcher.cancel()
            try:
                await self._dispatcher
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            self._dispatcher = None
        workers = list(self._key_workers.values())
        for t in workers:
            t.cancel()
        for t in workers:
            try:
                await t
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        self._key_workers.clear()

    def recover(self) -> list[str]:
        """进程重启后的确定恢复：IN_PROGRESS -> RETRY_WAIT（立即可再领取）。"""
        return self.store.recover_in_progress()

    def is_running(self) -> bool:
        return self._running

    # ---- 入队 --------------------------------------------------------------
    def enqueue(self, raw: Any) -> EnqueueResult:
        # 1) 信封结构校验
        try:
            req = validate_envelope(raw)
        except ValidationError as exc:
            eid = raw.get("event_id") if isinstance(raw, dict) else None
            eid = eid if isinstance(eid, str) and eid.strip() else f"rejected:{id(raw):x}"
            self.store.insert_rejected(eid, "-", "-", "{}", exc.message)
            logger.warning("enqueue rejected reason=%s detail=%s",
                           RejectReason.INVALID_ENVELOPE.value, exc.message)
            return EnqueueResult(
                accepted=False, event_id=eid,
                status=EventStatus.REJECTED,
                reason=RejectReason.INVALID_ENVELOPE, detail=exc.message,
            )

        # 2) 重复识别（在任何写操作之前，结论与非法事件可区分）。
        #    与 3)/4) 同处一把写锁内：检查与插入原子完成，并发重复投递
        #    只有一个线程能通过。
        with self.store.write_lock:
            if self.store.event_exists(req.event_id):
                existing = self.store.status_view(req.event_id)
                logger.warning(
                    "enqueue duplicate event=%s current_status=%s decision=%s",
                    req.event_id,
                    existing.status.value if existing else "?",
                    RejectReason.DUPLICATE.value,
                )
                return EnqueueResult(
                    accepted=False, event_id=req.event_id,
                    status=existing.status if existing else None,
                    reason=RejectReason.DUPLICATE,
                    detail="event_id already exists; treated as replay",
                )

            # 3) 已知类型的 payload 校验
            try:
                validate_payload(req)
            except ValidationError as exc:
                self.store.insert_rejected(
                    req.event_id, req.aggregate_key, req.event_type,
                    json.dumps(req.payload, sort_keys=True), exc.message,
                )
                logger.warning("enqueue rejected event=%s reason=%s detail=%s",
                               req.event_id, RejectReason.INVALID_PAYLOAD.value, exc.message)
                return EnqueueResult(
                    accepted=False, event_id=req.event_id,
                    status=EventStatus.REJECTED,
                    reason=RejectReason.INVALID_PAYLOAD, detail=exc.message,
                )

            # 4) 持久化入队（not_before=now → 立即可调度）
            self.store.insert_pending(
                req.event_id, req.aggregate_key, req.event_type,
                json.dumps(req.payload, sort_keys=True),
                self.config.max_attempts, iso(utc_now()),
            )

        logger.info("enqueue accepted event=%s key=%s type=%s",
                    req.event_id, req.aggregate_key, req.event_type)
        return EnqueueResult(
            accepted=True, event_id=req.event_id,
            status=EventStatus.PENDING, reason=None, detail="",
        )

    # ---- 观测 --------------------------------------------------------------
    def get_status(self, event_id: str) -> StatusView | None:
        return self.store.status_view(event_id)

    def list_by_aggregate(self, aggregate_key: str) -> list[StatusView]:
        return self.store.list_by_aggregate(aggregate_key)

    def list_dead_letters(self, limit: int = 100) -> list[DeadLetterView]:
        return self.store.list_dead_letters(limit)

    # ---- 死信重新驱动 --------------------------------------------------------
    def redrive_dead_letters(
        self,
        redrive_id: str | None = None,
        aggregate_key: str | None = None,
        event_type: str | None = None,
        event_ids: list[str] | None = None,
        limit: int | None = None,
    ) -> RedriveResult:
        """把死信事件按条件重新投入调度（幂等：同一 redrive_id 只生效一次）。

        * 选择条件：aggregate_key / event_type / 显式 event_ids（可组合）；
        * 数量上限：limit 截断到 config.redrive_batch_limit，命中更多时
          truncated=True，调用方可对剩余部分再次发起（换新 redrive_id）；
        * 重新驱动后事件保留原 event_id / payload / seq，仅重置 attempts，
          回到维度内原有次序，继续受顺序与副作用幂等约束；
        * 同步返回受理结果；处理结果通过 redrive_report 异步查询。
        """
        rid = redrive_id or f"rd-{uuid4().hex}"
        effective_limit = min(limit or self.config.redrive_batch_limit,
                              self.config.redrive_batch_limit)
        matched, rows = self.store.redrive_dead_letters(
            rid,
            aggregate_key=aggregate_key,
            event_type=event_type,
            event_ids=event_ids,
            limit=effective_limit,
        )
        items = [
            RedriveItemResult(
                event_id=eid, aggregate_key=key, event_type=etype,
                outcome=outcome, detail=detail,
            )
            for eid, key, etype, outcome, detail in rows
        ]
        accepted = sum(1 for i in items if i.outcome == RedriveOutcome.REDRIVEN)
        truncated = matched > accepted and matched > effective_limit
        for item in items:
            logger.info(
                "redrive=%s event=%s key=%s type=%s decision=%s %s",
                rid, item.event_id, item.aggregate_key, item.event_type,
                item.outcome.value, item.detail,
            )
        logger.info(
            "redrive=%s matched=%d accepted=%d skipped=%d truncated=%s limit=%d",
            rid, matched, accepted, len(items) - accepted,
            truncated, effective_limit,
        )
        return RedriveResult(
            redrive_id=rid,
            accepted=accepted,
            skipped=len(items) - accepted,
            truncated=truncated,
            matched=matched,
            items=items,
        )

    def redrive_report(self, redrive_id: str) -> RedriveReport | None:
        """查询某次重新驱动的结果报告（成功/重试中/再次失败可区分）。"""
        items = self.store.redrive_report_items(redrive_id)
        if not items:
            return None
        succeeded = sum(1 for i in items if i.outcome == RedriveOutcome.SUCCEEDED)
        failed = sum(1 for i in items if i.outcome == RedriveOutcome.FAILED_AGAIN)
        in_flight = len(items) - succeeded - failed
        return RedriveReport(
            redrive_id=redrive_id,
            total=len(items),
            succeeded=succeeded,
            in_flight=in_flight,
            failed_again=failed,
            items=items,
        )

    def redrive_summary(self) -> RedriveSummary:
        """全部重新驱动活动的汇总。"""
        c = self.store.redrive_summary_counts()
        return RedriveSummary(
            total_redrives=c["total_redrives"],
            total_events=c["total_events"],
            succeeded=c["succeeded"],
            in_flight=c["in_flight"],
            failed_again=c["failed_again"],
        )

    # ---- 调度 --------------------------------------------------------------
    async def _dispatch_loop(self) -> None:
        while self._running:
            try:
                keys = self.store.due_keys(iso(utc_now()))
            except Exception:  # noqa: BLE001
                logger.exception("dispatcher: due_keys failed")
                keys = []
            for key in keys:
                self._spawn_worker(key)
            await asyncio.sleep(self.config.poll_interval)

    def _spawn_worker(self, key: str) -> None:
        if key in self._crashed_keys:
            return  # 本进程已模拟崩溃，等待重启恢复
        existing = self._key_workers.get(key)
        if existing is not None and not existing.done():
            return  # 同一维度串行：已有 worker 在跑
        task = asyncio.create_task(self._run_key_worker(key), name=f"worker:{key}")
        self._key_workers[key] = task
        task.add_done_callback(lambda _: self._reap_worker(key, task))

    def _reap_worker(self, key: str, task: asyncio.Task[Any]) -> None:
        if self._key_workers.get(key) is task:
            del self._key_workers[key]
        if task.cancelled():
            return
        exc = task.exception()
        if exc is None:
            return
        if isinstance(exc, SimulatedCrash):
            return  # 测试模拟的崩溃，事件已留 IN_PROGRESS，等待恢复
        logger.exception("worker key=%s crashed", key, exc_info=exc)

    async def _run_key_worker(self, aggregate_key: str) -> None:
        """串行排空某维度的到期事件；队头未到期时退出（稍后由 dispatcher 再唤醒）。"""
        try:
            while self._running:
                record = self.store.claim_next_for_key(aggregate_key, iso(utc_now()))
                if record is None:
                    return  # 无可领取事件，或队头仍在退避等待（队头阻塞）
                await self._process_one(record)
                # 每个事件处理后让出事件循环：保证 dispatcher 能为其他维度 spawn
                # worker（跨维度公平并发），也让退避等待被及时观察到。
                if self._running:
                    await asyncio.sleep(0)
        except SimulatedCrash:
            # 模拟进程在该事件中途死亡：冻结本维度，等待“重启”后恢复。
            self._crashed_keys.add(aggregate_key)
            logger.error("worker key=%s frozen after SIMULATED_CRASH", aggregate_key)

    async def _process_one(self, record: EventRecord) -> None:
        attempt = record.attempts
        event_id = record.event_id
        try:
            payload = json.loads(record.payload_json or "{}")
        except json.JSONDecodeError as exc:
            self._finish_dead(record, attempt, f"payload not valid JSON: {exc}")
            return

        handler = self.registry.get(record.event_type)
        if handler is None:
            # 未注册类型 = 确定性失败：不重试，直接死信并保留原因。
            self._finish_dead(
                record, attempt,
                f"no handler registered for event_type={record.event_type!r}",
            )
            return

        ctx = _RuntimeHandleContext(self, record, attempt)
        decision = HandleDecision.RETRYABLE
        try:
            await asyncio.wait_for(handler(payload, ctx), timeout=self.config.handler_timeout)
            decision = HandleDecision.OK
        except SimulatedCrash:
            # 故意不做任何状态流转：事件保持 IN_PROGRESS，等待重启恢复。
            # worker 协程随异常终止（同维度暂停推进），其余维度不受影响。
            logger.error(
                "event=%s attempt=%d decision=SIMULATED_CRASH left IN_PROGRESS "
                "(effects not committed); worker terminated awaiting recovery",
                event_id, attempt,
            )
            raise
        except asyncio.TimeoutError as exc:
            decision = HandleDecision.RETRYABLE
            error = f"handler timeout after {self.config.handler_timeout}s"
            self._handle_failure(record, attempt, decision, error, exc)
            return
        except NonRetryableError as exc:
            decision = HandleDecision.NON_RETRYABLE
            self._finish_dead(record, attempt, f"non-retryable: {exc}")
            return
        except HandlerError as exc:
            # RetryableError 或其它带标记的可重试异常
            decision = HandleDecision.RETRYABLE if getattr(exc, "retryable", False) \
                else HandleDecision.NON_RETRYABLE
            if decision == HandleDecision.NON_RETRYABLE:
                self._finish_dead(record, attempt, f"non-retryable: {exc}")
                return
            error = f"retryable: {exc}"
            self._handle_failure(record, attempt, decision, error, exc)
            return
        except Exception as exc:  # noqa: BLE001 —— 未预期异常按不可重试处理
            decision = HandleDecision.NON_RETRYABLE
            self._finish_dead(record, attempt, f"unexpected error: {exc!r}")
            return

        # 成功路径：副作用 + COMPLETED 同事务提交（崩溃钩子在提交前触发）
        if event_id in self.crash_after_effects:
            logger.error(
                "event=%s attempt=%d decision=SIMULATED_CRASH effects_pending=%d "
                "left IN_PROGRESS; awaiting recovery", event_id, attempt,
                len(ctx.effects),
            )
            raise SimulatedCrash(event_id)

        try:
            self.store.mark_completed(
                event_id, attempt, ctx.effects,
                apply_account_delta=self.store.apply_account_delta,
            )
        except Exception as exc:  # noqa: BLE001 —— 唯一约束冲突 = 副作用此前已生效
            # 重放：副作用已存在。幂等化处理：不再重复执行副作用，直接置 COMPLETED。
            existing = self.store.get_event(event_id)
            if existing is not None and existing.status == EventStatus.COMPLETED:
                logger.info(
                    "event=%s attempt=%d decision=REPLAY already COMPLETED, "
                    "side-effect suppressed", event_id, attempt,
                )
                return
            self._handle_failure(
                record, attempt, HandleDecision.RETRYABLE,
                f"commit conflict: {exc!r}", exc,
            )
            return

        logger.info(
            "event=%s key=%s attempt=%d decision=%s effects=%d -> COMPLETED",
            event_id, record.aggregate_key, attempt, decision.value, len(ctx.effects),
        )

    # ---- 失败路由 ----------------------------------------------------------
    def _handle_failure(
        self,
        record: EventRecord,
        attempt: int,
        decision: HandleDecision,
        error: str,
        exc: BaseException | None,
    ) -> None:
        if attempt >= record.max_attempts:
            self._finish_dead(
                record, attempt,
                f"{error} | retries exhausted after {attempt}/{record.max_attempts}",
            )
            return
        delay = self.config.backoff.delay_for(attempt)
        not_before = utc_now() + timedelta(seconds=delay)
        self.store.mark_retry_wait(record.event_id, attempt, iso(not_before), error)
        logger.warning(
            "event=%s key=%s attempt=%d decision=%s reason=%r -> RETRY_WAIT "
            "next_not_before=%s (%s)",
            record.event_id, record.aggregate_key, attempt, decision.value,
            error, iso(not_before), self.config.backoff.describe(attempt),
        )

    def _finish_dead(self, record: EventRecord, attempt: int, error: str) -> None:
        self.store.mark_dead_letter(record.event_id, attempt, error)
        logger.error(
            "event=%s key=%s attempt=%d decision=DEAD_LETTER reason=%r",
            record.event_id, record.aggregate_key, attempt, error,
        )
