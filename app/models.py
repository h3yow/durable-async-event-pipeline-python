"""领域类型定义：事件、状态机、入队/出队结果与观测视图。

状态模型（确定性、可查询）::

    PENDING ──claim──▶ IN_PROGRESS ──success──▶ COMPLETED
      │                   │
      │                (retryable & attempts < max)
      │                   └──▶ RETRY_WAIT ──due──▶ IN_PROGRESS
      │
      └─(startup / lease sweep 中断恢复)── IN_PROGRESS 重置为 RETRY_WAIT

    IN_PROGRESS / RETRY_WAIT ──non-retryable 或 attempts 耗尽──▶ DEAD_LETTER

另外：
    * 结构非法事件（enqueue 时校验）→ REJECTED（不进入调度，可观测）
    * 重复 event_id → 识别为 DUPLICATE（幂等拒绝，与 REJECTED 可区分）
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class EventStatus(str, Enum):
    """事件在生命周期中的确定状态。"""

    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    RETRY_WAIT = "RETRY_WAIT"
    COMPLETED = "COMPLETED"
    DEAD_LETTER = "DEAD_LETTER"
    REJECTED = "REJECTED"


class RejectReason(str, Enum):
    """入队/处理拒绝的判别原因（重复 vs 非法必须可区分）。"""

    DUPLICATE = "DUPLICATE"                 # event_id 已存在：重放识别
    INVALID_PAYLOAD = "INVALID_PAYLOAD"     # 结构非法（schema 校验失败）
    INVALID_ENVELOPE = "INVALID_ENVELOPE"   # 外层字段缺失/类型错误


class HandleDecision(str, Enum):
    """处理器执行后的判定依据，用于日志与状态迁移。"""

    OK = "OK"
    RETRYABLE = "RETRYABLE"
    NON_RETRYABLE = "NON_RETRYABLE"


@dataclass(frozen=True, slots=True)
class EnqueueRequest:
    event_id: str
    aggregate_key: str
    event_type: str
    payload: dict[str, Any]


@dataclass(slots=True)
class EventRecord:
    seq: int
    event_id: str
    aggregate_key: str
    event_type: str
    payload_json: str
    status: EventStatus
    attempts: int
    max_attempts: int
    created_seq: int
    not_before: str  # ISO8601 UTC，最早可被领取的时间
    last_error: str
    updated_at: str


@dataclass(slots=True)
class EnqueueResult:
    accepted: bool
    event_id: str
    status: EventStatus | None
    reason: RejectReason | None
    detail: str = ""


@dataclass(slots=True)
class StatusView:
    event_id: str
    aggregate_key: str
    event_type: str
    status: EventStatus
    attempts: int
    max_attempts: int
    seq: int
    not_before: str
    last_error: str


@dataclass(slots=True)
class DeadLetterView:
    event_id: str
    aggregate_key: str
    event_type: str
    attempts: int
    max_attempts: int
    last_error: str
    updated_at: str


class RedriveOutcome(str, Enum):
    """重新驱动后单条事件的归类结果（由审计记录 + 事件当前状态推导）。"""

    REDRIVEN = "REDRIVEN"            # 本次调用成功把它从死信重新投入调度
    SKIPPED_NOT_DEAD = "SKIPPED_NOT_DEAD"      # 已不在死信（被并发/重复驱动过）
    SKIPPED_NOT_FOUND = "SKIPPED_NOT_FOUND"    # 指定的事件不存在
    SUCCEEDED = "SUCCEEDED"          # 重新驱动后已成功跑完（COMPLETED）
    IN_FLIGHT = "IN_FLIGHT"          # 重新驱动后仍在排队/重试/处理中
    FAILED_AGAIN = "FAILED_AGAIN"    # 重新驱动后再次失败、回到死信


@dataclass(slots=True)
class RedriveItemResult:
    """一次重新驱动调用中，单条事件的受理结果。"""

    event_id: str
    aggregate_key: str
    event_type: str
    outcome: RedriveOutcome
    detail: str = ""


@dataclass(slots=True)
class RedriveResult:
    """一次重新驱动调用的整体受理结果（同步返回，处理结果异步可查）。"""

    redrive_id: str
    accepted: int
    skipped: int
    truncated: bool          # 命中数量超过批量上限，只受理了前 limit 条
    matched: int             # 按条件命中的死信总数（不受 limit 截断影响）
    items: list[RedriveItemResult] = field(default_factory=list)


@dataclass(slots=True)
class RedriveReportItem:
    """重新驱动审计条目 + 该事件当前推导出的结果归类。"""

    event_id: str
    aggregate_key: str
    event_type: str
    redriven_at: str
    outcome: RedriveOutcome
    event_status: EventStatus | None
    attempts: int


@dataclass(slots=True)
class RedriveReport:
    """某次重新驱动（按 redrive_id）的可查询结果报告。"""

    redrive_id: str
    total: int
    succeeded: int
    in_flight: int
    failed_again: int
    items: list[RedriveReportItem] = field(default_factory=list)


@dataclass(slots=True)
class RedriveSummary:
    """全部重新驱动活动的汇总视图。"""

    total_redrives: int
    total_events: int
    succeeded: int
    in_flight: int
    failed_again: int


@dataclass(slots=True)
class HandleContext:
    """处理器运行上下文：以 event_id 为幂等键登记业务副作用。

    副作用与事件状态在同一个 SQLite 事务内提交（exactly-once 凭证）。
    若在提交前“崩溃”，副作用随事务回滚、事件回到 RETRY_WAIT，重放安全。
    """

    event_id: str
    aggregate_key: str
    attempt: int
    _effect_names: list[str] = field(default_factory=list)

    def record_effect(self, name: str, detail: dict[str, Any], kind: str = "generic") -> None:
        """声明一个业务副作用（真正落库由 Pipeline 在提交事务时完成）。

        同一 (event_id, name) 只可能落库一次；kind 决定提交时的附加动作
        （如 "account.delta" 会在同事务内更新账户余额）。
        """
        raise NotImplementedError

    def already_applied(self, name: str) -> bool:
        """该副作用是否已在先前尝试中生效（重放识别）。"""
        raise NotImplementedError
