"""领域类型定义：事件、状态机、入队/出队结果与观测视图。

状态模型（确定性、可查询）::

    PENDING ──claim──▶ IN_PROGRESS ──success──▶ COMPLETED
      │                   │
      │                (retryable & attempts < max)
      │                   └──▶ RETRY_WAIT ──due──▶ IN_PROGRESS
      │
      └─(startup / lease sweep 中断恢复)── IN_PROGRESS 重置为 RETRY_WAIT

    IN_PROGRESS / RETRY_WAIT ──non-retryable 或 attempts 耗尽──▶ DEAD_LETTER
                                                                    │
    DEAD_LETTER ──redrive（运维重新驱动）──▶ PENDING（保留原 seq，回到原处理次序）

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


class RedriveOutcome(str, Enum):
    """死信重新驱动之后的结果（由审计记录 + 事件当前状态推导，可查询）。"""

    SUCCEEDED = "SUCCEEDED"        # 重新驱动后成功：事件已 COMPLETED
    IN_PROGRESS = "IN_PROGRESS"    # 重新驱动后仍在排队/退避重试
    FAILED_AGAIN = "FAILED_AGAIN"  # 重新驱动后再次失败：又回到 DEAD_LETTER


@dataclass(frozen=True, slots=True)
class RedriveFilter:
    """挑选待重新驱动死信的条件（可组合；全空表示“全部死信”，仍受上限约束）。"""

    aggregate_key: str | None = None
    event_type: str | None = None
    event_ids: tuple[str, ...] | None = None
    limit: int | None = None


@dataclass(slots=True)
class RedriveEventResult:
    """批次内单个事件的重新驱动结果。"""

    event_id: str
    aggregate_key: str
    event_type: str
    redriven: bool          # False = 被跳过（已不在死信/被并发批次抢走）
    detail: str = ""


@dataclass(slots=True)
class RedriveResult:
    """一次重新驱动批次的汇总结果（重复提交同 batch_id 时原样返回）。"""

    batch_id: str
    duplicate: bool          # True = 该 batch_id 已执行过，本次为重放
    matched: int             # 过滤条件命中的死信数（上限截断后）
    redriven: int            # 实际重新入队数
    skipped: int             # 命中但被跳过（并发竞争下已不在死信）
    truncated: bool          # 命中数因到达批量上限被截断
    events: list[RedriveEventResult] = field(default_factory=list)


@dataclass(slots=True)
class RedriveRecord:
    """审计：某事件在某批次中被重新驱动过（落库留痕）。"""

    batch_id: str
    event_id: str
    aggregate_key: str
    event_type: str
    prior_error: str         # 重新驱动前最后一次失败原因
    redriven_at: str


@dataclass(slots=True)
class RedriveStatusView:
    """单事件“重新驱动后结果”的查询视图。"""

    event_id: str
    batch_id: str
    redriven_at: str
    outcome: RedriveOutcome
    status: EventStatus
    attempts: int
    last_error: str


@dataclass(slots=True)
class RedriveSummary:
    """重新驱动汇总（全部批次）。"""

    total_batches: int
    total_redriven: int
    succeeded: int
    in_progress: int
    failed_again: int


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
