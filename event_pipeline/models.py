"""事件管线的核心领域类型：状态模型、处理结论与重试策略。"""

from __future__ import annotations

import enum
import random
from dataclasses import dataclass
from typing import Any


class EventStatus(str, enum.Enum):
    """事件生命周期状态。

    PENDING  : 已持久化入队，等待派发
    RUNNING  : 已被某个处理者租约认领（崩溃后可恢复）
    SUCCEEDED: 业务副作用已生效（终态）
    FAILED   : 一次尝试失败、等待退避后重试
    DEAD     : 不可重试或超过重试上限，进入死信（终态）
    REJECTED : 结构非法或重复入队，未进入处理流程（终态）
    """

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    DEAD = "dead"
    REJECTED = "rejected"


class FailureKind(str, enum.Enum):
    """失败原因分类（用于决定是否可重试，以及彼此可区分的结论）。"""

    NONE = "none"
    RETRYABLE = "retryable"      # 可重试失败（瞬时错误、超时等）
    NON_RETRYABLE = "non_retryable"  # 不可重试失败（业务显式拒绝）
    MAX_ATTEMPTS = "max_attempts"    # 重试次数耗尽
    DUPLICATE = "duplicate"      # 重复投递/重复入队（幂等命中）
    INVALID = "invalid"          # 结构非法
    CRASH = "crash"              # 租约超时后恢复（上次尝试疑似未完成）


class IngestOutcome(str, enum.Enum):
    """入队接口的可区分结论。"""

    ACCEPTED = "accepted"        # 新事件被接受
    DUPLICATE = "duplicate"      # event_id 已存在，被识别为重放
    INVALID = "invalid"          # 结构非法，被拒绝


@dataclass(frozen=True, slots=True)
class EventRecord:
    """一条事件的持久化快照。"""

    event_id: str
    aggregate_key: str
    seq: int
    payload: dict[str, Any]
    status: EventStatus
    attempts: int
    max_attempts: int
    available_at: float
    lease_owner: str | None
    lease_expires_at: float | None
    last_error: str | None
    failure_kind: FailureKind
    created_at: float
    updated_at: float


@dataclass(frozen=True, slots=True)
class IngestResult:
    """入队结论：相互可区分。"""

    outcome: IngestOutcome
    event_id: str
    reason: str | None = None


@dataclass(slots=True)
class RetryPolicy:
    """指数退避重试策略（可解释：base * factor^(attempt-1)，含抖动上限）。

    第 n 次失败（attempts 已为 n）后的等待：
        delay = min(max_delay, base_delay * backoff_factor ** (n - 1))
    可选地加上不超过 jitter 的随机抖动，避免大量事件同时重试。
    """

    max_attempts: int = 5
    base_delay: float = 0.1
    backoff_factor: float = 2.0
    max_delay: float = 30.0
    jitter: float = 0.0

    def delay_for(self, attempts: int) -> float:
        """返回第 ``attempts`` 次失败后应等待的秒数。"""
        if attempts < 1:
            return 0.0
        raw = self.base_delay * self.backoff_factor ** (attempts - 1)
        delay = min(self.max_delay, raw)
        if self.jitter > 0:
            delay += random.uniform(0, min(self.jitter, delay))
        return delay

    def describe(self, attempts: int) -> str:
        """返回人类可解释的退避说明。"""
        if attempts >= self.max_attempts:
            return (
                f"已尝试 {attempts}/{self.max_attempts} 次，"
                "重试次数耗尽，进入死信"
            )
        return (
            f"第 {attempts}/{self.max_attempts} 次尝试失败，"
            f"等待 {self.delay_for(attempts):.3f}s 后重试"
            f"（指数退避 base={self.base_delay}s factor={self.backoff_factor}"
            f" cap={self.max_delay}s）"
        )
