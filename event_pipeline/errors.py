"""处理器可抛出的业务异常类型，用于区分可重试/不可重试失败。"""

from __future__ import annotations


class EventError(Exception):
    """事件处理失败基类。"""

    retryable: bool = True


class RetryableError(EventError):
    """可重试失败：瞬时错误、超时、下游暂时不可用等。"""

    retryable = True


class NonRetryableError(EventError):
    """不可重试失败：数据语义错误等，直接进入死信。"""

    retryable = False
