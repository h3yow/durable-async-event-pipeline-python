"""处理器可抛出的异常类型，用于区分可重试/不可重试失败。"""
from __future__ import annotations


class HandlerError(Exception):
    """业务处理失败基类。"""

    retryable: bool = False


class RetryableError(HandlerError):
    """瞬时失败：按退避策略重试，达到上限进入死信。"""

    retryable = True


class NonRetryableError(HandlerError):
    """确定性失败：不重试，直接进入死信。"""

    retryable = False
