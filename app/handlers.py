"""事件处理器注册表与演示处理器。

处理器约定：
    async def handle(payload, ctx) -> None
成功即返回；抛 RetryableError 走退避重试；抛其他异常视为不可重试，直接死信。
处理器通过 ctx 以 (event_id, name) 为幂等键声明副作用；副作用与事件状态
在同一 SQLite 事务内提交，崩溃/重放下业务副作用最多生效一次。

副作用 kind：
    "generic"       仅留痕（effects 表）；
    "account.delta" detail={"account": str, "amount": number}，
                    在提交事务内对账户余额增减（演示真实业务副作用）。
"""
from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from app.errors import NonRetryableError, RetryableError
from app.models import HandleContext

Handler = Callable[[dict[str, Any], HandleContext], Awaitable[None]]

EFF_ACCOUNT_DELTA = "account.delta"
EFF_GENERIC = "generic"


class HandlerRegistry:
    def __init__(self) -> None:
        self._handlers: dict[str, Handler] = {}

    def register(self, event_type: str, handler: Handler) -> None:
        if event_type in self._handlers:
            raise ValueError(f"handler for {event_type!r} already registered")
        self._handlers[event_type] = handler

    def get(self, event_type: str) -> Handler | None:
        return self._handlers.get(event_type)


# ---- 演示处理器 -------------------------------------------------------------
async def account_deposit(payload: dict[str, Any], ctx: HandleContext) -> None:
    """向账户加款：幂等键 (event_id, 'deposit')，重复执行不产生第二次加款。"""
    account = str(payload["account"])
    amount = float(payload["amount"])
    name = "deposit"
    if ctx.already_applied(name):  # 防御性重放识别（正常路径下不会命中）
        return
    ctx.record_effect(
        name,
        {"account": account, "amount": amount},
        kind=EFF_ACCOUNT_DELTA,
    )


# 进程内尝试计数（测试用：让事件前 N 次失败）。真实场景中这代表外部瞬时故障。
_FLAKY_ATTEMPTS: dict[str, int] = {}


async def flaky_service(payload: dict[str, Any], ctx: HandleContext) -> None:
    """前 fail_times 次尝试抛 RetryableError，之后成功（验证退避重试）。"""
    fail_times = int(payload.get("fail_times", 1))
    seen = _FLAKY_ATTEMPTS.get(ctx.event_id, 0) + 1
    _FLAKY_ATTEMPTS[ctx.event_id] = seen
    if seen <= fail_times:
        raise RetryableError(
            f"transient failure {seen}/{fail_times} for {ctx.event_id}"
        )
    ctx.record_effect("flaky-ok", {"attempt": seen})


async def always_bad(payload: dict[str, Any], ctx: HandleContext) -> None:
    """始终抛 NonRetryableError（验证不可重试即死信，不浪费重试次数）。"""
    raise NonRetryableError(f"permanent business rejection for {ctx.event_id}")


async def unknown_type_handler(payload: dict[str, Any], ctx: HandleContext) -> None:
    raise NonRetryableError(f"no handler registered for event type")


def reset_flaky_attempts() -> None:
    """测试隔离辅助：清空 flaky 处理器的内存尝试计数。"""
    _FLAKY_ATTEMPTS.clear()


def build_default_registry() -> HandlerRegistry:
    reg = HandlerRegistry()
    reg.register("account.deposit", account_deposit)
    reg.register("service.flaky", flaky_service)
    reg.register("service.always_bad", always_bad)
    return reg
