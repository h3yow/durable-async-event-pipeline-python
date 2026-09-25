"""演示用业务处理器：按 payload 指令模拟成功、可重试失败与不可重试失败。

用于本地端到端复现，真实使用时替换为自定义 handler 即可。
"""

from __future__ import annotations

from typing import Any

from event_pipeline.errors import NonRetryableError, RetryableError
from event_pipeline.models import EventRecord
from event_pipeline.worker import EffectContext


def extract_action(payload: dict[str, Any]) -> str:
    action = payload.get("action", "succeed")
    return action if isinstance(action, str) else "succeed"


async def demo_handler(event: EventRecord, ctx: EffectContext) -> None:
    """根据 payload["action"] 演示三类路径。

    - "reject" : 抛 NonRetryableError，直接死信
    - "fail"   : 抛 RetryableError，按退避重试；
                 可用 payload["fail_times"]=n 表示前 n 次尝试失败、之后成功
    - 其它/"succeed": 正常完成
    """
    payload = event.payload
    action = extract_action(payload)

    if action == "reject":
        raise NonRetryableError(f"payload 指令拒绝处理：{payload.get('reason', '')}")

    if action == "fail":
        fail_times = payload.get("fail_times")
        if not isinstance(fail_times, int) or event.attempts <= fail_times:
            raise RetryableError(
                f"模拟瞬时失败（第 {event.attempts} 次尝试，"
                f"fail_times={payload.get('fail_times')}）"
            )

    # 副作用在此处"生效"，立即向管线声明（崩溃于声明之后也不会重复执行）。
    await ctx.applied()
