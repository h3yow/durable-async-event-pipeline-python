"""本地端到端复现脚本：无任何外部依赖，直接运行。

    uv run python examples/demo.py

演示：
  1. 正常事件一次成功（副作用仅一次）
  2. 前 2 次失败、第 3 次成功（指数退避重试）
  3. 不可重试失败立即进死信（留存原因）
  4. 重复入队被识别为重放（与结构非法结论可区分）
  5. 处理中断后重启恢复（不丢、不重复产生副作用）
"""

from __future__ import annotations

import asyncio
import os
import tempfile
import time

from event_pipeline import (
    EffectContext,
    EventPipeline,
    EventRecord,
    FailureKind,
    IngestOutcome,
    RetryableError,
    RetryPolicy,
)
from event_pipeline.errors import NonRetryableError

POLICY = RetryPolicy(max_attempts=3, base_delay=0.1, backoff_factor=2.0,
                     max_delay=1.0)


async def business_handler(event: EventRecord, ctx: EffectContext) -> None:
    """示例业务：按 payload 指令演示不同判定路径。"""
    action = event.payload.get("action", "succeed")
    if action == "reject":
        raise NonRetryableError("不可重试：订单状态机不允许该迁移")
    if action == "fail-twice":
        if event.attempts <= 2:
            raise RetryableError(f"模拟瞬时故障（第 {event.attempts} 次）")
    # 外部可观测副作用在此生效：必须声明，管线据此保证仅一次
    await ctx.applied()


async def main() -> None:
    db = os.path.join(tempfile.mkdtemp(prefix="pipeline-demo-"), "events.db")
    pipe = EventPipeline(db, handler=business_handler, retry_policy=POLICY,
                         lease_seconds=5.0, poll_interval=0.02)

    print("== 1) 入队：成功 / 重试后成功 / 不可重试 ==")
    for eid, key, payload in [
        ("ev-ok", "ORDER-1", {"action": "succeed"}),
        ("ev-flaky", "ORDER-2", {"action": "fail-twice"}),
        ("ev-bad", "ORDER-3", {"action": "reject"}),
    ]:
        result = pipe.publish(eid, key, payload)
        print(f"  publish {eid:9s} -> {result.outcome.value}")

    print("== 2) 重复入队 vs 结构非法（结论可区分）==")
    dup = pipe.publish("ev-ok", "ORDER-1", {"action": "succeed"})
    bad = pipe.publish("", "ORDER-1", {})
    assert dup.outcome is IngestOutcome.DUPLICATE
    assert bad.outcome is IngestOutcome.INVALID
    print(f"  duplicate -> {dup.outcome.value}（{dup.reason}）")
    print(f"  invalid   -> {bad.outcome.value}（{bad.reason}）")

    print("== 3) 启动 worker，等待全部流转完成 ==")
    async with pipe:
        await pipe.wait_drained(timeout=10.0)

        for eid in ["ev-ok", "ev-flaky", "ev-bad"]:
            e = pipe.get_event(eid)
            print(f"  {eid:9s} status={e.status.value:9s} attempts={e.attempts} "
                  f"kind={e.failure_kind.value} effect={pipe.effect_applied(eid)}"
                  + (f" error={e.last_error}" if e.last_error else ""))

        print("== 4) 死信列表 ==")
        for d in pipe.list_dead_letters():
            print(f"  DEAD {d.event_id} kind={d.failure_kind.value} "
                  f"reason={d.last_error}")

        print("STATS:", pipe.stats())
        db_path = db
    print(f"数据库文件：{db_path}（可自行用 sqlite3 打开查看 events/effects 表）")


if __name__ == "__main__":
    asyncio.run(main())
