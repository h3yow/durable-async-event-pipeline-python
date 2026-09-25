"""可解释的指数退避策略。

公式（attempts_done 为已经完成的尝试次数，从 1 起）::

    raw   = min(base_delay * factor ** (attempts_done - 1), max_delay)
    delay = raw * (1 + jitter * pseudo(attempts_done))     # jitter=0 时完全确定

默认 jitter=0，测试结果可从头复现；需要抖动时再开启。
"""
from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class BackoffPolicy:
    base_delay: float = 0.05      # 第 1 次失败后的等待秒数
    factor: float = 2.0           # 每次失败延迟翻倍
    max_delay: float = 2.0        # 单次延迟上限
    jitter: float = 0.0           # 抖动比例（0 表示完全确定性，便于测试复现）

    def delay_for(self, attempts_done: int) -> float:
        """返回第 attempts_done 次尝试失败后应等待的秒数。"""
        if attempts_done < 1:
            raise ValueError("attempts_done must be >= 1")
        raw = min(self.base_delay * (self.factor ** (attempts_done - 1)), self.max_delay)
        if self.jitter <= 0:
            return raw
        # 基于尝试次数的确定性伪抖动（同一次数永远得到同一结果，可复现）。
        pseudo = 0.5 + 0.5 * math.sin(attempts_done * 12.9898)
        return min(raw * (1.0 + self.jitter * pseudo), self.max_delay)

    def describe(self, attempts_done: int) -> str:
        """返回人类可解释的退避说明，写入日志。"""
        d = self.delay_for(attempts_done)
        return (
            f"backoff: attempt#{attempts_done} failed -> wait {d:.3f}s "
            f"(base={self.base_delay}, factor={self.factor}, cap={self.max_delay})"
        )
