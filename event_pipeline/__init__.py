"""本地可运行的持久化异步事件管线（无外部消息中间件）。"""

from event_pipeline.errors import NonRetryableError, RetryableError
from event_pipeline.models import (
    EventRecord,
    EventStatus,
    FailureKind,
    IngestOutcome,
    IngestResult,
    RetryPolicy,
)
from event_pipeline.pipeline import EventPipeline
from event_pipeline.storage import EventStore
from event_pipeline.worker import EffectAlreadyApplied, EffectContext, EventWorker

__all__ = [
    "EventPipeline",
    "EventStore",
    "EventWorker",
    "EffectContext",
    "EffectAlreadyApplied",
    "RetryPolicy",
    "EventRecord",
    "EventStatus",
    "FailureKind",
    "IngestOutcome",
    "IngestResult",
    "RetryableError",
    "NonRetryableError",
]
