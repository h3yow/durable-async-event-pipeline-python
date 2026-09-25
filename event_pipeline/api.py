"""FastAPI 对外接口：入队、查询事件/死信、状态统计。

保留既有 "/" 健康检查；worker 随应用生命周期启停，接口在关停/恢复期间保持可用。
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from event_pipeline.handlers import demo_handler
from event_pipeline.models import EventRecord, EventStatus, IngestOutcome
from event_pipeline.pipeline import EventPipeline

logger = logging.getLogger("event_pipeline.api")


class PublishRequest(BaseModel):
    event_id: str = Field(min_length=1)
    aggregate_key: str = Field(min_length=1)
    payload: dict[str, Any]
    max_attempts: int | None = Field(default=None, ge=1)


def create_app(pipeline: EventPipeline | None = None) -> FastAPI:
    """构造 FastAPI 应用并挂载事件管线路由。"""
    pipe = pipeline

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if pipe is not None:
            await pipe.start()
            logger.info("事件管线 worker 随应用启动")
        yield
        if pipe is not None:
            await pipe.stop(drain=True)
            pipe.close()
            logger.info("事件管线 worker 随应用关停（在途事件已 drain）")

    app = FastAPI(
        title="本地持久化异步事件管线",
        version="0.1.0",
        lifespan=lifespan,
    )

    @app.get("/")
    def read_root():
        return {"Hello": "World", "service": "event-pipeline", "status": "ok"}

    @app.post("/events")
    def publish_events(req: PublishRequest):
        if pipe is None:
            raise HTTPException(503, "管线未配置")
        result = pipe.publish(
            req.event_id, req.aggregate_key, req.payload, req.max_attempts
        )
        body = {
            "event_id": result.event_id,
            "outcome": result.outcome.value,
            "reason": result.reason,
        }
        if result.outcome is IngestOutcome.ACCEPTED:
            body["event"] = _record_to_dict(pipe.get_event(result.event_id), pipe)
            return body
        # 重复与非法必须可区分：409 vs 422
        if result.outcome is IngestOutcome.DUPLICATE:
            existing = pipe.get_event(result.event_id)
            body["existing_status"] = (
                existing.status.value if existing is not None else None
            )
            raise HTTPException(status_code=409, detail=body)
        raise HTTPException(status_code=422, detail=body)

    @app.get("/events/{event_id}")
    def get_event(event_id: str):
        if pipe is None:
            raise HTTPException(503, "管线未配置")
        event = pipe.get_event(event_id)
        if event is None:
            raise HTTPException(404, "事件不存在")
        return _record_to_dict(event, pipe)

    @app.get("/aggregates/{aggregate_key}/events")
    def aggregate_events(aggregate_key: str):
        if pipe is None:
            raise HTTPException(503, "管线未配置")
        return {
            "aggregate_key": aggregate_key,
            "events": [
                _record_to_dict(e, pipe)
                for e in pipe.list_by_aggregate(aggregate_key)
            ],
        }

    @app.get("/events")
    def events(status: EventStatus | None = None):
        if pipe is None:
            raise HTTPException(503, "管线未配置")
        records = (
            pipe.list_status(status) if status is not None
            else pipe.store.all_events()
        )
        return {"events": [_record_to_dict(e, pipe) for e in records]}

    @app.get("/dead-letters")
    def dead_letters():
        if pipe is None:
            raise HTTPException(503, "管线未配置")
        return {"events": [_record_to_dict(e, pipe) for e in pipe.list_dead_letters()]}

    @app.get("/stats")
    def stats():
        if pipe is None:
            raise HTTPException(503, "管线未配置")
        return {"states": pipe.stats()}

    @app.get("/health")
    def health():
        return {"status": "ok", "states": pipe.stats() if pipe else None}

    return app


def _record_to_dict(event: EventRecord, pipe: EventPipeline) -> dict[str, Any]:
    return {
        "event_id": event.event_id,
        "aggregate_key": event.aggregate_key,
        "seq": event.seq,
        "payload": event.payload,
        "status": event.status.value,
        "attempts": event.attempts,
        "max_attempts": event.max_attempts,
        "available_at": event.available_at,
        "lease_owner": event.lease_owner,
        "lease_expires_at": event.lease_expires_at,
        "last_error": event.last_error,
        "failure_kind": event.failure_kind.value,
        "created_at": event.created_at,
        "updated_at": event.updated_at,
        "effect_applied": pipe.effect_applied(event.event_id),
    }


def create_default_app(db_path: str = "events.db") -> FastAPI:
    """main.py 使用：默认数据库 + demo handler + worker 自启。"""
    pipeline = EventPipeline(db_path, handler=demo_handler)
    return create_app(pipeline)
