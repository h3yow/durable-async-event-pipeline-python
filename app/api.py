"""HTTP 接口层（保持既有 GET / 对外可用，新增流水线投递/观测接口）。

约定：流水线实例在 lifespan 中挂到 app.state.pipeline；为便于测试，
也允许直接对 create_app(pipeline) 传入已构造实例。
"""
from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.models import EventStatus, RejectReason
from app.pipeline import Pipeline


def _svc(request: Request) -> Pipeline:
    pipeline = getattr(request.app.state, "pipeline", None)
    if pipeline is None:  # pragma: no cover - 装配错误时给出明确提示
        raise RuntimeError("pipeline not initialised")
    return pipeline


def register_routes(app: FastAPI) -> None:
    @app.get("/")
    def read_root() -> dict[str, str]:
        # 既有对外接口保持不变
        return {"Hello": "World"}

    @app.post("/events")
    async def enqueue_event(request: Request) -> JSONResponse:
        raw = await request.json()
        result = _svc(request).enqueue(raw)
        body: dict[str, Any] = {
            "accepted": result.accepted,
            "event_id": result.event_id,
            "status": result.status.value if result.status else None,
        }
        if result.reason is not None:
            body["reason"] = result.reason.value
            body["detail"] = result.detail
        # 重复重放 → 409；结构非法 → 422；二者状态码与 reason 均可区分
        if result.reason == RejectReason.DUPLICATE:
            return JSONResponse(status_code=409, content=body)
        if result.reason in (RejectReason.INVALID_ENVELOPE, RejectReason.INVALID_PAYLOAD):
            return JSONResponse(status_code=422, content=body)
        return JSONResponse(status_code=202, content=body)

    @app.get("/events/{event_id}")
    async def get_event(request: Request, event_id: str) -> JSONResponse:
        view = _svc(request).get_status(event_id)
        if view is None:
            return JSONResponse(status_code=404, content={"event_id": event_id,
                                                           "status": "NOT_FOUND"})
        return JSONResponse(status_code=200, content={
            "event_id": view.event_id,
            "aggregate_key": view.aggregate_key,
            "event_type": view.event_type,
            "status": view.status.value,
            "attempts": view.attempts,
            "max_attempts": view.max_attempts,
            "seq": view.seq,
            "not_before": view.not_before,
            "last_error": view.last_error,
        })

    @app.get("/aggregates/{aggregate_key}/events")
    async def aggregate_events(request: Request, aggregate_key: str) -> JSONResponse:
        views = _svc(request).list_by_aggregate(aggregate_key)
        return JSONResponse(status_code=200, content={
            "aggregate_key": aggregate_key,
            "count": len(views),
            "events": [
                {
                    "seq": v.seq,
                    "event_id": v.event_id,
                    "event_type": v.event_type,
                    "status": v.status.value,
                    "attempts": v.attempts,
                    "max_attempts": v.max_attempts,
                }
                for v in views
            ],
        })

    @app.get("/dead-letters")
    async def dead_letters(request: Request, limit: int = 100) -> JSONResponse:
        views = _svc(request).list_dead_letters(limit=limit)
        return JSONResponse(status_code=200, content={
            "count": len(views),
            "dead_letters": [
                {
                    "event_id": v.event_id,
                    "aggregate_key": v.aggregate_key,
                    "event_type": v.event_type,
                    "attempts": v.attempts,
                    "max_attempts": v.max_attempts,
                    "last_error": v.last_error,
                    "updated_at": v.updated_at,
                }
                for v in views
            ],
        })

    @app.get("/health")
    async def health(request: Request) -> JSONResponse:
        svc = _svc(request)
        return JSONResponse(status_code=200, content={
            "status": "ok",
            "dispatcher_running": svc.is_running(),
        })


def create_app(pipeline: Pipeline) -> FastAPI:
    """构造自带流水线实例的 app（主要供测试使用）。"""
    app = FastAPI(title="durable-async-event-pipeline")
    app.state.pipeline = pipeline
    register_routes(app)
    return app
