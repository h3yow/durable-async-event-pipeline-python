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

    @app.post("/dead-letters/redrive")
    async def redrive_dead_letters(request: Request) -> JSONResponse:
        raw = await request.json()
        if not isinstance(raw, dict):
            return JSONResponse(status_code=422, content={
                "accepted": 0, "detail": "body must be a JSON object",
            })
        event_ids = raw.get("event_ids")
        if event_ids is not None and not (
            isinstance(event_ids, list)
            and all(isinstance(e, str) for e in event_ids)
        ):
            return JSONResponse(status_code=422, content={
                "accepted": 0, "detail": "event_ids must be a list of strings",
            })
        limit = raw.get("limit")
        if limit is not None and not (isinstance(limit, int) and limit > 0):
            return JSONResponse(status_code=422, content={
                "accepted": 0, "detail": "limit must be a positive integer",
            })
        result = _svc(request).redrive_dead_letters(
            redrive_id=raw.get("redrive_id"),
            aggregate_key=raw.get("aggregate_key"),
            event_type=raw.get("event_type"),
            event_ids=event_ids,
            limit=limit,
        )
        return JSONResponse(status_code=202, content={
            "redrive_id": result.redrive_id,
            "accepted": result.accepted,
            "skipped": result.skipped,
            "matched": result.matched,
            "truncated": result.truncated,
            "items": [
                {
                    "event_id": i.event_id,
                    "aggregate_key": i.aggregate_key,
                    "event_type": i.event_type,
                    "outcome": i.outcome.value,
                    "detail": i.detail,
                }
                for i in result.items
            ],
        })

    @app.get("/redrives/{redrive_id}")
    async def redrive_report(request: Request, redrive_id: str) -> JSONResponse:
        report = _svc(request).redrive_report(redrive_id)
        if report is None:
            return JSONResponse(status_code=404, content={
                "redrive_id": redrive_id, "status": "NOT_FOUND",
            })
        return JSONResponse(status_code=200, content={
            "redrive_id": report.redrive_id,
            "total": report.total,
            "succeeded": report.succeeded,
            "in_flight": report.in_flight,
            "failed_again": report.failed_again,
            "items": [
                {
                    "event_id": i.event_id,
                    "aggregate_key": i.aggregate_key,
                    "event_type": i.event_type,
                    "redriven_at": i.redriven_at,
                    "outcome": i.outcome.value,
                    "event_status": i.event_status.value if i.event_status else None,
                    "attempts": i.attempts,
                }
                for i in report.items
            ],
        })

    @app.get("/redrives")
    async def redrive_summary(request: Request) -> JSONResponse:
        s = _svc(request).redrive_summary()
        return JSONResponse(status_code=200, content={
            "total_redrives": s.total_redrives,
            "total_events": s.total_events,
            "succeeded": s.succeeded,
            "in_flight": s.in_flight,
            "failed_again": s.failed_again,
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
