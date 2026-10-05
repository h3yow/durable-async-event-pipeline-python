"""HTTP 接口层（保持既有 GET / 对外可用，新增流水线投递/观测接口）。

约定：流水线实例在 lifespan 中挂到 app.state.pipeline；为便于测试，
也允许直接对 create_app(pipeline) 传入已构造实例。
"""
from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.models import EventStatus, RedriveFilter, RejectReason
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

    # ---- 死信重新驱动 --------------------------------------------------------
    @app.post("/dead-letters/redrive")
    async def redrive_dead_letters(request: Request) -> JSONResponse:
        raw = await request.json()
        if not isinstance(raw, dict):
            return JSONResponse(status_code=422,
                                content={"detail": "request body must be a JSON object"})
        batch_id = raw.get("batch_id")
        aggregate_key = raw.get("aggregate_key")
        event_type = raw.get("event_type")
        event_ids = raw.get("event_ids")
        limit = raw.get("limit")
        for name, val in (("batch_id", batch_id), ("aggregate_key", aggregate_key),
                          ("event_type", event_type)):
            if val is not None and (not isinstance(val, str) or not val.strip()):
                return JSONResponse(status_code=422,
                                    content={"detail": f"field '{name}' must be a non-empty string"})
        if event_ids is not None and (
            not isinstance(event_ids, list)
            or any(not isinstance(e, str) or not e.strip() for e in event_ids)
        ):
            return JSONResponse(status_code=422,
                                content={"detail": "field 'event_ids' must be a list of non-empty strings"})
        if limit is not None and (not isinstance(limit, int) or isinstance(limit, bool) or limit < 1):
            return JSONResponse(status_code=422,
                                content={"detail": "field 'limit' must be a positive integer"})

        filt = RedriveFilter(
            aggregate_key=aggregate_key,
            event_type=event_type,
            event_ids=tuple(event_ids) if event_ids else None,
            limit=limit,
        )
        result = await _svc(request).redrive(filt, batch_id=batch_id)
        body: dict[str, Any] = {
            "batch_id": result.batch_id,
            "duplicate": result.duplicate,
            "matched": result.matched,
            "redriven": result.redriven,
            "skipped": result.skipped,
            "truncated": result.truncated,
            "events": [
                {
                    "event_id": e.event_id,
                    "aggregate_key": e.aggregate_key,
                    "event_type": e.event_type,
                    "redriven": e.redriven,
                    **({"detail": e.detail} if e.detail else {}),
                }
                for e in result.events
            ],
        }
        # 重复提交同 batch_id → 200 且 duplicate=true（幂等重放首次结果）；
        # 首次执行 → 202 Accepted（重新投入处理是异步完成的）。
        return JSONResponse(status_code=200 if result.duplicate else 202, content=body)

    @app.get("/dead-letters/redrive/{event_id}")
    async def redrive_outcome(request: Request, event_id: str) -> JSONResponse:
        view = _svc(request).redrive_status(event_id)
        if view is None:
            return JSONResponse(status_code=404, content={
                "event_id": event_id,
                "outcome": "NEVER_REDRIVEN",
                "detail": "event has no redrive audit record",
            })
        return JSONResponse(status_code=200, content={
            "event_id": view.event_id,
            "batch_id": view.batch_id,
            "redriven_at": view.redriven_at,
            "outcome": view.outcome.value,
            "status": view.status.value,
            "attempts": view.attempts,
            "last_error": view.last_error,
        })

    @app.get("/redrive-audit")
    async def redrive_audit(
        request: Request,
        event_id: str | None = None,
        batch_id: str | None = None,
        limit: int = 200,
    ) -> JSONResponse:
        records = _svc(request).list_redrive_records(
            event_id=event_id, batch_id=batch_id, limit=limit,
        )
        return JSONResponse(status_code=200, content={
            "count": len(records),
            "records": [
                {
                    "batch_id": r.batch_id,
                    "event_id": r.event_id,
                    "aggregate_key": r.aggregate_key,
                    "event_type": r.event_type,
                    "prior_error": r.prior_error,
                    "redriven_at": r.redriven_at,
                }
                for r in records
            ],
        })

    @app.get("/redrive-summary")
    async def redrive_summary(request: Request) -> JSONResponse:
        s = _svc(request).redrive_summary()
        return JSONResponse(status_code=200, content={
            "total_batches": s.total_batches,
            "total_redriven": s.total_redriven,
            "succeeded": s.succeeded,
            "in_progress": s.in_progress,
            "failed_again": s.failed_again,
        })


def create_app(pipeline: Pipeline) -> FastAPI:
    """构造自带流水线实例的 app（主要供测试使用）。"""
    app = FastAPI(title="durable-async-event-pipeline")
    app.state.pipeline = pipeline
    register_routes(app)
    return app
