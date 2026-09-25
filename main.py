"""本地可运行入口：uvicorn main:app —— 不依赖任何外部服务。

存储默认使用本地 SQLite 文件（可用环境变量 PIPELINE_DB 覆盖，":memory:" 亦可）。
"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from app.api import register_routes
from app.handlers import build_default_registry
from app.pipeline import Pipeline
from app.store import EventStore


@asynccontextmanager
async def lifespan(app: FastAPI):
    db_path = os.environ.get("PIPELINE_DB", "pipeline.db")
    store = EventStore(db_path)
    pipeline = Pipeline(store, build_default_registry())
    app.state.pipeline = pipeline
    await pipeline.start()
    try:
        yield
    finally:
        await pipeline.stop()
        store.close()


app = FastAPI(lifespan=lifespan)
register_routes(app)


if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000)
