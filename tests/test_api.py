"""HTTP 对外接口：既有 GET / 可用；投递/重复/非法、状态与死信可查询。"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.api import create_app
from app.handlers import reset_flaky_attempts
from app.models import EventStatus
from app.pipeline import Pipeline
from tests.conftest import make_config


@pytest.fixture
def client(store, registry):
    import asyncio
    import threading

    reset_flaky_attempts()
    pipeline = Pipeline(store, registry, make_config())
    app = create_app(pipeline)

    loop = asyncio.new_event_loop()
    ready = threading.Event()

    def loop_thread():
        asyncio.set_event_loop(loop)
        loop.run_until_complete(pipeline.start())   # 启动完成后再放行，消除竞态
        ready.set()
        loop.run_forever()

    t = threading.Thread(target=loop_thread, daemon=True)
    t.start()
    ready.wait(timeout=2)

    with TestClient(app) as c:
        try:
            yield c
        finally:
            fut = asyncio.run_coroutine_threadsafe(pipeline.stop(), loop)
            fut.result(timeout=3)
            loop.call_soon_threadsafe(loop.stop)
            t.join(timeout=2)
            loop.close()
    reset_flaky_attempts()


def deposit(eid, key="acct:a", account="a", amount=10):
    return {"event_id": eid, "aggregate_key": key,
            "event_type": "account.deposit",
            "payload": {"account": account, "amount": amount}}


def test_legacy_root_endpoint_unchanged(client):
    r = client.get("/")
    assert r.status_code == 200
    assert r.json() == {"Hello": "World"}        # 既有对外接口保持可用


def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["dispatcher_running"] is True


def test_enqueue_status_and_duplicate_status_codes(client):
    r1 = client.post("/events", json=deposit("h1"))
    assert r1.status_code == 202
    assert r1.json()["status"] == "PENDING"

    r2 = client.post("/events", json=deposit("h1"))
    assert r2.status_code == 409                  # 重复重放
    assert r2.json()["reason"] == "DUPLICATE"


def test_invalid_payload_status_code(client):
    bad = deposit("h2", amount=-3)
    r = client.post("/events", json=bad)
    assert r.status_code == 422
    assert r.json()["reason"] == "INVALID_PAYLOAD"

    bad_env = {"event_id": "h3"}                  # 信封缺字段
    r2 = client.post("/events", json=bad_env)
    assert r2.status_code == 422
    assert r2.json()["reason"] == "INVALID_ENVELOPE"


def test_event_status_transitions_observable(client):
    import time

    client.post("/events", json=deposit("h4"))
    deadline = time.time() + 3
    status = None
    while time.time() < deadline:
        status = client.get("/events/h4").json()["status"]
        if status == "COMPLETED":
            break
        time.sleep(0.01)
    assert status == "COMPLETED"
    body = client.get("/events/h4").json()
    assert body["attempts"] >= 1
    assert body["event_type"] == "account.deposit"

    assert client.get("/events/does-not-exist").status_code == 404


def test_aggregate_listing_and_dead_letters_query(client):
    import time

    client.post("/events", json=deposit("o1", key="acct:z", account="z"))
    client.post("/events", json={
        "event_id": "o2", "aggregate_key": "acct:z",
        "event_type": "service.always_bad", "payload": {},
    })
    deadline = time.time() + 3
    while time.time() < deadline:
        dl = client.get("/dead-letters").json()
        if dl["count"] == 1:
            break
        time.sleep(0.01)

    agg = client.get("/aggregates/acct:z/events").json()
    assert agg["count"] == 2
    statuses = {e["event_id"]: e["status"] for e in agg["events"]}
    assert statuses["o1"] == "COMPLETED"
    assert statuses["o2"] == "DEAD_LETTER"
    # 顺序：seq 递增
    assert agg["events"][0]["seq"] < agg["events"][1]["seq"]

    dl = client.get("/dead-letters").json()["dead_letters"][0]
    assert dl["event_id"] == "o2"
    assert "non-retryable" in dl["last_error"]
    assert dl["attempts"] == 1
