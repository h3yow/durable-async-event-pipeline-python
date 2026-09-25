"""基于 SQLite 的事件持久化存储（单文件、零外部依赖）。

承担：事件入队去重、顺序序号、租约认领、状态流转、
死信原因留存，以及业务副作用的"仅一次"登记。
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path

from event_pipeline.models import (
    EventRecord,
    EventStatus,
    FailureKind,
    IngestOutcome,
    IngestResult,
)

# 已终结状态：不再参与派发；同聚合后续事件可越过它们继续推进。
_TERMINAL = (
    EventStatus.SUCCEEDED.value,
    EventStatus.DEAD.value,
    EventStatus.REJECTED.value,
)
_DISPATCHABLE = (EventStatus.PENDING.value, EventStatus.FAILED.value)


class EventStore:
    """SQLite 事件存储。所有写操作都在事务中完成，进程重启后状态确定。"""

    def __init__(self, path: str | Path = "events.db") -> None:
        self._path = str(path)
        self._lock = threading.RLock()
        self._conn = self._connect()
        self._init_schema()

    # ------------------------------------------------------------------ 基础
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self._path,
            check_same_thread=False,
            isolation_level=None,  # 显式事务（BEGIN IMMEDIATE）
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=5000")
        if self._path != ":memory:":
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=FULL")
        return conn

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS events (
                    event_id         TEXT PRIMARY KEY,
                    aggregate_key    TEXT NOT NULL,
                    seq              INTEGER NOT NULL,
                    payload          TEXT NOT NULL,
                    status           TEXT NOT NULL,
                    attempts         INTEGER NOT NULL DEFAULT 0,
                    max_attempts     INTEGER NOT NULL,
                    available_at     REAL NOT NULL,
                    lease_owner      TEXT,
                    lease_expires_at REAL,
                    last_error       TEXT,
                    failure_kind     TEXT NOT NULL DEFAULT 'none',
                    created_at       REAL NOT NULL,
                    updated_at       REAL NOT NULL,
                    UNIQUE(aggregate_key, seq)
                );
                CREATE INDEX IF NOT EXISTS idx_dispatch
                    ON events(status, available_at);
                CREATE INDEX IF NOT EXISTS idx_aggregate
                    ON events(aggregate_key, seq);

                CREATE TABLE IF NOT EXISTS aggregate_seqs (
                    aggregate_key TEXT PRIMARY KEY,
                    next_seq      INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS effects (
                    event_id   TEXT PRIMARY KEY,
                    state      TEXT NOT NULL,           -- pending | applied
                    applied_at REAL,
                    FOREIGN KEY(event_id) REFERENCES events(event_id)
                );
                """
            )

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def __enter__(self) -> "EventStore":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ------------------------------------------------------------------ 入队
    def enqueue(
        self,
        event_id: str,
        aggregate_key: str,
        payload: dict,
        max_attempts: int,
    ) -> IngestResult:
        """持久化入队：结构校验失败返回 INVALID，event_id 重复返回 DUPLICATE。"""
        reason = _validate_envelope(event_id, aggregate_key, payload, max_attempts)
        if reason is not None:
            return IngestResult(IngestOutcome.INVALID, str(event_id), reason)
        now = time.time()
        with self._lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                self._conn.execute(
                    "INSERT INTO aggregate_seqs(aggregate_key, next_seq) "
                    "VALUES (?, 1) ON CONFLICT(aggregate_key) DO NOTHING",
                    (aggregate_key,),
                )
                row = self._conn.execute(
                    "SELECT next_seq FROM aggregate_seqs WHERE aggregate_key=?",
                    (aggregate_key,),
                ).fetchone()
                seq = int(row["next_seq"])
                try:
                    self._conn.execute(
                        "INSERT INTO events(event_id, aggregate_key, seq, payload,"
                        " status, attempts, max_attempts, available_at,"
                        " failure_kind, created_at, updated_at)"
                        " VALUES (?,?,?,?,'pending',0,?,?,'none',?,?)",
                        (
                            event_id,
                            aggregate_key,
                            seq,
                            json.dumps(payload, ensure_ascii=False),
                            max_attempts,
                            now,
                            now,
                            now,
                        ),
                    )
                except sqlite3.IntegrityError:
                    # event_id 已存在（或同键同序号，理论不会）：识别为重放。
                    self._conn.execute("ROLLBACK")
                    existing = self.get(event_id)
                    note = (
                        f"event_id={event_id} 已存在"
                        f"（当前状态={existing.status.value if existing else '?'}），"
                        "识别为重复投递/重放，拒绝再次入队"
                    )
                    return IngestResult(IngestOutcome.DUPLICATE, event_id, note)
                self._conn.execute(
                    "UPDATE aggregate_seqs SET next_seq=? WHERE aggregate_key=?",
                    (seq + 1, aggregate_key),
                )
                self._conn.execute("COMMIT")
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
        return IngestResult(IngestOutcome.ACCEPTED, event_id, None)

    # ------------------------------------------------------------------ 派发
    def _head_due_sql(self, limit_clause: str) -> str:
        """每个聚合只允许"队首未终结"事件被派发，保证严格入队顺序。"""
        return (
            "SELECT * FROM events e WHERE e.status IN (?, ?) AND e.available_at<=?"
            " AND NOT EXISTS ("
            "   SELECT 1 FROM events p WHERE p.aggregate_key=e.aggregate_key"
            "   AND p.seq<e.seq AND p.status NOT IN (?, ?, ?))"
            " ORDER BY e.aggregate_key, e.seq " + limit_clause
        )

    def fetch_due(self, now: float, limit: int) -> list[EventRecord]:
        """取出到点可处理的事件（每个聚合仅队首）。"""
        sql = self._head_due_sql("LIMIT ?")
        with self._lock:
            rows = self._conn.execute(
                sql,
                (*_DISPATCHABLE, now, *_TERMINAL, limit),
            ).fetchall()
        return [self._row_to_record(r) for r in rows]

    def list_due(self, now: float) -> list[EventRecord]:
        """所有到点事件（运维/测试观测用，不受 limit 限制）。"""
        sql = self._head_due_sql("")
        with self._lock:
            rows = self._conn.execute(
                sql, (*_DISPATCHABLE, now, *_TERMINAL)
            ).fetchall()
        return [self._row_to_record(r) for r in rows]

    def fetch_recoverable(self, now: float) -> list[EventRecord]:
        """取出租约过期仍处于 RUNNING 的事件（处理中断/崩溃恢复）。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM events WHERE status='running'"
                " AND lease_expires_at IS NOT NULL AND lease_expires_at<?",
                (now,),
            ).fetchall()
        return [self._row_to_record(r) for r in rows]

    def claim(
        self,
        event_id: str,
        owner: str,
        lease_seconds: float,
        now: float,
    ) -> EventRecord | None:
        """乐观认领事件：仅当仍可派发且为队首时成功（防并发重复认领）。"""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            cur = self._conn.execute(
                "UPDATE events SET status='running', lease_owner=?,"
                " lease_expires_at=?, attempts=attempts+1, updated_at=? "
                "WHERE event_id=? AND status IN (?, ?) AND available_at<=?"
                " AND NOT EXISTS ("
                "   SELECT 1 FROM events p WHERE p.aggregate_key=events.aggregate_key"
                "   AND p.seq<events.seq AND p.status NOT IN (?, ?, ?))",
                (
                    owner,
                    now + lease_seconds,
                    now,
                    event_id,
                    *_DISPATCHABLE,
                    now,
                    *_TERMINAL,
                ),
            )
            if cur.rowcount == 0:
                self._conn.execute("ROLLBACK")
                return None
            row = self._conn.execute(
                "SELECT * FROM events WHERE event_id=?", (event_id,)
            ).fetchone()
            self._conn.execute("COMMIT")
        return self._row_to_record(row)

    # -------------------------------------------------------------- 状态流转
    def mark_succeeded(self, event_id: str, now: float) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE events SET status='succeeded', lease_owner=NULL,"
                " lease_expires_at=NULL, last_error=NULL, failure_kind='none',"
                " available_at=?, updated_at=? WHERE event_id=?",
                (now, now, event_id),
            )

    def mark_retry(
        self,
        event_id: str,
        error: str,
        available_at: float,
        now: float,
    ) -> None:
        """一次尝试失败回到 FAILED，记录原因，attempts 已在 claim 时递增。"""
        with self._lock:
            self._conn.execute(
                "UPDATE events SET status='failed', lease_owner=NULL,"
                " lease_expires_at=NULL, last_error=?, failure_kind='retryable',"
                " available_at=?, updated_at=? WHERE event_id=?",
                (error[:2000], available_at, now, event_id),
            )

    def mark_dead(
        self,
        event_id: str,
        kind: FailureKind,
        error: str,
        now: float,
    ) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE events SET status='dead', lease_owner=NULL,"
                " lease_expires_at=NULL, last_error=?, failure_kind=?,"
                " updated_at=? WHERE event_id=?",
                (error[:2000], kind.value, now, event_id),
            )

    def mark_recovered(self, event: EventRecord, now: float) -> None:
        """租约超时恢复：RUNNING -> FAILED（CRASH），立即可重试。"""
        with self._lock:
            self._conn.execute(
                "UPDATE events SET status='failed', lease_owner=NULL,"
                " lease_expires_at=NULL, failure_kind='crash',"
                " last_error=?, available_at=?, updated_at=? WHERE event_id=?",
                (
                    "处理进程在租约内未完成（中断/崩溃），租约超时恢复",
                    now,
                    now,
                    event.event_id,
                ),
            )

    # ---------------------------------------------------- 副作用仅一次登记
    def begin_effect(self, event_id: str, now: float) -> bool:
        """幂等登记副作用。

        - 首次尝试：插入 pending 行，返回 True（应执行业务）；
        - 已存在 applied 行：副作用此前已生效，返回 False（重放，跳过业务）；
        - 残留 pending 行（上次崩溃于业务执行中）：删除后重新插入，返回 True。
        """
        with self._lock:
            row = self._conn.execute(
                "SELECT state FROM effects WHERE event_id=?", (event_id,)
            ).fetchone()
            if row is not None and row["state"] == "applied":
                return False
            if row is not None:
                self._conn.execute(
                    "DELETE FROM effects WHERE event_id=?", (event_id,)
                )
            self._conn.execute(
                "INSERT INTO effects(event_id, state, applied_at)"
                " VALUES (?,'pending',NULL)",
                (event_id,),
            )
            return True

    def commit_effect(self, event_id: str, now: float) -> None:
        """副作用确认生效：pending -> applied（幂等）。"""
        with self._lock:
            self._conn.execute(
                "UPDATE effects SET state='applied', applied_at=? "
                "WHERE event_id=?",
                (now, event_id),
            )

    def rollback_effect(self, event_id: str) -> None:
        """副作用未成功（异常/崩溃后由 begin 清理）：显式移除 pending 登记。"""
        with self._lock:
            self._conn.execute(
                "DELETE FROM effects WHERE event_id=? AND state='pending'",
                (event_id,),
            )

    def effect_applied(self, event_id: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT state FROM effects WHERE event_id=?", (event_id,)
            ).fetchone()
        return row is not None and row["state"] == "applied"

    # ------------------------------------------------------------------ 查询
    def get(self, event_id: str) -> EventRecord | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM events WHERE event_id=?", (event_id,)
            ).fetchone()
        return self._row_to_record(row) if row is not None else None

    def list_by_aggregate(self, aggregate_key: str) -> list[EventRecord]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM events WHERE aggregate_key=? ORDER BY seq",
                (aggregate_key,),
            ).fetchall()
        return [self._row_to_record(r) for r in rows]

    def list_status(self, status: EventStatus) -> list[EventRecord]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM events WHERE status=? ORDER BY aggregate_key, seq",
                (status.value,),
            ).fetchall()
        return [self._row_to_record(r) for r in rows]

    def stats(self) -> dict[str, int]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT status, COUNT(*) AS n FROM events GROUP BY status"
            ).fetchall()
        counts = {r["status"]: int(r["n"]) for r in rows}
        return {s.value: counts.get(s.value, 0) for s in EventStatus}

    def all_events(self) -> list[EventRecord]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM events ORDER BY aggregate_key, seq"
            ).fetchall()
        return [self._row_to_record(r) for r in rows]

    def count_unfinished(self) -> int:
        """统计尚未到终态的事件数（含退避等待中的 failed）。"""
        placeholders = ", ".join("?" for _ in _TERMINAL)
        with self._lock:
            row = self._conn.execute(
                f"SELECT COUNT(*) AS n FROM events WHERE status NOT IN ({placeholders})",
                _TERMINAL,
            ).fetchone()
        return int(row["n"])

    # ------------------------------------------------------------------ 映射
    def _row_to_record(self, row: sqlite3.Row) -> EventRecord:
        return EventRecord(
            event_id=row["event_id"],
            aggregate_key=row["aggregate_key"],
            seq=int(row["seq"]),
            payload=json.loads(row["payload"]),
            status=EventStatus(row["status"]),
            attempts=int(row["attempts"]),
            max_attempts=int(row["max_attempts"]),
            available_at=float(row["available_at"]),
            lease_owner=row["lease_owner"],
            lease_expires_at=(
                float(row["lease_expires_at"])
                if row["lease_expires_at"] is not None
                else None
            ),
            last_error=row["last_error"],
            failure_kind=FailureKind(row["failure_kind"]),
            created_at=float(row["created_at"]),
            updated_at=float(row["updated_at"]),
        )


def _validate_envelope(
    event_id: object,
    aggregate_key: object,
    payload: object,
    max_attempts: object,
) -> str | None:
    """返回 None 表示合法；否则返回可解释的拒绝原因。"""
    if not isinstance(event_id, str) or not event_id.strip():
        return "event_id 必须是非空字符串"
    if not isinstance(aggregate_key, str) or not aggregate_key.strip():
        return "aggregate_key 必须是非空字符串"
    if not isinstance(payload, dict):
        return "payload 必须是 JSON 对象"
    try:
        json.dumps(payload)
    except (TypeError, ValueError) as exc:
        return f"payload 不是合法 JSON：{exc}"
    if not isinstance(max_attempts, int) or isinstance(max_attempts, bool) or max_attempts < 1:
        return "max_attempts 必须是 >=1 的整数"
    return None
