"""SQLite 持久层：事件表、业务副作用表、账户表演示表。

关键保障：
    * events.event_id  UNIQUE         —— 重复入队/重放可识别；
    * effects(event_id, name) UNIQUE  —— 同一事件同一副作用在并发/重放下
      数据库层面只能落一次（副作用幂等的最终防线）；
    * 领取与状态流转全部使用“条件 UPDATE ... WHERE 状态=预期”，
      状态竞争时影响行数为 0，调用方据此判定是否抢到；
    * mark_completed 将“写副作用 + 置 COMPLETED”放在同一事务，
      崩溃发生在提交前则整体回滚（事件随后恢复为 RETRY_WAIT，重放安全）。
"""
from __future__ import annotations

import json
import sqlite3
import threading
from typing import Any

from app.models import DeadLetterView, EventRecord, EventStatus, StatusView

SCHEMA_VERSION = 1

_DDL = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    aggregate_key TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}',
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 5,
    not_before TEXT NOT NULL,
    last_error TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE INDEX IF NOT EXISTS ix_events_due
    ON events(status, not_before);
CREATE INDEX IF NOT EXISTS ix_events_key_seq
    ON events(aggregate_key, seq);

CREATE TABLE IF NOT EXISTS effects (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL,
    name TEXT NOT NULL,
    detail_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    UNIQUE(event_id, name)
);

CREATE TABLE IF NOT EXISTS account_balances (
    account TEXT PRIMARY KEY,
    balance REAL NOT NULL DEFAULT 0
);
"""


class EventStore:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._conn: sqlite3.Connection | None = None
        # 写事务串行锁：保证“检查后写入”与条件 UPDATE 在多线程并发提交下的
        # 原子语义（asyncio 单线程内无竞争；锁仅供测试模拟并发投递时使用）。
        self._write_lock = threading.RLock()

    # ---- 生命周期 ----------------------------------------------------------
    def connect(self) -> None:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=5000")
        self._conn = conn
        conn.executescript(_DDL)
        conn.execute(
            "INSERT INTO schema_meta(key, value) VALUES('version', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (str(SCHEMA_VERSION),),
        )
        conn.commit()

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    @property
    def conn(self) -> sqlite3.Connection:
        assert self._conn is not None, "EventStore.connect() 尚未调用"
        return self._conn

    @property
    def write_lock(self) -> threading.RLock:
        return self._write_lock

    @staticmethod
    def _row_to_record(r: sqlite3.Row) -> EventRecord:
        return EventRecord(
            seq=r["seq"],
            event_id=r["event_id"],
            aggregate_key=r["aggregate_key"],
            event_type=r["event_type"],
            payload_json=r["payload_json"],
            status=EventStatus(r["status"]),
            attempts=r["attempts"],
            max_attempts=r["max_attempts"],
            created_seq=r["seq"],
            not_before=r["not_before"],
            last_error=r["last_error"] or "",
            updated_at=r["updated_at"],
        )

    @staticmethod
    def _row_to_status(r: sqlite3.Row) -> StatusView:
        return StatusView(
            event_id=r["event_id"],
            aggregate_key=r["aggregate_key"],
            event_type=r["event_type"],
            status=EventStatus(r["status"]),
            attempts=r["attempts"],
            max_attempts=r["max_attempts"],
            seq=r["seq"],
            not_before=r["not_before"],
            last_error=r["last_error"] or "",
        )

    # ---- 入队 / 查询 -------------------------------------------------------
    def event_exists(self, event_id: str) -> bool:
        with self._write_lock:
            row = self.conn.execute(
                "SELECT 1 FROM events WHERE event_id=? LIMIT 1", (event_id,)
            ).fetchone()
            return row is not None

    def insert_pending(
        self,
        event_id: str,
        aggregate_key: str,
        event_type: str,
        payload_json: str,
        max_attempts: int,
        not_before_iso: str,
    ) -> None:
        with self._write_lock:
            self.conn.execute(
                "INSERT INTO events(event_id, aggregate_key, event_type, payload_json,"
                " status, attempts, max_attempts, not_before) "
                "VALUES(?,?,?,?,?,0,?,?)",
                (
                    event_id,
                    aggregate_key,
                    event_type,
                    payload_json,
                    EventStatus.PENDING.value,
                    max_attempts,
                    not_before_iso,
                ),
            )
            self.conn.commit()

    def insert_rejected(
        self,
        event_id: str,
        aggregate_key: str,
        event_type: str,
        payload_json: str,
        detail: str,
    ) -> None:
        # REJECTED 同样持久化以便观测；event_id 冲突（极端情况）时静默保留首条。
        self.conn.execute(
            "INSERT OR IGNORE INTO events(event_id, aggregate_key, event_type,"
            " payload_json, status, attempts, max_attempts, not_before, last_error)"
            " VALUES(?,?,?,?,?,0,1,strftime('%Y-%m-%dT%H:%M:%fZ','now'),?)",
            (
                event_id,
                aggregate_key,
                event_type,
                payload_json,
                EventStatus.REJECTED.value,
                detail,
            ),
        )
        self.conn.commit()

    def get_event(self, event_id: str) -> EventRecord | None:
        row = self.conn.execute(
            "SELECT * FROM events WHERE event_id=?", (event_id,)
        ).fetchone()
        return self._row_to_record(row) if row else None

    def status_view(self, event_id: str) -> StatusView | None:
        row = self.conn.execute(
            "SELECT * FROM events WHERE event_id=?", (event_id,)
        ).fetchone()
        return self._row_to_status(row) if row else None

    def list_by_aggregate(self, aggregate_key: str) -> list[StatusView]:
        rows = self.conn.execute(
            "SELECT * FROM events WHERE aggregate_key=? ORDER BY seq", (aggregate_key,)
        ).fetchall()
        return [self._row_to_status(r) for r in rows]

    def list_dead_letters(self, limit: int = 100) -> list[DeadLetterView]:
        rows = self.conn.execute(
            "SELECT * FROM events WHERE status=? ORDER BY seq LIMIT ?",
            (EventStatus.DEAD_LETTER.value, limit),
        ).fetchall()
        return [
            DeadLetterView(
                event_id=r["event_id"],
                aggregate_key=r["aggregate_key"],
                event_type=r["event_type"],
                attempts=r["attempts"],
                max_attempts=r["max_attempts"],
                last_error=r["last_error"] or "",
                updated_at=r["updated_at"],
            )
            for r in rows
        ]

    # ---- 调度 --------------------------------------------------------------
    def due_keys(self, now_iso: str) -> list[str]:
        """返回存在“可领取（含队头阻塞后的后续到期事件）”事件的聚合维度。

        注意：某维度只要队头尚未到期，后续事件即使到期也不能越过它（保序）。
        这里只返回维度集合；严格的队头判定在 claim_next_for_key 内完成。
        """
        rows = self.conn.execute(
            "SELECT DISTINCT aggregate_key FROM events "
            "WHERE status IN (?, ?) AND not_before <= ?",
            (EventStatus.PENDING.value, EventStatus.RETRY_WAIT.value, now_iso),
        ).fetchall()
        return [r["aggregate_key"] for r in rows]

    def claim_next_for_key(self, aggregate_key: str, now_iso: str) -> EventRecord | None:
        """领取某聚合维度上“最靠前且到期”的事件。

        只在该维度最靠前的可调度事件到期时领取（队头阻塞 = 保序核心）。
        条件 UPDATE 保证并发竞争下只有一个领取者成功。
        """
        with self._write_lock:
            conn = self.conn
            head = conn.execute(
                "SELECT * FROM events WHERE aggregate_key=? AND status IN (?, ?) "
                "ORDER BY seq LIMIT 1",
                (
                    aggregate_key,
                    EventStatus.PENDING.value,
                    EventStatus.RETRY_WAIT.value,
                ),
            ).fetchone()
            if head is None or head["not_before"] > now_iso:
                return None
            cur = conn.execute(
                "UPDATE events SET status=?, attempts=attempts+1, "
                "updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') "
                "WHERE seq=? AND status IN (?, ?)",
                (
                    EventStatus.IN_PROGRESS.value,
                    head["seq"],
                    EventStatus.PENDING.value,
                    EventStatus.RETRY_WAIT.value,
                ),
            )
            conn.commit()
            if cur.rowcount != 1:
                return None  # 被其他领取者抢走
            row = conn.execute(
                "SELECT * FROM events WHERE seq=?", (head["seq"],)
            ).fetchone()
            return self._row_to_record(row)

    def mark_completed(
        self,
        event_id: str,
        attempts: int,
        effects: list[dict[str, Any]],
        apply_account_delta: Any | None = None,
    ) -> None:
        """同一事务：写业务副作用 + 执行副作用附加动作 + 置 COMPLETED。

        副作用 (event_id, name) 冲突时抛 sqlite3.IntegrityError，
        附加动作与 COMPLETED 都不会写入 —— 调用方据此识别“副作用其实已生效”
        的重放。apply_account_delta(account, delta) 在本事务内执行。
        """
        conn = self.conn
        with self._write_lock:
            try:
                conn.execute("BEGIN IMMEDIATE")
                for eff in effects:
                    conn.execute(
                        "INSERT INTO effects(event_id, name, detail_json) VALUES(?,?,?)",
                        (event_id, eff["name"], json.dumps(eff.get("detail", {}), sort_keys=True)),
                    )
                    if eff.get("kind") == "account.delta" and apply_account_delta is not None:
                        detail = eff.get("detail", {})
                        apply_account_delta(str(detail["account"]), float(detail["amount"]))
                cur = conn.execute(
                    "UPDATE events SET status=?, attempts=?, last_error='', "
                    "updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') "
                    "WHERE event_id=? AND status=?",
                    (EventStatus.COMPLETED.value, attempts, event_id,
                     EventStatus.IN_PROGRESS.value),
                )
                if cur.rowcount != 1:
                    raise sqlite3.IntegrityError(
                        f"event {event_id} 不在 IN_PROGRESS，提交被拒绝"
                    )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def mark_retry_wait(
        self, event_id: str, attempts: int, not_before_iso: str, error: str
    ) -> None:
        conn = self.conn
        with self._write_lock:
            cur = conn.execute(
                "UPDATE events SET status=?, attempts=?, not_before=?, last_error=?, "
                "updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') "
                "WHERE event_id=? AND status=?",
                (
                    EventStatus.RETRY_WAIT.value,
                    attempts,
                    not_before_iso,
                    error,
                    event_id,
                    EventStatus.IN_PROGRESS.value,
                ),
            )
            conn.commit()
        if cur.rowcount != 1:
            raise RuntimeError(f"event {event_id} 状态竞争，无法置 RETRY_WAIT")

    def mark_dead_letter(self, event_id: str, attempts: int, error: str) -> None:
        conn = self.conn
        with self._write_lock:
            cur = conn.execute(
                "UPDATE events SET status=?, attempts=?, last_error=?, "
                "updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') "
                "WHERE event_id=? AND status IN (?, ?)",
                (
                    EventStatus.DEAD_LETTER.value,
                    attempts,
                    error,
                    event_id,
                    EventStatus.IN_PROGRESS.value,
                    EventStatus.RETRY_WAIT.value,
                ),
            )
            conn.commit()
        if cur.rowcount != 1:
            raise RuntimeError(f"event {event_id} 状态竞争，无法置 DEAD_LETTER")

    def recover_in_progress(self) -> list[str]:
        """中断恢复：IN_PROGRESS -> RETRY_WAIT(立即可领取)，返回恢复的 event_id。

        进程崩溃时已领取的事件停留在 IN_PROGRESS；重启后统一回退到
        RETRY_WAIT 且 not_before=now，由调度器重新领取。由于上次尝试的
        事务未提交（副作用未落库），重新执行不会产生重复副作用。
        """
        conn = self.conn
        with self._write_lock:
            rows = conn.execute(
                "SELECT event_id FROM events WHERE status=?",
                (EventStatus.IN_PROGRESS.value,),
            ).fetchall()
            ids = [r["event_id"] for r in rows]
            if not ids:
                return []
            conn.execute(
                "UPDATE events SET status=?, not_before=strftime('%Y-%m-%dT%H:%M:%fZ','now'), "
                "updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') "
                "WHERE status=?",
                (EventStatus.RETRY_WAIT.value, EventStatus.IN_PROGRESS.value),
            )
            conn.commit()
            return ids

    # ---- 业务演示表（与事件状态同事务提交） --------------------------------
    def apply_account_delta(self, account: str, delta: float) -> None:
        """在当前事务内对账户余额增减（mark_completed 的事务上下文）。"""
        self.conn.execute(
            "INSERT INTO account_balances(account, balance) VALUES(?, ?) "
            "ON CONFLICT(account) DO UPDATE SET balance=balance+excluded.balance",
            (account, delta),
        )

    def account_balance(self, account: str) -> float:
        row = self.conn.execute(
            "SELECT balance FROM account_balances WHERE account=?", (account,)
        ).fetchone()
        return float(row["balance"]) if row else 0.0

    def effect_exists(self, event_id: str, name: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM effects WHERE event_id=? AND name=? LIMIT 1",
            (event_id, name),
        ).fetchone()
        return row is not None

    def count_effects(self, event_id: str | None = None, name: str | None = None) -> int:
        sql = "SELECT COUNT(*) AS c FROM effects WHERE 1=1"
        args: list[Any] = []
        if event_id is not None:
            sql += " AND event_id=?"
            args.append(event_id)
        if name is not None:
            sql += " AND name=?"
            args.append(name)
        return int(self.conn.execute(sql, args).fetchone()["c"])
