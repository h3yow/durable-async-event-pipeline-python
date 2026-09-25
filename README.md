# 本地持久化异步事件管线（Python）

在**没有任何外部消息中间件、没有外部服务或凭据**的前提下，提供一套本地可直接运行、
从头可复现的异步事件投递与处理流程，保证：

- 事件持久化入队、**至少一次投递（at-least-once）**、业务副作用**仅生效一次**；
- 失败按可解释的指数退避重试、有次数上限，不可重试/超限事件进入**死信**并留存原因；
- 处理中断或进程重启后，未完成事件恢复到**确定状态**，不丢失、不重复产生副作用；
- **同一聚合维度严格按入队顺序串行**完成，不同聚合维度之间**并发**推进；
- 全部状态流转可通过 HTTP / 日志查询观测，且保留既有 `/` 接口可用。

## 快速开始

```bash
uv sync --group dev          # 安装依赖（仅 fastapi/pytest/httpx，无外部服务）
uv run python examples/demo.py   # 端到端复现脚本
uv run pytest                   # 运行全部单测（34 个）
uv run python main.py           # 启动 HTTP 服务（默认 127.0.0.1:8000）
```

## 架构总览

```
HTTP 入队 (FastAPI)
      │  结构校验 + event_id 去重
      ▼
┌──────────────────────────────────────────────┐
│  EventStore (SQLite, WAL, 单文件持久化)        │
│   events            事件行：状态/序号/租约/原因 │
│   aggregate_seqs    每聚合单调递增 seq         │
│   effects           副作用幂等登记(pending/…)  │
└──────────────────────────────────────────────┘
      ▲ 认领/流转/恢复            │ 轮询到点事件
      │                          ▼
┌──────────────────────────────────────────────┐
│  EventWorker (asyncio)                        │
│   · 每聚合一把锁：同 key 串行                  │
│   · 信号量限并发：不同 key 并发                │
│   · 租约 + 租约超时恢复（崩溃恢复）            │
│   · 指数退避重试 / 死信                        │
└──────────────────────────────────────────────┘
```

SQLite 是 Python 标准库自带的**嵌入式**存储，数据落在本地文件，不需要任何消息中间件
或数据库进程。所有写操作在 `BEGIN IMMEDIATE` 事务中完成，配合 `synchronous=FULL` + WAL，
进程崩溃/掉电后已确认的状态不丢。

## 投递语义与“副作用仅一次”

投递层是**至少一次**：事件可能因重试、租约恢复、重复投递而被处理器多次看到。
在此之上通过**幂等登记表 `effects`** 把业务副作用收敛为**仅一次**：

1. 处理器执行前先 `begin_effect(event_id)`：
   - 首次：插入 `pending` 登记，允许执行业务；
   - 已存在 `applied`：说明副作用此前已生效（重放），**跳过业务体直接判成功**；
   - 残留 `pending`（上次在业务执行中崩溃）：清掉后重新执行业务。
2. 处理器在**外部可观测副作用真正持久生效的那一刻**调用 `await ctx.applied()`，
   登记从 `pending` 置为 `applied`（崩溃于此之后也不会再执行一次）。
3. 处理器抛错时回滚 `pending`，允许下次重试。

> 处理器约定（幂等消费者）：所有外部副作用都应在 `ctx.applied()` 之前的最近一刻完成。
> 纯计算型 handler 不调用 `applied()` 也可，管线会在其正常返回后自动登记。

### 重复与非法：彼此可区分的结论

入队返回 `IngestOutcome` 三态，HTTP 状态码也不同：

| 结论 | 含义 | HTTP |
|---|---|---|
| `accepted`  | 新事件持久化成功 | 200 |
| `duplicate` | `event_id` 已存在，识别为**重放**，拒绝再次入队（原数据不覆盖），并返回其当前状态 | 409 |
| `invalid`   | 结构非法（空 event_id/aggregate_key、payload 非对象、max_attempts 非法），**不入库** | 422 |

## 重试与死信规则

处理器用异常表达失败性质（`event_pipeline.errors`）：

- `RetryableError`：可重试失败（瞬时错误、超时、暂时不可用）。未预期的普通异常也保守按可重试处理。
- `NonRetryableError`：不可重试失败，**第 1 次即进入死信**。

可重试失败采用可解释的**指数退避**（`RetryPolicy`）：

```
第 n 次失败后等待 = min(max_delay, base_delay * backoff_factor^(n-1))  [+ 可选 jitter]
```

默认 `max_attempts=5, base_delay=0.1s, factor=2, cap=30s`；每次失败日志都会打印
`event_id`、`attempts=已尝试/上限`、退避秒数与原因。当 `attempts >= max_attempts`
仍失败，进入死信，`failure_kind=max_attempts`，`last_error` 留存最后一次原因与
“重试次数耗尽”说明。

### 失败原因分类 `FailureKind`（互斥、可查询/可区分）

| kind | 含义 |
|---|---|
| `retryable` | 一次可重试失败，事件在 `failed` 等待退避 |
| `non_retryable` | 业务显式不可重试，直接 `dead` |
| `max_attempts` | 重试次数耗尽，进入 `dead` |
| `crash` | 租约超时恢复：上次处理在租约内未完成（中断/崩溃） |
| `duplicate` | 重复入队/重放命中（结论侧） |
| `invalid` | 结构非法被拒（结论侧） |
| `none` | 无失败 |

## 状态模型

```
                 结构非法 / event_id 重复
  publish ──► (不入库或 rejected) ─────────────── 终态
     │ accepted
     ▼
  pending ──claim(租约)──► running
     ▲                       │
     │  可重试失败(退避到期)  ├──► succeeded（终态，副作用已生效）
     └───────────────────────┤
                             ├──► dead（终态：non_retryable / max_attempts，留存 last_error）
                             └── 租约超时未完成 ──► failed(crash) ──(重新认领)──► running
```

- `pending`：已持久化，等待派发；
- `running`：已被某 worker 租约认领（带 `lease_owner` 与 `lease_expires_at`）；
- `failed`：一次尝试失败，`available_at` 为退避后可再派发时间；
- `succeeded` / `dead` / `rejected`：终态。终态事件**不会阻塞**同聚合后续事件。

### 中断与进程重启恢复

worker 每轮先执行恢复扫描：凡处于 `running` 且租约已过期的事件，置回
`failed(crash)`，下一轮重新认领重试。配合 `effects` 表：

- 崩溃在副作用**生效前**：`pending` 登记被清理 → 重试，事件不丢失；
- 崩溃在副作用**生效后**：`applied` 登记已落盘 → 重放时跳过业务，副作用不重复。

恢复动作也可在进程启动时显式调用：`pipeline.recover_stuck()`。

## 并发与顺序约束

- **顺序**：存储层“每聚合只允许队首未终结事件被认领”的 SQL 约束
  （`NOT EXISTS 更小 seq 的非终态事件`）+ worker 每聚合一把 `asyncio.Lock`，
  双保险保证同一 `aggregate_key` 严格按 `seq` 串行完成。
- **并发**：不同 `aggregate_key` 的处理经过同一信号量限并发地并行推进；
  某个聚合长期失败只会让其自身后续事件等待（队首约束），**不会阻塞其他聚合**。
- worker 关停时默认 `drain`：等待所有在途事件完成，避免关停瞬间制造不确定状态。

## HTTP 接口

| 方法/路径 | 说明 |
|---|---|
| `GET  /` | 既有健康检查（保留 `{"Hello":"World"}`，附加 service/status） |
| `POST /events` | 入队；body `{event_id, aggregate_key, payload, max_attempts?}` |
| `GET  /events/{id}` | 查询单事件（含 status/attempts/failure_kind/last_error/effect_applied） |
| `GET  /events?status=dead` | 按状态过滤 |
| `GET  /aggregates/{key}/events` | 查询某聚合事件（按 seq 排序） |
| `GET  /dead-letters` | 死信列表（含失败原因与分类） |
| `GET  /stats` | 各状态计数 |
| `GET  /health` | 健康与状态计数 |

## 日志可观测性

worker 每条结论日志都带 **事件标识、尝试次数、判定依据**，例如：

```
INFO  派发 event_id=ev-flaky aggregate=ORD-2 seq=2 尝试 attempts=1/3 判定依据=该事件为聚合队首且已到点
WARN  event_id=ev-flaky 结论=等待重试 attempts=1/3 判定依据=第 1/3 次尝试失败，等待 0.100s 后重试（指数退避 base=0.1s factor=2.0 cap=1.0s）；原因：模拟瞬时失败
ERROR event_id=ev-bad   结论=死信   attempts=1/3 判定依据=不可重试错误：订单状态机不允许该迁移
WARN  event_id=e1 恢复：上次尝试 attempts=2 租约超时未完成，判定依据=running 且 lease_expires_at<=…，状态 running->failed(crash)
WARN  event_id=e1 结论=重放命中 attempts=10 判定依据=effects 已存在 applied，业务副作用不重复执行，状态 ->succeeded
```

## 验证方式（全部本地、从头复现）

```bash
uv run pytest -s
```

测试覆盖：

| 测试文件 | 覆盖场景 |
|---|---|
| `tests/test_retry_policy.py` | 指数退避序列、上限截断、重试/耗尽说明可区分 |
| `tests/test_store.py` | 去重入队、非法拒绝、序号单调、队首派发、乐观认领互斥、租约恢复、副作用仅一次登记、重开持久化 |
| `tests/test_pipeline_delivery.py` | 重复投递 vs 结构非法结论可区分；并发重复投递副作用仅一次；成功后重放不再执行；日志含事件标识/尝试次数/判定依据 |
| `tests/test_retry_deadletter.py` | 退避重试后成功、不可重试立即死信、重试耗尽死信（原因分类可区分）、未预期异常按可重试、单聚合失败不阻塞其他聚合 |
| `tests/test_ordering_concurrency.py` | 同聚合严格入队顺序串行、不同聚合并发同时在途、慢聚合不阻塞快聚合、处理中状态可查询 |
| `tests/test_recovery.py` | 崩溃于副作用前→恢复重试不丢；崩溃于副作用后→重放不重复；恢复置 crash 分类并记录原因 |
| `tests/test_api.py` | 健康检查保留；200/409/422 区分；状态/聚合/死信查询 |

也可直接观察持久化内容：

```bash
sqlite3 <db文件> "SELECT event_id,aggregate_key,seq,status,attempts,failure_kind,last_error FROM events;"
sqlite3 <db文件> "SELECT event_id,state,applied_at FROM effects;"
```

## 目录结构

```
event_pipeline/
  models.py    状态/失败分类枚举、EventRecord、RetryPolicy
  errors.py    RetryableError / NonRetryableError
  storage.py   SQLite 持久化：入队去重、序号、租约、状态流转、effects 幂等登记
  worker.py    异步调度：认领、串行/并发、幂等闸门、退避、死信、恢复
  pipeline.py  门面：组合 store + policy + worker
  handlers.py  演示业务处理器
  api.py       FastAPI 路由与生命周期
examples/demo.py  端到端复现脚本
tests/           上述全部场景测试
```
