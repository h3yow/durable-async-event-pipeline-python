# Durable Async Event Pipeline（本地可运行的持久化异步事件流水线）

在**不依赖任何外部消息中间件 / 外部服务 / 凭据**的前提下，仅用 Python 标准库
（`asyncio` + `sqlite3`）与 FastAPI 实现一套可从头复现的异步事件投递与处理流程，
提供持久化入队、至少一次投递、业务副作用幂等、退避重试、死信沉淀、
按聚合维度串行/跨维度并发，以及完整的状态可观测性。

---

## 1. 快速开始

要求 Python 3.14（由 `pyproject.toml` 约束），使用 `uv` 管理依赖。

```bash
uv sync                       # 安装依赖（fastapi / pytest / httpx）
uv run uvicorn main:app --port 8000   # 启动服务（默认 SQLite 文件 pipeline.db）
```

可用环境变量 `PIPELINE_DB` 指定存储位置（默认 `pipeline.db`，重启后状态保留）：

```bash
PIPELINE_DB=events.db uv run uvicorn main:app --port 8000
```

投递一个事件：

```bash
curl -s -X POST localhost:8000/events -H 'Content-Type: application/json' -d '{
  "event_id": "evt-001",
  "aggregate_key": "acct:alice",
  "event_type": "account.deposit",
  "payload": {"account": "alice", "amount": 100}
}'
```

运行全部测试（35 个用例）：

```bash
uv run pytest -v
```

---

## 2. 投递语义（Delivery Semantics）

* **至少一次投递（at-least-once）**：事件先持久化（`PENDING`）再被调度；
  领取时状态以条件 `UPDATE ... WHERE status IN (PENDING, RETRY_WAIT)` 原子地
  翻转为 `IN_PROGRESS`，任何失败/超时/崩溃后都会被重新投递，因此**不丢事件**。
* **业务副作用恰好生效一次（effectively exactly-once）**：处理器不直接落业务
  结果，而是以 `(event_id, effect_name)` 为幂等键向上下文**声明副作用**；流水线在
  **同一个 SQLite 事务**里完成「写副作用 + （可选）更新业务表 + 置 COMPLETED」。
  `effects(event_id, name)` 上有 `UNIQUE` 约束作为最终防线：
  * 若进程在事务提交前崩溃 → 事务回滚，副作用未落库，事件回到 `RETRY_WAIT` 重放，
    不会产生重复副作用；
  * 若同一事件被重复执行（重放/并发）→ 唯一约束冲突，第二次副作用被拒绝。
* **重复投递 vs 非法事件，结论可区分**：

  | 情形 | 判定 | HTTP | 是否持久化 |
  |------|------|------|-----------|
  | `event_id` 已存在 | `reason=DUPLICATE`（识别为重放，不覆盖原事件） | `409` | 保留原记录 |
  | 外层字段缺失/类型错误 | `reason=INVALID_ENVELOPE` | `422` | 落 `REJECTED` |
  | 已知类型 payload 字段非法 | `reason=INVALID_PAYLOAD` | `422` | 落 `REJECTED` |

  合法事件首次入队返回 `202 Accepted`。

---

## 3. 重试与死信规则（Retry & Dead-Letter）

处理器通过抛出不同异常表达失败性质（见 `app/errors.py`）：

* `RetryableError`（以及处理器超时）→ **可重试**，进入退避等待；
* `NonRetryableError`、未注册的事件类型、未预期异常 → **不可重试**，
  **当次即进入死信**，不浪费重试次数；
* 可重试失败在 `attempts == max_attempts`（默认 5）时也进入死信。

**退避策略（可解释、有上限、默认确定性可复现）**，见 `app/backoff.py`：

```
delay(attempts_done) = min(base_delay * factor ** (attempts_done - 1), max_delay)
                     = min(0.05 * 2^(n-1), 2.0)   # 秒，默认参数
```

* 指数增长并被 `max_delay` 封顶，次数上限为 `max_attempts`；
* 默认 `jitter=0`，相同输入永远得到相同延迟，测试结果可从头复现
  （可配置基于尝试次数的确定性抖动）；
* 每次流转日志都打印人类可读的退避依据，例如
  `backoff: attempt#3 failed -> wait 0.200s (base=0.05, factor=2.0, cap=2.0)`。

**死信沉淀**：进入 `DEAD_LETTER` 的事件保留在 `events` 表中不删除，
`last_error` 记录最终失败原因（含是否 `non-retryable` / `retries exhausted after n/m`），
可通过 `GET /dead-letters` 查询；修复下游后可通过**死信重新驱动**复活（见 §4.1）。

---

## 4. 状态模型（State Model）

```
                 结构非法（入队时）
  输入 ───────────────────────────────▶ REJECTED（持久化、可观测、不调度）

  合法输入 ─▶ PENDING ──claim──▶ IN_PROGRESS ──成功(副作用同事务提交)──▶ COMPLETED
                ▲                  │
                │                  ├─ 可重试 & attempts<max ─▶ RETRY_WAIT
                │                  │                            │
                └──── 到期(not_before<=now) ◀───────────────────┘
                                   │
                                   ├─ 不可重试（即时）
                                   └─ 可重试但 attempts==max ──▶ DEAD_LETTER

  崩溃/进程重启：IN_PROGRESS  ──recover() 启动扫描──▶ RETRY_WAIT(not_before=now)
```

| 状态 | 含义 |
|------|------|
| `PENDING` | 已持久化，尚未被领取 |
| `IN_PROGRESS` | 已被某 worker 领取，正在执行（崩溃时停留于此态） |
| `RETRY_WAIT` | 失败后退避等待，`not_before` 之前不可被领取 |
| `COMPLETED` | 成功且副作用已提交（终态） |
| `DEAD_LETTER` | 不可重试或超过重试上限（终态，保留原因） |
| `REJECTED` | 结构非法，入队即拒绝（终态，与 DUPLICATE 区分） |

每条事件记录持久化字段：`seq`(自增，决定同维度顺序)、`event_id`(UNIQUE)、
`aggregate_key`、`event_type`、`payload_json`、`status`、`attempts`、
`max_attempts`、`not_before`、`last_error`、时间戳。

**中断恢复**：流水线 `start()` 时执行一次 `recover()`，把所有遗留的
`IN_PROGRESS` 原子回退为 `RETRY_WAIT`（`not_before=now`），随后立即重新参与调度。
由于上次尝试的事务在崩溃时未提交，副作用不会落库，因此重放安全、不产生重复。

---

## 4.1 死信重新驱动（Dead-Letter Redrive）

**适用场景**：下游故障修复后，把积压在死信里的事件重新投入处理，而不是人工
重发或丢弃。重新驱动**沿用原事件身份与内容**（`event_id` / `payload` / `seq`
不变），仅重置 `attempts=0` 给予新的重试预算，并立即到期（`not_before=now`）。

**状态流转**：

```
DEAD_LETTER ──redrive(条件 UPDATE)──▶ RETRY_WAIT(attempts=0, not_before=now)
                                        │
        ┌───────────────────────────────┼───────────────────────────┐
        ▼                               ▼                           ▼
   处理成功                        退避重试中                    再次失败
   COMPLETED                    PENDING/IN_PROGRESS/          DEAD_LETTER
   → SUCCEEDED                  RETRY_WAIT → IN_FLIGHT        → FAILED_AGAIN
```

三种结果通过 `GET /redrives/{redrive_id}` 实时区分（由审计记录 + 事件当前
状态推导，无需额外状态机）。

**顺序与幂等保证**：

* **不插队**：事件保留原 `seq`，重新进入调度后仍按「同维度 seq 最小且到期」
  的队头规则被领取——它回到自己原来的位置，先于同维度更晚入队的事件，
  也不会越过任何本应排在它前面的事件；
* **同一 redrive_id 只生效一次**：`redrive_audit UNIQUE(redrive_id, event_id)`
  作为幂等键，重复调用（含事件被驱动后再次死亡、又用同一 id 重放的极端
  情形）一律跳过并返回 `SKIPPED_NOT_DEAD`；
* **多入口并发不重复**：状态流转是条件
  `UPDATE ... WHERE status='DEAD_LETTER'`，并发驱动下只有一个调用能把事件
  翻回 `RETRY_WAIT`，其余调用影响行数为 0 并识别为「已被并发驱动」；
* **副作用不翻倍**：复活后仍走既有的 `(event_id, effect_name)` 幂等键 +
  同事务提交机制，之前已生效的副作用不会执行第二遍。

**批量与容量限制**：单次调用受理上限为 `PipelineConfig.redrive_batch_limit`
（默认 200，`limit` 参数只能调小不能调大）；命中更多时响应 `truncated=true`，
调用方用**新的 redrive_id** 分批继续。整个驱动是单个快速 SQLite 事务，
不会长时间占用写锁，正在运行的调度与其他维度不受影响；既有对外接口
（含 `GET /`）行为不变。

**审计**：每次驱动在 `redrive_audit` 表留痕（`redrive_id`、`event_id`、维度、
类型、驱动时间），`GET /redrives/{redrive_id}` 查单次报告、
`GET /redrives` 查全局汇总（成功/重试中/再次失败计数）。

**接口**：

| 方法 & 路径 | 说明 |
|-------------|------|
| `POST /dead-letters/redrive` | 按 `aggregate_key` / `event_type` / `event_ids` 选择死信重新驱动；`redrive_id` 为幂等键（缺省自动生成），`limit` 控制批量；`202` 返回逐条受理结果 |
| `GET /redrives/{redrive_id}` | 单次驱动报告：总数 + SUCCEEDED / IN_FLIGHT / FAILED_AGAIN 分类与逐条明细 |
| `GET /redrives` | 全部驱动活动的汇总计数 |

本地验证示例：

```bash
# 1) 制造一条死信（service.always_bad 不可重试，当次即死信）
curl -s -X POST localhost:8000/events -H 'Content-Type: application/json' -d '{
  "event_id": "evt-bad", "aggregate_key": "acct:alice",
  "event_type": "service.always_bad", "payload": {}}'
curl -s localhost:8000/dead-letters

# 2) 重新驱动（自带幂等键，可安全重放）
curl -s -X POST localhost:8000/dead-letters/redrive \
  -H 'Content-Type: application/json' \
  -d '{"redrive_id": "ops-2026-10-05", "event_ids": ["evt-bad"]}'

# 3) 查询本次驱动结果与全局汇总
curl -s localhost:8000/redrives/ops-2026-10-05
curl -s localhost:8000/redrives
```

---

## 5. 并发与顺序约束（Ordering & Concurrency）

* **同一 `aggregate_key` 严格串行**：每个聚合维度至多一个 worker 协程；worker
  每次只领取该维度「`seq` 最小且到期」的事件。即使队头事件正在退避，后续事件
  也不能越过它（队头阻塞，head-of-line ordering），从而保证**按入队顺序完成**。
* **不同 `aggregate_key` 允许并发**：dispatcher 周期性扫描到期维度并为每个维度
  启动独立 worker，彼此不共享锁；一个维度长时间失败/退避**不会阻塞**其他维度。
* 状态流转全部落 SQLite，任意时刻都能通过接口查询，**不破坏既有对外接口**
  （原始 `GET /` 仍返回 `{"Hello":"World"}`）。

---

## 6. HTTP 接口

| 方法 & 路径 | 说明 |
|-------------|------|
| `GET /` | 既有接口，保持不变：`{"Hello":"World"}` |
| `POST /events` | 入队；`202` 接受 / `409 DUPLICATE` / `422 INVALID_*` |
| `GET /events/{event_id}` | 查询单事件完整状态（含 attempts、not_before、last_error） |
| `GET /aggregates/{key}/events` | 按维度列出事件（seq 排序，观察顺序约束） |
| `GET /dead-letters?limit=` | 列出死信及失败原因 |
| `POST /dead-letters/redrive` | 死信重新驱动（幂等键 `redrive_id`，批量上限保护） |
| `GET /redrives/{redrive_id}` | 单次驱动报告（成功/重试中/再次失败可区分） |
| `GET /redrives` | 全部驱动活动汇总 |
| `GET /health` | 存活状态与 dispatcher 是否运行 |

---

## 7. 代码结构

```
main.py                  入口：lifespan 中构造 store/pipeline 并启动恢复+调度
app/models.py            状态枚举、拒绝原因、判定依据、事件/视图/上下文类型
app/errors.py            RetryableError / NonRetryableError
app/validation.py        信封(INVALID_ENVELOPE) 与 payload(INVALID_PAYLOAD) 校验
app/backoff.py           确定性指数退避（公式、上限、可解释描述）
app/store.py             SQLite 持久层：条件 UPDATE、UNIQUE、同事务副作用、恢复、
                         重新驱动（条件流转 + redrive_audit 审计）
app/handlers.py          处理器注册表 + 演示处理器（存款/时好时坏/永久失败）
app/pipeline.py          编排：入队、dispatcher、按维度 worker、失败路由、崩溃恢复、
                         死信重新驱动（批量上限、结果报告、汇总）
app/api.py               FastAPI 路由
tests/                   35 个单测，覆盖下列全部场景
```

演示处理器（`event_type` → 行为）：
`account.deposit`（幂等加款）、`service.flaky`（前 N 次可重试失败后成功）、
`service.always_bad`（始终不可重试）。

---

## 8. 验证方式（测试覆盖）

`uv run pytest -v`，共 35 个用例，分七组：

1. **`test_enqueue_validation.py`**：重复投递识别为 `DUPLICATE`、信封/payload
   非法识别为 `INVALID_*`，两类结论可区分；重复提交不覆盖原记录。
2. **`test_idempotency.py`**：成功副作用恰好一次；完成后重复投递不加第二次款；
   8 线程并发重复入队只有 1 个被接受；6 线程并发领取只有 1 个成功；
   绕过上层时数据库 `UNIQUE` 仍阻止副作用落第二次。
3. **`test_retry_deadletter.py`**：失败 2 次后成功（attempts=3、副作用 1 次）；
   超过上限进入死信且原因含 `retries exhausted`；不可重试当次进死信（attempts=1）；
   未注册类型即死信；退避序列指数增长且被封顶、描述可读。
4. **`test_ordering_concurrency.py`**：同维度 5 个事件严格按入队顺序完成；
   3 个维度并发耗时接近单事件而非 3 倍；某维度队头失败退避期间不阻塞其他维度，
   恢复后该维度仍按序。
5. **`test_recovery.py`**：副作用登记后、提交前崩溃 → 事件停留 `IN_PROGRESS`、
   无副作用；重启 `recover()` 后完成且**只加一次款、只有一条副作用**；
   启动时批量恢复全部遗留 `IN_PROGRESS`；多事件混合崩溃后无丢失无重复。
6. **`test_api.py`**：`GET /` 不变；健康检查；202/409/422 状态码；
   状态迁移可观测；维度列表与死信查询；重新驱动接口端到端
   （驱动 → 幂等重放 → 报告分类 → 汇总 → 404/422）。
7. **`test_redrive.py`**：修复下游后死信复活并跑完（副作用恰好一次、
   报告归类 SUCCEEDED）；同一 `redrive_id` 重复发起（含驱动后再次死亡的
   极端重放）只生效一次；多线程并发驱动同一批死信每条只被驱动一次；
   复活事件回到原 `seq` 位置、同维度完成顺序不乱；账户加款副作用不翻倍；
   批量上限截断（`truncated`）与审计/汇总可查、大批量驱动期间其他维度
   照常推进；三种结果（SUCCEEDED / IN_FLIGHT / FAILED_AGAIN）可区分；
   按类型 / 显式 event_ids 选择，不存在与不在死信的条目可区分。

**单测日志**：`pipeline` logger 对每次状态流转打印
**事件标识 `event=...`、尝试次数 `attempt=N`、判定依据 `decision=...`**
（`OK / RETRYABLE / NON_RETRYABLE / DEAD_LETTER / REPLAY / SIMULATED_CRASH`，
重新驱动另有 `redrive=<id> event=... decision=REDRIVEN / SKIPPED_*`），
失败路径额外打印退避说明与 `last_error`。测试通过 `caplog` 捕获并断言这些内容。

所有结论均使用本地 SQLite 与进程内 `asyncio`，无需任何外部依赖，可从头复现。
