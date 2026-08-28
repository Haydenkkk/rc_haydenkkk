# API 通知系统 (Outbound Webhook Delivery System) 详细设计与实施计划

## 1. 背景与目标
在企业级分布式架构中，内部业务系统（如订单、支付、用户中心）在触发关键事件时，需要调用外部供应商的 HTTP(S) API（如广告回传、CRM 更新、第三方库存同步）。

由于外部供应商 API 存在：
1. 网络不可靠（偶发抖动、高延迟、超时、短暂宕机）；
2. 接口协议异构（不同地址、Header、认证方式、Body 格式）；
3. 响应 SLA 差异大，极易阻塞主业务流。

本项目旨在设计并实现一个高可靠、异步解耦的**内部 API 通知投递服务 (MVP)**，为上游业务系统提供吞吐隔离与可靠性托底，确保通知请求在各种异常情况下最终可靠送达。

---

## 2. 系统总体架构与数据流

```
   [ 上游业务系统 ] (Order/Pay/User)
         |
         | 1. POST /api/v1/tasks (带目标 URL, Header, Body, idempotency_key)
         v
+-----------------------------------------------------------------------------------+
|  Ingestion API Layer (FastAPI)                                                    |
|  - 参数校验 (Pydantic)                                                            |
|  - 幂等检查 (idempotency_key 查重)                                                |
|  - 任务落盘持久化                                                                 |
|  - 立即返回 202 Accepted { "task_id": "...", "status": "PENDING" }                |
+-----------------------------------------------------------------------------------+
         |
         | 2. 写入 DB (SQLite WAL 模式，事务保证落盘)
         v
+-----------------------------------------------------------------------------------+
|  Durable Storage (SQLite / aiosqlite + SQLAlchemy)                                |
|  - 状态: PENDING -> PROCESSING -> DELIVERED / RETRYING -> DEAD (DLQ)              |
|  - 包含: 重试次数、下一次执行时间 next_retry_at、投递日志历史                     |
+-----------------------------------------------------------------------------------+
         |
         | 3. 定时/事件驱动拉取到期待处理任务 (Atomic Claim)
         v
+-----------------------------------------------------------------------------------+
|  Dispatcher & Delivery Engine (Async Worker / httpx)                             |
|  - 并发限流控制 (asyncio.Semaphore)                                               |
|  - 严格超时控制 (Connect: 3s, Read: 10s)                                          |
|  - 状态码决策引擎:                                                                |
|      * 2xx -> 标记 DELIVERED (记录耗时/响应)                                      |
|      * 4xx (非429) -> 客户端错误，不可重试，直接 DEAD                             |
|      * 5xx / 429 / 网络超时 -> 可重试，计算 Exponential Backoff + Jitter          |
|      * 超过最大重试次数 -> 归档至 DEAD (死信)                                     |
+-----------------------------------------------------------------------------------+
         |
         | 4. HTTP POST 实际投递
         v
   [ 外部供应商 API ] (第三方广告 / CRM / 库存系统)
```

---

## 3. 系统边界界定 (System Boundaries)

### 3.1 本系统解决的核心问题 (In-Scope)
1. **异步解耦与快速 ACK**：业务系统提交请求后毫秒级返回，不被外部系统的网络延迟阻塞。
2. **持久化与防丢保障**：请求立即落盘（SQLite WAL 模式），服务重启或宕机任务不丢失。
3. **幂等接收**：通过 `idempotency_key` 避免上游因网络抖动重复提交任务。
4. **智能退避重试**：支持基于指数退避 (Exponential Backoff) 与全抖动 (Full Jitter) 的重试算法，避免重试风暴。
5. **故障隔离与死信管理 (DLQ)**：对超过最大重试次数或 4xx 错误的任务进行归档，提供死信查询与手动重放 API。
6. **全链路投递审计**：详细记录每一次投递尝试的时间、状态码、耗时与错误信息。

### 3.2 明确不解决的问题及原因 (Out-of-Scope)
1. **不保证外部系统的接收幂等性**：
   - *原因*：本系统采用 At-least-once（至少一次）投递语义。在网络超时等极端情况下，外部可能已接收但响应丢失，重试可能造成重复投递。外部系统必须依赖业务 Payload 中的唯一业务 ID 自行实现幂等。
2. **不解析外部系统的业务响应内容**：
   - *原因*：只要 HTTP 状态码为 2xx 即视为协议层投递成功。系统作为通用基础设施，不与具体供应商的 JSON Schema 产生业务耦合。
3. **不做复杂的异构数据格式转换与流程编排**：
   - *原因*：遵循单一职责原则，Payload 的拼装由上游业务系统完成。

---

## 4. 核心数据模型与状态机

### 4.1 任务状态流转
- `PENDING`: 刚创建，等待被调度器分发。
- `PROCESSING`: 正在投递执行中（已被 Worker 抢占锁定）。
- `DELIVERED`: 外部返回 2xx，投递成功（终态）。
- `RETRYING`: 投递遇到可恢复异常（5xx/429/超时），等待退避时间到达后重试。
- `DEAD`: 超过最大重试次数或遇到不可恢复错误（4xx 客户端错误），进入死信池（终态，可人工触发重放）。

### 4.2 数据库表设计
1. **`tasks` 表**
   - `id`: `String(36)` - UUID
   - `idempotency_key`: `String(128)` - 业务幂等键 (唯一索引)
   - `target_url`: `String(1024)` - 目标 URL
   - `method`: `String(10)` - HTTP 方法 (默认 POST)
   - `headers`: `JSON / Text` - 自定义 HTTP Header
   - `body`: `Text` - HTTP 请求载荷
   - `status`: `String(20)` - 任务状态
   - `retry_count`: `Integer` - 已重试次数 (初始 0)
   - `max_retries`: `Integer` - 最大重试次数 (默认 5)
   - `next_retry_at`: `DateTime` - 下次重试时间 (索引字段)
   - `last_error`: `Text` - 最近一次错误详情
   - `created_at`, `updated_at`: `DateTime`

2. **`delivery_logs` 表**
   - `id`: `Integer` - 自增主键
   - `task_id`: `String(36)` - 关联任务 ID
   - `attempt_number`: `Integer` - 第几次尝试
   - `response_status_code`: `Integer` - HTTP 响应码
   - `response_body_snippet`: `Text` - 响应摘要 (排错辅助)
   - `duration_ms`: `Integer` - 请求耗时
   - `error_message`: `Text` - 异常详情
   - `created_at`: `DateTime`

---

## 5. 核心算法与机制设计

### 5.1 指数退避与全抖动算法 (Exponential Backoff with Full Jitter)
$$Interval = \min(MaxInterval, Base \times 2^{retry\_count})$$
$$WaitTime = Uniform(0, Interval)$$
- 参数配置：$Base = 1s$, $MaxInterval = 60s$, $MaxRetries = 5$。
- 目的：打散重试流量，消除重试风暴（Thundering Herd）。

### 5.2 状态码决策矩阵
| 响应 / 异常 | 分类 | 系统行为 |
| :--- | :--- | :--- |
| `200 ~ 299` | 成功 | 标记 `DELIVERED`，结束生命周期 |
| `400, 401, 403, 404, 422` | 客户端配置/参数错误 | 标记 `DEAD`（不可重试，避免浪费资源） |
| `429` | 频率限制 | 标记 `RETRYING`，计算退避时间 |
| `500, 502, 503, 504` | 外部服务端暂时错误 | 标记 `RETRYING`，计算退避时间 |
| `ConnectTimeout / ReadTimeout / NetworkError` | 网络异常 | 标记 `RETRYING`，计算退避时间 |
| `retry_count >= max_retries` | 重试耗尽 | 标记 `DEAD`（进入死信池） |

---

## 6. API 接口规范

1. `POST /api/v1/tasks` - 创建通知投递任务
   - 请求参数：`target_url`, `method`, `headers`, `body`, `idempotency_key`, `max_retries`
   - 返回：`202 Accepted` + Task 详情
2. `GET /api/v1/tasks/{task_id}` - 查询任务详情与投递审计流水
3. `POST /api/v1/tasks/{task_id}/retry` - 手动重放死信任务
4. `GET /api/v1/tasks` - 分页查询任务列表 (可按状态筛选)
5. `GET /health` - 探针与服务健康状态

---

## 7. 文档交付要求
1. `README.md`：涵盖问题理解、架构图、关键工程决策、系统边界、可靠性策略、中间件替代分析及演进路径。
2. `AI_USAGE.md`：详细阐述 AI 的贡献点、未采纳的过度设计、以及自主做出的核心工程决策。
