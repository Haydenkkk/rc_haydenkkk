# AI 使用说明 (AI Collaboration & Engineering Decision Record)

本说明记录了在本项目（API 通知系统设计与实现）开发全周期中，人机协作的过程、对 AI 产出内容的判断与筛选，以及自主做出的关键工程决策与 Review 迭代优化。

---

## 一、 AI 在哪些关键地方提供了帮助

在项目的分析、设计与编码过程中，AI 作为高效的“辅助副驾驶 (Copilot)”，主要在以下环节提供了高质量支持：

1. **需求解构与协议标准梳理**：
   - 辅助梳理分布式 Webhook / Outbound Notification 的典型场景，对比了业界主流 Webhook 提供商（如 Stripe, GitHub, Twilio）的标准投递与重试语义。
   - 辅助设计标准化的 API 契约与 Pydantic 请求/响应模型。

2. **样板代码与基础框架快速搭建**：
   - 快速生成了基于 FastAPI + SQLAlchemy 异步 ORM 的数据模型、路由层与配置解析样板代码。
   - 自动生成符合 PEP 8 规范的类型注解（Type Hints）与 Docstrings。

3. **全面的测试用例矩阵与 Mock 生成**：
   - 辅助构建了覆盖率极高的 `pytest` 测试套件，利用 `respx` 拦截并模拟了各种网络异常场景（200 成功、503 临时故障、401 权限失效、网络超时、幂等防重等）。
   - 编写了独立的 `mock_server.py` 与自动化验证演示脚本 `demo_verify.py`。

---

## 二、 AI 给出过哪些我们“明确未采纳”的建议及原因

在方案设计阶段，AI 提出过若干看似“功能齐全”、“高大上”的建议，但结合本项目当前的阶段定位（MVP）、系统边界与复杂度管理原则，我们进行了**主动识别并予以否决**：

### 1. 否决建议一：建议在第一版直接引入 Redis + Celery / RabbitMQ / Kafka
- **AI 的原始建议**：建议使用 Redis 作为 Broker 并通过 Celery 进行分布式任务调度，或者使用 Kafka 做消息持久化。
- **未采纳原因（工程判断）**：
  - **违背 MVP 与自包含原则**：作业要求“最小可行实现（MVP）”，关注工程判断而非过度堆砌。引入外部 Redis/RabbitMQ 会强制依赖外部守护进程，增加部署门槛，且使单机单元测试（pytest）变得脆弱。
  - **数据持久化可靠性风险**：Redis 默认偏向内存缓存，AOF/RDB 在断电或未及时刷盘时存在潜在丢单风险；而关系型数据库的 WAL 模式（Write-Ahead Logging）具备严格的 ACID 事务特性，写入即安全。
  - **决策结果**：采用 **FastAPI + SQLite (WAL 模式) + 原生 asyncio 协程池调度**。系统架构高度内聚，零外部依赖，同时基于 SQLAlchemy 异步封装，未来平滑切换到 PostgreSQL 无缝衔接。

### 2. 否决建议二：建议引入分布式事务 (2PC / Saga) 确保外部调用一致性
- **AI 的原始建议**：建议设计两阶段提交或 Saga 工作流，以确保上游业务和外部第三方状态绝对一致。
- **未采纳原因（工程判断）**：
  - **严重脱离业务实际**：外部系统由第三方供应商维护，通常仅提供简单的 HTTP Webhook 接口，根本不可能支持两阶段提交等协议。
  - **职责越界**：通知系统本质是**异步解耦与最终一致性传输通道**，不应侵入业务事务。
  - **决策结果**：明确采用标准的 **At-least-once（至少一次）投递语义**，结合幂等键与死信池解决可靠性问题。

### 3. 否决建议三：建议系统内置动态 JSON 解析引擎与多步骤工作流编排 (Workflow DAG)
- **AI 的原始建议**：建议在通知系统内部加入数据转换引擎（如 JSONPath / Liquid 模板），支持将上游数据自动转换并编排成不同第三方的格式。
- **未采纳原因（工程判断）**：
  - **模糊系统边界，导致职责蔓延**：根据“单一职责原则 (SRP)”，Payload 的组装与业务转换属于上游业务域逻辑；通知系统的职责应聚焦于“通用、透明、高可靠的 HTTP 投递管道”。过早引入数据编排会使系统与各业务高度耦合，丧失通用性。
  - **决策结果**：坚持透明传输（Transparent Payload Delivery），上游提交什么，系统就可靠地投递什么。

---

## 三、 哪些关键决策是我们自己做出的，以及背后的原因

以下核心工程决策由开发者主导并实施，体现了对真实网络环境、故障恢复与系统边界的考量：

### 1. 严格区分 4xx 客户端错误与 5xx 服务端错误（差异化重试决策）
- **决策内容**：
  - 遇到 `500/502/503/504`、`429 (Rate Limit)` 以及网络超时（Timeout/ConnectError）时，判定为**可恢复异常**，执行指数退避重试。
  - 遇到 `400 (Bad Request)`、`401 (Unauthorized)`、`403 (Forbidden)`、`404 (Not Found)` 时，判定为**不可恢复的客户端配置/业务错误**，直接标记为 `DEAD`（进入死信池），不执行无意义的重试。
- **原因**：如果第三方由于 Token 过期或 URL 错误返回 401/404，持续重试只会白白浪费 CPU 和带宽资源，甚至被对方安全防火墙拉黑。及时归入死信池并保留错误上下文，便于运维排错。

### 2. 引入 Full Jitter（全抖动算法）消除重试雪崩
- **决策内容**：重试间隔不使用确定性的 $2^n$，而是使用 $\text{WaitTime} = \text{random.uniform}(0.1, \min(\text{MaxBackoff}, \text{Base} \times 2^n))$。
- **原因**：在真实网络中，若外部服务发生短时间宕机，大量并发请求会在相同时间点失败。若采用固定退避，所有失败请求将在下一个相同的时间点同时发起重试，造成巨大的“重试风暴 (Thundering Herd)”，直接击垮正在重启恢复中的外部服务。引入随机 Jitter 能将重试流量平滑打散。

### 3. 采用 Atomic Claiming（原子状态抢占）避免并发重复投递
- **决策内容**：调度器在拉取任务时，采用带有条件检查的原子更新语句：
  ```sql
  UPDATE tasks 
  SET status = 'PROCESSING', updated_at = :now 
  WHERE id = :task_id AND status IN ('PENDING', 'RETRYING')
  ```
  只有 `rowcount > 0` 的 Worker 才能拿到该任务的投递权。
- **原因**：即便在单进程内开启多并发协程或未来扩展到多 Worker 实例时，该机制也能从数据库行级锁层面杜绝同一任务被多个 Worker 同时执行导致的重复投递。

### 4. 完整的死信机制 (DLQ) 与“一键重放”闭环
- **决策内容**：不仅记录失败，更提供了 `GET /api/v1/tasks?status=DEAD` 与 `POST /api/v1/tasks/{task_id}/retry` 接口，并在重放时自动在审计日志中记录 `Manual replay triggered by operator`。
- **原因**：通知系统不仅需要处理常态化投递，更要具备极端故障发生后（如第三方大面积宕机 2 小时后恢复）的人工介入与批量兜底补偿能力，形成完整的工程闭环。

---

## 四、 Code Review 审查意见辨析与二次演进

在完成初代版本后，针对团队/审查报告提出的改进点，我们进行了二次技术研判与优化落地：

1. **采纳「状态孤儿自愈 (Orphan Sweeper)」建议**：
   - **分析**：当 Worker 进程发生 OOM 崩溃或容器重启时，已抢占的 `PROCESSING` 任务可能永远停滞。
   - **落地**：在 `Dispatcher` 中实现 `recover_orphaned_tasks()`，在服务启动时和周期性循环中自动扫描超时停留在 `PROCESSING` 的任务，安全回滚至 `RETRYING`，彻底杜绝丢单。

2. **采纳「原子批量抢占 (`UPDATE ... RETURNING`)」建议**：
   - **分析**：旧实现采用先查出 ID 再在 Python 循环中执行 N 次单独 UPDATE，存在 N+1 数据库开销。
   - **落地**：利用 SQL 原生 `UPDATE ... WHERE id IN (...) RETURNING id`，单次 SQL 调用原子完成批量锁定与结果获取。

3. **采纳「优雅关机 (Graceful Shutdown & Draining)」建议**：
   - **分析**：旧实现在 `stop()` 时仅取消主轮询循环，在途派发的 HTTP 协程可能被立即强杀。
   - **落地**：维护 `_active_tasks` 任务集合，在关机时使用 `asyncio.wait(..., timeout=GRACEFUL_SHUTDOWN_TIMEOUT)` 等待在途网络请求排空。

4. **采纳「现代 Python 语法规范与类型系统 (Ruff 0 警告)」建议**：
   - **落地**：升级至 Python 3.10+ 原生类型语法 (`T | None`, `tuple`, `dict`)，FastAPI 依赖注入全部重构为 `typing.Annotated` 推荐模式，并通过 Ruff 检查达成 **0 Warnings / 0 Errors**。
