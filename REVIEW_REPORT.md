# 代码审查及修改建议报告 (Code Review Report)

基于对当前 Webhook / API 通知交付引擎 MVP 项目的深度审查，本报告涵盖了关于代码健壮性、设计缺陷、语法规范及测试覆盖率的详细评估和改进建议。

## 1. 消除潜在的 "State Orphan" (状态孤儿) 风险

**当前现象**:
在 `app/services/dispatcher.py` 的 `dispatch_batch` 方法中，认领任务时会将其状态更新为 `PROCESSING` 并异步执行。如果此时 FastAPI 进程因 OOM、崩溃或容器被强杀而异常退出，这些任务将永远停留在 `PROCESSING` 状态。由于调度器只轮询 `PENDING` 和 `RETRYING`，这些任务将被永久遗漏（成为孤儿任务）。

**修改建议 (引入 Sweeper 机制)**:
*   在 `Dispatcher` 的 `_worker_loop` 中，添加定时（如每隔几分钟）或在启动时执行的孤儿任务恢复逻辑 (`recover_orphaned_tasks`)。
*   扫描数据库中处于 `PROCESSING` 状态，并且 `updated_at` 距今已超过某个安全阈值（如大于 HTTP 最大超时时间配置，如 5 分钟）的任务，将其安全地回滚至 `RETRYING` 状态。
*   此举能真正保证极端情况下的“最终至少一次（At-least-once）”语义。

## 2. 解决严重的 N+1 数据库并发与性能问题 (设计优化)

**当前现象**:
在 `dispatch_batch` 认领任务时，目前的代码实现为：
```python
candidate_ids = list(result.scalars().all())
# ...
for tid in candidate_ids:
    claim_stmt = update(Task).where(...).values(status="PROCESSING")
    await db.execute(claim_stmt)
```
如果批处理获取了 100 条任务，这里将会执行 1 次 SELECT 和 100 次独立的 UPDATE 操作，导致严重的 N+1 问题。并且，如果后续迁移到 PostgreSQL 多节点环境，这种分离的读取和循环单条更新会导致竞态条件（Race Condition）。

**修改建议 (原子批量抢占)**:
*   利用 SQL 原生的原子批量更新。使用一次性批量 `UPDATE`，结合 SQLAlchemy 的 `returning()` 特性，一步到位地更新并获取真正抢占成功的 ID。
```python
claim_stmt = (
    update(Task)
    .where(
        Task.id.in_(candidate_ids),
        Task.status.in_([TaskStatus.PENDING.value, TaskStatus.RETRYING.value])
    )
    .values(status=TaskStatus.PROCESSING.value, updated_at=utc_now())
    .returning(Task.id)
)
result = await db.execute(claim_stmt)
claimed_ids = list(result.scalars().all())
```

## 3. Graceful Shutdown (优雅关机) 机制不完善

**当前现象**:
在 `app/services/dispatcher.py` 的 `stop(self)` 逻辑中，目前仅调用了 `self._task.cancel()`，这只中断了主轮询循环。如果有多个 HTTP 请求正在被 `asyncio.create_task` 派发执行，它们并没有被安全地等待（await）或被赋予处理中断的机会，会导致请求强行被截断。

**修改建议**:
*   在 `Dispatcher` 类中引入任务追踪机制（例如 `self._active_deliveries = set()`）。
*   在触发单条任务时将其加入 set，在任务完成后 `discard`。
*   在 `stop(self)` 逻辑中使用 `asyncio.gather(*self._active_deliveries, return_exceptions=True)`，配合一个超时阈值，确保在服务关闭前等待正在飞行的投递任务安全结束。

## 4. 消除 Ruff (Linting & 语法) 警告

代码虽然在逻辑层面安全运行，但通过 `uv run ruff check .` 扫描发现了 88 条规范与风格相关的警告。为了符合现代 Python (>=3.10) 的最佳实践，需要进行以下清理：

*   **Type Hints 更新**: 移除冗余的 `Optional[T]` 和 `Tuple[A, B]`，全面采用 Python 3.10+ 标准的 `T | None` 及内建小写 `tuple[A, B]`。
*   **修复 FastAPI `Depends` 默认参数 (B008)**: 不在函数参数默认值中直接调用 `Depends`，改用业界更推荐的 `typing.Annotated` 方式，例如：`db: Annotated[AsyncSession, Depends(get_db_session)]`。
*   **异常捕获优化 (G201 & BLE001)**: 避免宽泛的 `except Exception as ex:` 并强行拼接 `str(ex)`，应当使用 `logger.exception("...")` 以在日志中保留完整的 Traceback。
*   **移除死代码**: 清理未使用的库（如 `demo_verify.py` 中的 `subprocess` 和 `sys`）以及无效的空转 F-String。

## 5. 补充测试用例边界 (测试盲区)

在重构后，需要在现有的测试用例基础上增加对系统级边缘场景的覆盖：
1.  **Orphan Recovery (死任务清理测试)**: 模拟创建一条已经处于 `PROCESSING` 状态，并且 `updated_at` 被篡改为很久之前的任务，验证系统能够自动扫描并将其恢复为 `RETRYING` 状态。
2.  **Concurrency Semaphore (并发流控测试)**: 创建超过限制上限的任务（例如配置 `MAX_CONCURRENT_DELIVERIES=5`，但触发 20 个任务），验证通过 `asyncio.Semaphore` 是否成功阻塞并限制了同一时刻在途的外部并发 HTTP 请求。
