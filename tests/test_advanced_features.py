import asyncio
from datetime import timedelta

import httpx
import pytest
import respx
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.schema import TaskCreateRequest
from app.models.task import TaskStatus, utc_now
from app.services.dispatcher import Dispatcher
from app.services.dlq import DLQService
from app.services.ingestion import IngestionService


@pytest.mark.asyncio
async def test_orphan_task_recovery(db_session: AsyncSession):
    # 1. Create a task and artificially place it in PROCESSING state 10 minutes ago
    req = TaskCreateRequest(
        target_url="https://external.service.com/webhook",
        method="POST",
        body='{"event": "orphan_test"}',
        max_retries=3,
    )
    task, _ = await IngestionService.create_task(db_session, req)
    task.status = TaskStatus.PROCESSING.value
    task.updated_at = utc_now() - timedelta(seconds=600)  # 10 minutes ago
    await db_session.commit()

    # 2. Run orphan sweeper with 300s (5 min) threshold
    task_id = task.id
    test_dispatcher = Dispatcher()
    recovered_count = await test_dispatcher.recover_orphaned_tasks(timeout_seconds=300.0)
    assert recovered_count == 1

    # 3. Verify task is reverted to RETRYING and has audit log
    db_session.expire_all()
    recovered_task = await DLQService.get_task_by_id(db_session, task_id)
    assert recovered_task is not None
    assert recovered_task.status == TaskStatus.RETRYING.value
    assert "[Orphan Recovered]" in recovered_task.last_error
    assert len(recovered_task.logs) == 1
    assert "Orphan sweeper" in recovered_task.logs[0].error_message


@pytest.mark.asyncio
@respx.mock
async def test_concurrency_semaphore_throttling(db_session: AsyncSession):
    url = "https://concurrent.service.com/webhook"

    current_concurrency = 0
    max_observed_concurrency = 0
    lock = asyncio.Lock()

    async def delayed_response(request):
        nonlocal current_concurrency, max_observed_concurrency
        async with lock:
            current_concurrency += 1
            max_observed_concurrency = max(max_observed_concurrency, current_concurrency)

        await asyncio.sleep(0.08)

        async with lock:
            current_concurrency -= 1

        return httpx.Response(200, json={"status": "ok"})

    respx.post(url).mock(side_effect=delayed_response)

    # Ingest 8 tasks
    for i in range(8):
        req = TaskCreateRequest(
            target_url=url,
            method="POST",
            body=f'{{"item": {i}}}',
        )
        await IngestionService.create_task(db_session, req)

    # Configure dispatcher with concurrency limit of 3
    original_limit = settings.MAX_CONCURRENT_DELIVERIES
    settings.MAX_CONCURRENT_DELIVERIES = 3

    test_dispatcher = Dispatcher()
    test_dispatcher._semaphore = asyncio.Semaphore(3)
    await test_dispatcher.start()

    try:
        dispatched = await test_dispatcher.dispatch_batch(batch_size=8)
        assert dispatched == 8

        # Wait for all tasks to complete
        await asyncio.sleep(0.4)

        # Assert concurrency never exceeded 3
        assert max_observed_concurrency <= 3
        assert max_observed_concurrency > 0
    finally:
        await test_dispatcher.stop()
        settings.MAX_CONCURRENT_DELIVERIES = original_limit


@pytest.mark.asyncio
@respx.mock
async def test_graceful_shutdown_draining(db_session: AsyncSession):
    url = "https://slow-drain.service.com/webhook"

    async def slow_response(request):
        await asyncio.sleep(0.1)
        return httpx.Response(200, json={"status": "drained"})

    respx.post(url).mock(side_effect=slow_response)

    req = TaskCreateRequest(
        target_url=url,
        method="POST",
        body='{"event": "drain"}',
    )
    task, _ = await IngestionService.create_task(db_session, req)

    task_id = task.id
    test_dispatcher = Dispatcher()
    await test_dispatcher.start()

    # Dispatch batch
    await test_dispatcher.dispatch_batch(batch_size=1)
    assert len(test_dispatcher._active_tasks) == 1

    # Stop dispatcher - should gracefully drain in-flight task
    await test_dispatcher.stop()
    assert len(test_dispatcher._active_tasks) == 0

    # Verify task successfully delivered
    db_session.expire_all()
    drained_task = await DLQService.get_task_by_id(db_session, task_id)
    assert drained_task.status == TaskStatus.DELIVERED.value
