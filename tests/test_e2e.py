import asyncio

import httpx
import pytest
import respx
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.task import TaskStatus
from app.services.dispatcher import Dispatcher
from app.services.dlq import DLQService


@pytest.mark.asyncio
@respx.mock
async def test_e2e_workflow(async_client: AsyncClient, db_session: AsyncSession):
    url_ok = "https://partner.com/success"
    url_retry = "https://partner.com/retry"
    url_fail = "https://partner.com/fail"

    respx.post(url_ok).mock(return_value=httpx.Response(200, json={"status": "ok"}))
    respx.post(url_retry).mock(return_value=httpx.Response(502, text="Bad Gateway"))
    respx.post(url_fail).mock(return_value=httpx.Response(404, text="Not Found"))

    # 1. Ingest 3 tasks
    res_ok = await async_client.post(
        "/api/v1/tasks",
        json={"target_url": url_ok, "method": "POST", "body": '{"type": "ok"}'},
    )
    assert res_ok.status_code == 202
    task_ok_id = res_ok.json()["id"]

    res_retry = await async_client.post(
        "/api/v1/tasks",
        json={"target_url": url_retry, "method": "POST", "body": '{"type": "retry"}'},
    )
    assert res_retry.status_code == 202
    task_retry_id = res_retry.json()["id"]

    res_fail = await async_client.post(
        "/api/v1/tasks",
        json={"target_url": url_fail, "method": "POST", "body": '{"type": "fail"}'},
    )
    assert res_fail.status_code == 202
    task_fail_id = res_fail.json()["id"]

    # 2. Run dispatcher dispatch_batch directly
    test_dispatcher = Dispatcher()
    await test_dispatcher.start()

    try:
        count = await test_dispatcher.dispatch_batch(batch_size=10)
        assert count == 3

        # Allow coroutines to complete execution
        await asyncio.sleep(0.3)

        # 3. Verify final states
        db_session.expire_all()
        task_ok = await DLQService.get_task_by_id(db_session, task_ok_id)
        assert task_ok.status == TaskStatus.DELIVERED.value
        assert len(task_ok.logs) == 1
        assert task_ok.logs[0].response_status_code == 200

        task_retry = await DLQService.get_task_by_id(db_session, task_retry_id)
        assert task_retry.status == TaskStatus.RETRYING.value
        assert task_retry.retry_count == 1
        assert task_retry.logs[0].response_status_code == 502

        task_fail = await DLQService.get_task_by_id(db_session, task_fail_id)
        assert task_fail.status == TaskStatus.DEAD.value
        assert task_fail.logs[0].response_status_code == 404

    finally:
        await test_dispatcher.stop()


@pytest.mark.asyncio
async def test_health_check(async_client: AsyncClient):
    response = await async_client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"
