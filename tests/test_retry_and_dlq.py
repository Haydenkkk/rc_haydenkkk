import httpx
import pytest
import respx
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.schema import TaskCreateRequest
from app.models.task import TaskStatus
from app.services.delivery import DeliveryEngine
from app.services.dlq import DLQService
from app.services.ingestion import IngestionService


@pytest.mark.asyncio
@respx.mock
async def test_exhaust_retries_into_dlq(db_session: AsyncSession):
    url = "https://flaky.provider.com/hook"
    respx.post(url).mock(return_value=httpx.Response(500, text="Internal Server Error"))

    req = TaskCreateRequest(
        target_url=url,
        method="POST",
        body='{"event": "retry_test"}',
        max_retries=2,
    )
    task, _ = await IngestionService.create_task(db_session, req)

    # Attempt 1: failure -> RETRYING (retry_count=1)
    task = await DeliveryEngine.deliver_task(db_session, task.id)
    assert task.status == TaskStatus.RETRYING.value
    assert task.retry_count == 1

    # Attempt 2: failure -> DEAD (retry_count=2 >= max_retries=2)
    task = await DeliveryEngine.deliver_task(db_session, task.id)
    assert task.status == TaskStatus.DEAD.value
    assert task.retry_count == 2
    assert "Exceeded max retries (2)" in task.last_error
    assert len(task.logs) == 2


@pytest.mark.asyncio
@respx.mock
async def test_dlq_replay_endpoint(async_client: AsyncClient, db_session: AsyncSession):
    url = "https://recover.provider.com/hook"
    respx.post(url).mock(return_value=httpx.Response(500, text="Internal Server Error"))

    # 1. Create a task with max_retries=1
    req = TaskCreateRequest(
        target_url=url,
        method="POST",
        body='{"event": "dlq_replay"}',
        max_retries=1,
    )
    task, _ = await IngestionService.create_task(db_session, req)

    # Trigger delivery to fail and go to DEAD
    task = await DeliveryEngine.deliver_task(db_session, task.id)
    assert task.status == TaskStatus.DEAD.value

    # 2. Query task detail API
    detail_res = await async_client.get(f"/api/v1/tasks/{task.id}")
    assert detail_res.status_code == 200
    detail_data = detail_res.json()
    assert detail_data["status"] == "DEAD"
    assert len(detail_data["logs"]) == 1

    # 3. Query task list with status filter
    list_res = await async_client.get("/api/v1/tasks?status=DEAD")
    assert list_res.status_code == 200
    list_data = list_res.json()
    assert list_data["total"] >= 1
    assert any(item["id"] == task.id for item in list_data["items"])

    # 4. Trigger replay via API
    task_id = task.id
    replay_res = await async_client.post(f"/api/v1/tasks/{task_id}/retry")
    assert replay_res.status_code == 200
    replay_data = replay_res.json()
    assert replay_data["task_id"] == task_id
    assert replay_data["status"] == "PENDING"

    # Verify task state in DB
    db_session.expire_all()
    replayed_task = await DLQService.get_task_by_id(db_session, task_id)
    assert replayed_task.status == TaskStatus.PENDING.value
    assert replayed_task.retry_count == 0
    assert len(replayed_task.logs) == 2  # Original fail log + manual replay log


@pytest.mark.asyncio
async def test_cannot_replay_delivered_task(async_client: AsyncClient, db_session: AsyncSession):
    req = TaskCreateRequest(
        target_url="https://example.com/ok",
        method="POST",
        body="{}",
    )
    task, _ = await IngestionService.create_task(db_session, req)
    task.status = TaskStatus.DELIVERED.value
    await db_session.commit()

    # Replay on DELIVERED should return 400 Bad Request
    replay_res = await async_client.post(f"/api/v1/tasks/{task.id}/retry")
    assert replay_res.status_code == 400
    assert "DELIVERED" in replay_res.json()["detail"]
