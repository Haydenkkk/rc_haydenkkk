import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.schema import TaskCreateRequest
from app.models.task import TaskStatus
from app.services.ingestion import IngestionService


@pytest.mark.asyncio
async def test_pagination_and_status_filtering(async_client: AsyncClient, db_session: AsyncSession):
    # Ingest 5 tasks with various statuses
    for i in range(5):
        req = TaskCreateRequest(
            target_url=f"https://example.com/api/{i}",
            method="POST",
            body=f'{{"item": {i}}}',
        )
        task, _ = await IngestionService.create_task(db_session, req)
        if i == 0:
            task.status = TaskStatus.DELIVERED.value
        elif i == 1:
            task.status = TaskStatus.DEAD.value
        else:
            task.status = TaskStatus.PENDING.value
    await db_session.commit()

    # 1. Test pagination page_size=2
    res_p1 = await async_client.get("/api/v1/tasks?page=1&page_size=2")
    assert res_p1.status_code == 200
    data_p1 = res_p1.json()
    assert data_p1["total"] == 5
    assert len(data_p1["items"]) == 2
    assert data_p1["page"] == 1

    # 2. Test filter by DELIVERED
    res_deliv = await async_client.get("/api/v1/tasks?status=DELIVERED")
    assert res_deliv.status_code == 200
    data_deliv = res_deliv.json()
    assert data_deliv["total"] == 1
    assert data_deliv["items"][0]["status"] == "DELIVERED"

    # 3. Test filter by DEAD
    res_dead = await async_client.get("/api/v1/tasks?status=DEAD")
    assert res_dead.status_code == 200
    data_dead = res_dead.json()
    assert data_dead["total"] == 1
    assert data_dead["items"][0]["status"] == "DEAD"


@pytest.mark.asyncio
async def test_get_nonexistent_task(async_client: AsyncClient):
    res = await async_client.get("/api/v1/tasks/00000000-0000-0000-0000-000000000000")
    assert res.status_code == 404
    assert "not found" in res.json()["detail"].lower()


@pytest.mark.asyncio
async def test_replay_nonexistent_task(async_client: AsyncClient):
    res = await async_client.post("/api/v1/tasks/00000000-0000-0000-0000-000000000000/retry")
    assert res.status_code == 404
    assert "not found" in res.json()["detail"].lower()
