import httpx
import pytest
import respx
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.schema import TaskCreateRequest
from app.models.task import TaskStatus
from app.services.delivery import DeliveryEngine
from app.services.ingestion import IngestionService


def test_backoff_calculation():
    # Test without jitter for exact bounds
    original_jitter = settings.ENABLE_JITTER
    try:
        settings.ENABLE_JITTER = False
        assert DeliveryEngine.calculate_backoff(0) == 1.0
        assert DeliveryEngine.calculate_backoff(1) == 2.0
        assert DeliveryEngine.calculate_backoff(2) == 4.0
        assert DeliveryEngine.calculate_backoff(3) == 8.0
        assert DeliveryEngine.calculate_backoff(10) == 60.0  # capped at MAX_BACKOFF_SECONDS

        # Test with jitter
        settings.ENABLE_JITTER = True
        for retry_count in range(5):
            val = DeliveryEngine.calculate_backoff(retry_count)
            max_expected = min(settings.MAX_BACKOFF_SECONDS, settings.INITIAL_BACKOFF_SECONDS * (2**retry_count))
            assert 0.1 <= val <= max_expected
    finally:
        settings.ENABLE_JITTER = original_jitter


def test_is_retryable_status_code():
    assert DeliveryEngine.is_retryable_status_code(200) is False
    assert DeliveryEngine.is_retryable_status_code(201) is False
    assert DeliveryEngine.is_retryable_status_code(204) is False
    assert DeliveryEngine.is_retryable_status_code(400) is False
    assert DeliveryEngine.is_retryable_status_code(401) is False
    assert DeliveryEngine.is_retryable_status_code(403) is False
    assert DeliveryEngine.is_retryable_status_code(404) is False
    assert DeliveryEngine.is_retryable_status_code(422) is False
    assert DeliveryEngine.is_retryable_status_code(429) is True  # Rate limit is retryable
    assert DeliveryEngine.is_retryable_status_code(500) is True
    assert DeliveryEngine.is_retryable_status_code(502) is True
    assert DeliveryEngine.is_retryable_status_code(503) is True
    assert DeliveryEngine.is_retryable_status_code(504) is True


@pytest.mark.asyncio
@respx.mock
async def test_deliver_task_200_success(db_session: AsyncSession):
    url = "https://ad.provider.com/events"
    respx.post(url).mock(return_value=httpx.Response(200, json={"result": "ok"}))

    req = TaskCreateRequest(
        target_url=url,
        method="POST",
        body='{"event": "install"}',
        max_retries=3,
    )
    task, _ = await IngestionService.create_task(db_session, req)

    delivered_task = await DeliveryEngine.deliver_task(db_session, task.id)
    assert delivered_task is not None
    assert delivered_task.status == TaskStatus.DELIVERED.value
    assert delivered_task.retry_count == 0
    assert len(delivered_task.logs) == 1
    assert delivered_task.logs[0].response_status_code == 200
    assert delivered_task.logs[0].error_message is None


@pytest.mark.asyncio
@respx.mock
async def test_deliver_task_503_transient_retry(db_session: AsyncSession):
    url = "https://crm.provider.com/webhook"
    respx.post(url).mock(return_value=httpx.Response(503, text="Service Unavailable"))

    req = TaskCreateRequest(
        target_url=url,
        method="POST",
        body='{"user_id": 123}',
        max_retries=3,
    )
    task, _ = await IngestionService.create_task(db_session, req)

    updated_task = await DeliveryEngine.deliver_task(db_session, task.id)
    assert updated_task is not None
    assert updated_task.status == TaskStatus.RETRYING.value
    assert updated_task.retry_count == 1
    assert "503" in updated_task.last_error
    assert len(updated_task.logs) == 1
    assert updated_task.logs[0].response_status_code == 503


@pytest.mark.asyncio
@respx.mock
async def test_deliver_task_400_non_retryable_client_error(db_session: AsyncSession):
    url = "https://inventory.provider.com/stock"
    respx.post(url).mock(return_value=httpx.Response(400, text="Bad Request: Missing Field"))

    req = TaskCreateRequest(
        target_url=url,
        method="POST",
        body='{"bad": "data"}',
        max_retries=5,
    )
    task, _ = await IngestionService.create_task(db_session, req)

    updated_task = await DeliveryEngine.deliver_task(db_session, task.id)
    assert updated_task is not None
    # 400 is non-retryable, should immediately go to DEAD
    assert updated_task.status == TaskStatus.DEAD.value
    assert updated_task.retry_count == 0
    assert "Non-retryable HTTP 400" in updated_task.last_error
    assert len(updated_task.logs) == 1
    assert updated_task.logs[0].response_status_code == 400


@pytest.mark.asyncio
@respx.mock
async def test_deliver_task_timeout(db_session: AsyncSession):
    url = "https://slow.provider.com/notify"
    respx.post(url).mock(side_effect=httpx.ReadTimeout("Read timed out"))

    req = TaskCreateRequest(
        target_url=url,
        method="POST",
        body='{"data": 1}',
        max_retries=2,
    )
    task, _ = await IngestionService.create_task(db_session, req)

    updated_task = await DeliveryEngine.deliver_task(db_session, task.id)
    assert updated_task is not None
    assert updated_task.status == TaskStatus.RETRYING.value
    assert updated_task.retry_count == 1
    assert "Timeout" in updated_task.last_error
    assert len(updated_task.logs) == 1
    assert updated_task.logs[0].response_status_code is None
