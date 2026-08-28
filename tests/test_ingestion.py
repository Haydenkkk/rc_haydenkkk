import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_submit_task_success(async_client: AsyncClient):
    payload = {
        "target_url": "https://api.external-ad.com/callback",
        "method": "POST",
        "headers": {"Authorization": "Bearer key_abc123", "Content-Type": "application/json"},
        "body": '{"event": "user_signup", "ad_id": "ad_998"}',
        "max_retries": 3,
    }
    response = await async_client.post("/api/v1/tasks", json=payload)
    assert response.status_code == 202
    data = response.json()
    assert "id" in data
    assert data["target_url"] == "https://api.external-ad.com/callback"
    assert data["method"] == "POST"
    assert data["status"] == "PENDING"
    assert data["retry_count"] == 0
    assert data["max_retries"] == 3


@pytest.mark.asyncio
async def test_submit_task_validation_errors(async_client: AsyncClient):
    # Invalid URL scheme
    bad_url_payload = {
        "target_url": "ftp://bad-url.com",
        "method": "POST",
    }
    res = await async_client.post("/api/v1/tasks", json=bad_url_payload)
    assert res.status_code == 422

    # Invalid HTTP Method
    bad_method_payload = {
        "target_url": "https://api.partner.com/webhook",
        "method": "INVALID_METHOD",
    }
    res = await async_client.post("/api/v1/tasks", json=bad_method_payload)
    assert res.status_code == 422

    # Negative max_retries
    bad_retries_payload = {
        "target_url": "https://api.partner.com/webhook",
        "max_retries": -1,
    }
    res = await async_client.post("/api/v1/tasks", json=bad_retries_payload)
    assert res.status_code == 422


@pytest.mark.asyncio
async def test_idempotency_key_deduplication(async_client: AsyncClient):
    payload = {
        "target_url": "https://crm.partner.com/v1/contacts",
        "method": "POST",
        "body": '{"email": "test@example.com"}',
        "idempotency_key": "crm_sync_order_54321",
    }

    # First request creates task
    res1 = await async_client.post("/api/v1/tasks", json=payload)
    assert res1.status_code == 202
    task1 = res1.json()

    # Second request with identical idempotency_key returns existing task
    res2 = await async_client.post("/api/v1/tasks", json=payload)
    assert res2.status_code == 202
    task2 = res2.json()

    assert task1["id"] == task2["id"]
    assert task1["idempotency_key"] == "crm_sync_order_54321"
