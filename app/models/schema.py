from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class TaskCreateRequest(BaseModel):
    target_url: str = Field(
        ...,
        description="Target destination HTTP(S) URL",
        examples=["https://api.partner.com/webhook/v1"],
    )
    method: str = Field(
        default="POST",
        description="HTTP Method to invoke (POST, PUT, PATCH, GET)",
        examples=["POST"],
    )
    headers: dict[str, str] | None = Field(
        default=None,
        description="Custom HTTP headers to include with the delivery request",
        examples=[{"Authorization": "Bearer token123", "Content-Type": "application/json"}],
    )
    body: str | None = Field(
        default=None,
        description="Raw request payload to send to target URL",
        examples=['{"event": "payment_success", "user_id": "u_98765"}'],
    )
    idempotency_key: str | None = Field(
        default=None,
        max_length=128,
        description="Unique business key to prevent duplicate delivery tasks",
        examples=["order_pay_evt_20260828_001"],
    )
    max_retries: int | None = Field(
        default=5,
        ge=0,
        le=20,
        description="Maximum number of retry attempts on transient failures",
    )

    @field_validator("target_url")
    @classmethod
    def validate_url(cls, v: str) -> str:
        v = v.strip()
        if not v.startswith(("http://", "https://")):
            raise ValueError("target_url must start with http:// or https://")
        return v

    @field_validator("method")
    @classmethod
    def validate_method(cls, v: str) -> str:
        v = v.strip().upper()
        if v not in {"POST", "PUT", "PATCH", "GET", "DELETE"}:
            raise ValueError("method must be one of POST, PUT, PATCH, GET, DELETE")
        return v


class DeliveryLogResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    attempt_number: int
    response_status_code: int | None
    response_body_snippet: str | None
    duration_ms: int
    error_message: str | None
    created_at: datetime


class TaskResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    idempotency_key: str | None
    target_url: str
    method: str
    headers: dict[str, Any] | None = None
    body: str | None = None
    status: str
    retry_count: int
    max_retries: int
    next_retry_at: datetime
    last_error: str | None
    created_at: datetime
    updated_at: datetime


class TaskDetailResponse(TaskResponse):
    logs: list[DeliveryLogResponse] = []


class TaskListResponse(BaseModel):
    items: list[TaskResponse]
    total: int
    page: int
    page_size: int


class RetryTaskResponse(BaseModel):
    task_id: str
    status: str
    message: str


class HealthCheckResponse(BaseModel):
    status: str = "ok"
    version: str = "1.0.0"
    active_worker: bool = True
