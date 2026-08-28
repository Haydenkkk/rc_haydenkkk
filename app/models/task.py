import enum
import json
import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import relationship

from app.db.database import Base


def utc_now() -> datetime:
    """Return current UTC timestamp with timezone."""
    return datetime.now(UTC)


class TaskStatus(str, enum.Enum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    DELIVERED = "DELIVERED"
    RETRYING = "RETRYING"
    DEAD = "DEAD"


class Task(Base):
    __tablename__ = "tasks"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    idempotency_key = Column(String(128), unique=True, index=True, nullable=True)
    target_url = Column(String(2048), nullable=False)
    method = Column(String(10), nullable=False, default="POST")
    headers_json = Column(Text, nullable=True, default="{}")
    body = Column(Text, nullable=True)
    status = Column(String(20), nullable=False, default=TaskStatus.PENDING.value, index=True)
    retry_count = Column(Integer, nullable=False, default=0)
    max_retries = Column(Integer, nullable=False, default=5)
    next_retry_at = Column(DateTime(timezone=True), nullable=False, default=utc_now, index=True)
    last_error = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=utc_now)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=utc_now, onupdate=utc_now)

    # Relationships
    logs = relationship(
        "DeliveryLog",
        back_populates="task",
        cascade="all, delete-orphan",
        order_by="DeliveryLog.attempt_number",
        lazy="selectin",
    )

    __table_args__ = (
        Index("ix_tasks_status_next_retry", "status", "next_retry_at"),
    )

    @property
    def headers(self) -> dict[str, str]:
        if not self.headers_json:
            return {}
        try:
            return json.loads(self.headers_json)
        except (json.JSONDecodeError, TypeError):
            return {}

    @headers.setter
    def headers(self, value: dict[str, Any] | None) -> None:
        if value:
            self.headers_json = json.dumps(value)
        else:
            self.headers_json = "{}"


class DeliveryLog(Base):
    __tablename__ = "delivery_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(String(36), ForeignKey("tasks.id", ondelete="CASCADE"), nullable=False, index=True)
    attempt_number = Column(Integer, nullable=False)
    response_status_code = Column(Integer, nullable=True)
    response_body_snippet = Column(Text, nullable=True)
    duration_ms = Column(Integer, nullable=False, default=0)
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=utc_now)

    task = relationship("Task", back_populates="logs")
