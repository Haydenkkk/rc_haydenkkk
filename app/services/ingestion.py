import json
import logging

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.schema import TaskCreateRequest
from app.models.task import Task, TaskStatus, utc_now

logger = logging.getLogger(__name__)


class IngestionService:
    @staticmethod
    async def create_task(
        db: AsyncSession,
        request: TaskCreateRequest,
    ) -> tuple[Task, bool]:
        """
        Ingests a notification task.
        If idempotency_key is provided and already exists, returns (existing_task, False).
        Otherwise creates and commits a new task, returning (new_task, True).
        """
        # 1. Idempotency check
        if request.idempotency_key:
            stmt = select(Task).where(Task.idempotency_key == request.idempotency_key)
            result = await db.execute(stmt)
            existing_task = result.scalars().first()
            if existing_task:
                logger.info(
                    "Duplicate request ignored via idempotency_key: %s (task_id=%s)",
                    request.idempotency_key,
                    existing_task.id,
                )
                return existing_task, False

        # 2. Construct new Task
        max_retries = (
            request.max_retries
            if request.max_retries is not None
            else settings.DEFAULT_MAX_RETRIES
        )

        headers_str = "{}"
        if request.headers:
            headers_str = json.dumps(request.headers)

        new_task = Task(
            idempotency_key=request.idempotency_key,
            target_url=request.target_url,
            method=request.method,
            headers_json=headers_str,
            body=request.body,
            status=TaskStatus.PENDING.value,
            retry_count=0,
            max_retries=max_retries,
            next_retry_at=utc_now(),
        )

        db.add(new_task)
        await db.commit()
        await db.refresh(new_task)

        logger.info(
            "Created notification task: id=%s target_url=%s status=%s",
            new_task.id,
            new_task.target_url,
            new_task.status,
        )
        try:
            from app.services.dispatcher import dispatcher
            dispatcher.wake_up()
        except (ImportError, RuntimeError, AttributeError) as ex:
            logger.debug("Dispatcher wake-up ignored: %s", ex)

        return new_task, True
