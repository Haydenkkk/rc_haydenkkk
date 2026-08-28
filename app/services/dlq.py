import logging

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models.task import DeliveryLog, Task, TaskStatus, utc_now

logger = logging.getLogger(__name__)


class DLQService:
    @staticmethod
    async def get_task_by_id(db: AsyncSession, task_id: str) -> Task | None:
        """Fetch a single task along with its delivery attempt logs."""
        stmt = (
            select(Task)
            .where(Task.id == task_id)
            .options(selectinload(Task.logs))
        )
        result = await db.execute(stmt)
        return result.scalars().first()

    @staticmethod
    async def list_tasks(
        db: AsyncSession,
        status: str | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> tuple[list[Task], int]:
        """List tasks with optional status filter and pagination."""
        page = max(page, 1)
        if page_size < 1 or page_size > 100:
            page_size = 20

        offset = (page - 1) * page_size

        count_stmt = select(func.count(Task.id))
        if status:
            count_stmt = count_stmt.where(Task.status == status.upper())

        total_result = await db.execute(count_stmt)
        total = total_result.scalar_one()

        stmt = select(Task).order_by(Task.created_at.desc()).offset(offset).limit(page_size)
        if status:
            stmt = stmt.where(Task.status == status.upper())

        result = await db.execute(stmt)
        tasks = list(result.scalars().all())
        return tasks, total

    @staticmethod
    async def replay_task(
        db: AsyncSession,
        task_id: str,
        reset_retry_count: bool = True,
    ) -> tuple[bool, str, Task | None]:
        """
        Manually replay a failed or dead task.
        Re-queues the task as PENDING for immediate delivery.
        """
        task = await DLQService.get_task_by_id(db, task_id)
        if not task:
            return False, f"Task {task_id} not found", None

        if task.status not in {TaskStatus.DEAD.value, TaskStatus.RETRYING.value}:
            return False, f"Task {task_id} has status '{task.status}', only DEAD or RETRYING tasks can be manually replayed", task

        # Reset state for retry
        previous_status = task.status
        task.status = TaskStatus.PENDING.value
        task.next_retry_at = utc_now()
        if reset_retry_count:
            task.retry_count = 0
        task.last_error = f"[Manual Replay Triggered from {previous_status}] Previous error: {task.last_error or 'None'}"

        # Record manual intervention log
        manual_log = DeliveryLog(
            task_id=task.id,
            attempt_number=len(task.logs) + 1,
            response_status_code=None,
            response_body_snippet=None,
            duration_ms=0,
            error_message="Manual replay triggered by operator",
        )
        db.add(manual_log)
        await db.commit()
        await db.refresh(task)

        logger.info("Manually replayed task: id=%s new_status=PENDING", task.id)
        try:
            from app.services.dispatcher import dispatcher
            dispatcher.wake_up()
        except (ImportError, RuntimeError, AttributeError) as ex:
            logger.debug("Dispatcher wake-up ignored: %s", ex)

        return True, "Task successfully re-queued for immediate delivery", task
