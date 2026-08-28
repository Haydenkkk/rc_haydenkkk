import asyncio
import logging
from datetime import timedelta

import httpx
from sqlalchemy import select, update
from sqlalchemy.orm import selectinload

import app.db.database as db_module
from app.config import settings
from app.models.task import DeliveryLog, Task, TaskStatus, utc_now
from app.services.delivery import DeliveryEngine

logger = logging.getLogger(__name__)


class Dispatcher:
    def __init__(self, session_factory=None):
        self._running = False
        self._task: asyncio.Task | None = None
        self._wake_event = asyncio.Event()
        self._semaphore = asyncio.Semaphore(settings.MAX_CONCURRENT_DELIVERIES)
        self._http_client: httpx.AsyncClient | None = None
        self._session_factory = session_factory
        self._active_tasks: set[asyncio.Task] = set()
        self._last_sweep_time: float = 0.0

    @property
    def session_factory(self):
        return self._session_factory or db_module.async_session_factory

    async def start(self) -> None:
        """Start the background dispatcher worker and run an initial orphan sweep."""
        if self._running:
            return
        self._running = True
        self._http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(
                connect=settings.HTTP_CONNECT_TIMEOUT,
                read=settings.HTTP_READ_TIMEOUT,
                write=settings.HTTP_WRITE_TIMEOUT,
                pool=settings.HTTP_POOL_TIMEOUT,
            ),
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
        )

        # Initial recovery of orphaned tasks from previous process restarts
        try:
            recovered = await self.recover_orphaned_tasks()
            if recovered > 0:
                logger.info("Startup orphan sweep recovered %d tasks", recovered)
        except Exception:
            logger.exception("Error during initial orphan task recovery sweep")

        self._task = asyncio.create_task(self._worker_loop(), name="notification-dispatcher")
        logger.info("Notification Dispatcher background worker started.")

    async def stop(self) -> None:
        """Gracefully stop the background dispatcher worker and drain active deliveries."""
        if not self._running:
            return
        self._running = False
        self._wake_event.set()

        # 1. Stop main poll loop
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

        # 2. Wait for active in-flight delivery tasks to drain
        if self._active_tasks:
            logger.info("Waiting for %d active in-flight deliveries to complete...", len(self._active_tasks))
            _done, pending = await asyncio.wait(
                self._active_tasks,
                timeout=settings.GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS,
            )
            if pending:
                logger.warning(
                    "Graceful shutdown timeout exceeded; cancelling %d remaining delivery tasks",
                    len(pending),
                )
                for p in pending:
                    p.cancel()
                await asyncio.gather(*pending, return_exceptions=True)

        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None

        logger.info("Notification Dispatcher background worker stopped cleanly.")

    def wake_up(self) -> None:
        """Signal the dispatcher loop to check for newly ingested tasks immediately."""
        self._wake_event.set()

    async def recover_orphaned_tasks(self, timeout_seconds: float | None = None) -> int:
        """
        Recovers tasks stuck in PROCESSING state due to unexpected process crashes or OOM.
        Reverts their status to RETRYING and schedules them for immediate retry.
        """
        timeout = timeout_seconds if timeout_seconds is not None else settings.ORPHAN_TASK_TIMEOUT_SECONDS
        threshold = utc_now() - timedelta(seconds=timeout)

        async with self.session_factory() as db:
            stmt = (
                select(Task)
                .where(
                    Task.status == TaskStatus.PROCESSING.value,
                    Task.updated_at <= threshold,
                )
                .options(selectinload(Task.logs))
            )
            result = await db.execute(stmt)
            orphaned_tasks = list(result.scalars().all())

            if not orphaned_tasks:
                return 0

            for task in orphaned_tasks:
                task.status = TaskStatus.RETRYING.value
                task.next_retry_at = utc_now()
                task.last_error = f"[Orphan Recovered] Task was stuck in PROCESSING for >{int(timeout)}s"

                log_entry = DeliveryLog(
                    task_id=task.id,
                    attempt_number=len(task.logs) + 1,
                    response_status_code=None,
                    response_body_snippet=None,
                    duration_ms=0,
                    error_message="Orphan sweeper: recovered stuck PROCESSING task back to RETRYING",
                )
                db.add(log_entry)

            await db.commit()
            logger.warning("Recovered %d orphaned tasks stuck in PROCESSING", len(orphaned_tasks))
            return len(orphaned_tasks)

    async def _worker_loop(self) -> None:
        """Main event loop for periodically polling and dispatching tasks."""
        import time

        while self._running:
            try:
                # Periodic orphan recovery sweep
                current_time = time.monotonic()
                if current_time - self._last_sweep_time >= settings.ORPHAN_SWEEPER_INTERVAL_SECONDS:
                    self._last_sweep_time = current_time
                    await self.recover_orphaned_tasks()

                dispatched_count = await self.dispatch_batch()
                # If there were tasks processed, do a quick yield; otherwise wait for interval or wake event
                if dispatched_count > 0:
                    await asyncio.sleep(0.05)
                else:
                    try:
                        await asyncio.wait_for(
                            self._wake_event.wait(),
                            timeout=settings.DISPATCHER_POLL_INTERVAL,
                        )
                        self._wake_event.clear()
                    except TimeoutError:
                        pass
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Error in dispatcher worker loop")
                await asyncio.sleep(1.0)

    async def dispatch_batch(self, batch_size: int | None = None) -> int:
        """
        Polls database for due tasks, claims them atomically via UPDATE ... RETURNING,
        and spawns delivery worker coroutines.
        """
        limit = batch_size or settings.DISPATCHER_BATCH_SIZE
        now = utc_now()

        async with self.session_factory() as db:
            # 1. Query pending / retrying candidate task IDs
            candidate_stmt = (
                select(Task.id)
                .where(
                    Task.status.in_([TaskStatus.PENDING.value, TaskStatus.RETRYING.value]),
                    Task.next_retry_at <= now,
                )
                .order_by(Task.next_retry_at.asc())
                .limit(limit)
            )
            result = await db.execute(candidate_stmt)
            candidate_ids = list(result.scalars().all())

            if not candidate_ids:
                return 0

            # 2. Atomic batch claim with RETURNING (eliminating N+1 single UPDATE queries)
            claim_stmt = (
                update(Task)
                .where(
                    Task.id.in_(candidate_ids),
                    Task.status.in_([TaskStatus.PENDING.value, TaskStatus.RETRYING.value]),
                )
                .values(status=TaskStatus.PROCESSING.value, updated_at=utc_now())
                .returning(Task.id)
            )
            claim_result = await db.execute(claim_stmt)
            claimed_ids = list(claim_result.scalars().all())
            await db.commit()

        # 3. Dispatch deliveries asynchronously with task tracking
        for tid in claimed_ids:
            task = asyncio.create_task(self._process_single_task(tid))
            self._active_tasks.add(task)
            task.add_done_callback(self._active_tasks.discard)

        return len(claimed_ids)

    async def _process_single_task(self, task_id: str) -> None:
        """Processes a single claimed task under concurrency semaphore limit."""
        async with self._semaphore, self.session_factory() as db:
            try:
                await DeliveryEngine.deliver_task(
                    db=db,
                    task_id=task_id,
                    http_client=self._http_client,
                )
            except Exception:
                logger.exception("Unhandled error delivering task %s", task_id)


dispatcher = Dispatcher()
