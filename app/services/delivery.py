import logging
import random
import time
from datetime import timedelta

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.config import settings
from app.models.task import DeliveryLog, Task, TaskStatus, utc_now

logger = logging.getLogger(__name__)


class DeliveryEngine:
    @staticmethod
    def calculate_backoff(retry_count: int) -> float:
        """
        Calculate exponential backoff with full jitter:
        interval = min(max_backoff, initial_backoff * multiplier^retry_count)
        wait_time = random.uniform(0, interval) (if jitter enabled)
        """
        multiplier = settings.BACKOFF_MULTIPLIER**retry_count
        raw_interval = settings.INITIAL_BACKOFF_SECONDS * multiplier
        capped_interval = min(settings.MAX_BACKOFF_SECONDS, raw_interval)

        if settings.ENABLE_JITTER:
            # Full Jitter prevents thundering herd / retry storms
            return random.uniform(0.1, capped_interval)
        return capped_interval

    @staticmethod
    def is_retryable_status_code(status_code: int) -> bool:
        """
        Determine if HTTP status code is transient/retryable.
        - 2xx: Success, not a retryable error.
        - 429: Too Many Requests (Rate limit), retryable.
        - 5xx: Server errors (500, 502, 503, 504), retryable.
        - Other 4xx (400, 401, 403, 404, 422): Client errors, not retryable.
        """
        if 200 <= status_code < 300:
            return False
        return status_code == 429 or 500 <= status_code < 600

    @staticmethod
    async def deliver_task(
        db: AsyncSession,
        task_id: str,
        http_client: httpx.AsyncClient | None = None,
    ) -> Task | None:
        """
        Executes HTTP delivery for a single task and updates task state & delivery logs.
        """
        stmt = (
            select(Task)
            .where(Task.id == task_id)
            .options(selectinload(Task.logs))
        )
        result = await db.execute(stmt)
        task = result.scalars().first()

        if not task:
            logger.warning("Task %s not found for delivery", task_id)
            return None

        # Prepare HTTP parameters
        headers = task.headers
        headers.setdefault("User-Agent", "OutboundNotificationEngine/1.0")
        headers.setdefault("X-Task-ID", task.id)
        if task.idempotency_key:
            headers.setdefault("X-Idempotency-Key", task.idempotency_key)

        timeout = httpx.Timeout(
            connect=settings.HTTP_CONNECT_TIMEOUT,
            read=settings.HTTP_READ_TIMEOUT,
            write=settings.HTTP_WRITE_TIMEOUT,
            pool=settings.HTTP_POOL_TIMEOUT,
        )

        start_time = time.perf_counter()
        status_code: int | None = None
        response_snippet: str | None = None
        error_message: str | None = None
        success = False
        is_transient_failure = False

        close_client = False
        client = http_client
        if client is None:
            client = httpx.AsyncClient(timeout=timeout)
            close_client = True

        try:
            response = await client.request(
                method=task.method,
                url=task.target_url,
                headers=headers,
                content=task.body.encode("utf-8") if task.body else None,
            )
            elapsed_ms = int((time.perf_counter() - start_time) * 1000)
            status_code = response.status_code

            # Capture snippet of response body for debugging (first 500 chars)
            try:
                response_snippet = response.text[:500]
            except (UnicodeDecodeError, AttributeError):
                response_snippet = None

            if 200 <= status_code < 300:
                success = True
                logger.info(
                    "Task %s delivered successfully with HTTP %d in %dms",
                    task.id,
                    status_code,
                    elapsed_ms,
                )
            elif DeliveryEngine.is_retryable_status_code(status_code):
                is_transient_failure = True
                error_message = f"Transient HTTP {status_code}: {response_snippet or 'Server error'}"
                logger.warning(
                    "Task %s received retryable HTTP %d in %dms",
                    task.id,
                    status_code,
                    elapsed_ms,
                )
            else:
                # 4xx client error: non-retryable
                is_transient_failure = False
                error_message = f"Non-retryable HTTP {status_code}: {response_snippet or 'Client error'}"
                logger.warning(
                    "Task %s received non-retryable HTTP %d in %dms",
                    task.id,
                    status_code,
                    elapsed_ms,
                )

        except (httpx.ConnectTimeout, httpx.ReadTimeout, httpx.WriteTimeout, httpx.PoolTimeout) as ex:
            elapsed_ms = int((time.perf_counter() - start_time) * 1000)
            is_transient_failure = True
            error_message = f"HTTP Timeout ({ex.__class__.__name__}) after {elapsed_ms}ms"
            logger.warning("Task %s timed out: %s", task.id, error_message)

        except (httpx.ConnectError, httpx.NetworkError, httpx.TransportError) as ex:
            elapsed_ms = int((time.perf_counter() - start_time) * 1000)
            is_transient_failure = True
            error_message = f"Network connection error: {ex!s}"
            logger.warning("Task %s network error: %s", task.id, error_message)

        except Exception as ex:
            elapsed_ms = int((time.perf_counter() - start_time) * 1000)
            is_transient_failure = True
            error_message = f"Unexpected delivery error: {ex!s}"
            logger.exception("Task %s unexpected error during delivery", task.id)

        finally:
            if close_client and client is not None:
                await client.aclose()

        # Update task state & append delivery log
        attempt_number = len(task.logs) + 1
        log_entry = DeliveryLog(
            task_id=task.id,
            attempt_number=attempt_number,
            response_status_code=status_code,
            response_body_snippet=response_snippet,
            duration_ms=elapsed_ms,
            error_message=error_message,
        )
        db.add(log_entry)

        if success:
            task.status = TaskStatus.DELIVERED.value
            task.last_error = None
        elif is_transient_failure:
            task.retry_count += 1
            if task.retry_count >= task.max_retries:
                # Exceeded max retry quota -> Dead Letter Queue
                task.status = TaskStatus.DEAD.value
                task.last_error = (
                    f"Exceeded max retries ({task.max_retries}). Last error: {error_message}"
                )
                logger.error("Task %s moved to DEAD (DLQ). Retries exhausted.", task.id)
            else:
                # Calculate backoff delay
                backoff_seconds = DeliveryEngine.calculate_backoff(task.retry_count)
                task.status = TaskStatus.RETRYING.value
                task.next_retry_at = utc_now() + timedelta(seconds=backoff_seconds)
                task.last_error = error_message
                logger.info(
                    "Task %s scheduled for retry #%d in %.2fs at %s",
                    task.id,
                    task.retry_count,
                    backoff_seconds,
                    task.next_retry_at,
                )
        else:
            # Non-retryable error (e.g. 400 Bad Request, 401 Unauthorized, 404 Not Found)
            task.status = TaskStatus.DEAD.value
            task.last_error = error_message
            logger.error(
                "Task %s moved to DEAD (DLQ) due to non-retryable error: %s",
                task.id,
                error_message,
            )

        task.updated_at = utc_now()
        await db.commit()
        await db.refresh(task)
        return task
