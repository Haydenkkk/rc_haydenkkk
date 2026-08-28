from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.database import get_db_session
from app.models.schema import (
    HealthCheckResponse,
    RetryTaskResponse,
    TaskCreateRequest,
    TaskDetailResponse,
    TaskListResponse,
    TaskResponse,
)
from app.services.dlq import DLQService
from app.services.ingestion import IngestionService

router = APIRouter()

DbSessionDep = Annotated[AsyncSession, Depends(get_db_session)]


@router.post(
    "/api/v1/tasks",
    response_model=TaskResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Submit a notification task",
    description="Asynchronously ingest an HTTP notification request with idempotency and retry guarantees.",
)
async def submit_task(
    request: TaskCreateRequest,
    db: DbSessionDep,
) -> TaskResponse:
    task, _ = await IngestionService.create_task(db, request)
    return TaskResponse.model_validate(task)


@router.get(
    "/api/v1/tasks/{task_id}",
    response_model=TaskDetailResponse,
    summary="Get task details and delivery logs",
    description="Fetch real-time delivery status, error logs, and retry attempts for a specific task.",
)
async def get_task(
    task_id: str,
    db: DbSessionDep,
) -> TaskDetailResponse:
    task = await DLQService.get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task with ID '{task_id}' not found",
        )
    return TaskDetailResponse.model_validate(task)


@router.get(
    "/api/v1/tasks",
    response_model=TaskListResponse,
    summary="List notification tasks",
    description="Retrieve a paginated list of notification tasks with optional status filter.",
)
async def list_tasks(
    db: DbSessionDep,
    status_filter: Annotated[
        str | None,
        Query(
            alias="status",
            description="Filter tasks by status (PENDING, PROCESSING, DELIVERED, RETRYING, DEAD)",
        ),
    ] = None,
    page: Annotated[int, Query(ge=1, description="Page number")] = 1,
    page_size: Annotated[int, Query(ge=1, le=100, description="Items per page")] = 20,
) -> TaskListResponse:
    tasks, total = await DLQService.list_tasks(
        db=db,
        status=status_filter,
        page=page,
        page_size=page_size,
    )
    return TaskListResponse(
        items=[TaskResponse.model_validate(t) for t in tasks],
        total=total,
        page=page,
        page_size=page_size,
    )


@router.post(
    "/api/v1/tasks/{task_id}/retry",
    response_model=RetryTaskResponse,
    summary="Manually replay a dead or failed task",
    description="Re-queue a dead-lettered or failed task for immediate delivery attempt.",
)
async def replay_task(
    task_id: str,
    db: DbSessionDep,
) -> RetryTaskResponse:
    success, message, task = await DLQService.replay_task(db, task_id)
    if not task and not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=message,
        )
    if not success:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=message,
        )

    return RetryTaskResponse(
        task_id=task_id,
        status=task.status,
        message=message,
    )


@router.get(
    "/health",
    response_model=HealthCheckResponse,
    summary="Health check endpoint",
)
async def health_check() -> HealthCheckResponse:
    return HealthCheckResponse()
