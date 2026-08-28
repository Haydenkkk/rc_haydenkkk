import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes import router
from app.db.database import close_db, init_db
from app.services.dispatcher import dispatcher

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)
logger = logging.getLogger("app.main")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application startup and graceful shutdown lifecycle."""
    logger.info("Initializing database tables...")
    await init_db()

    logger.info("Starting background delivery dispatcher...")
    await dispatcher.start()

    yield

    logger.info("Stopping background delivery dispatcher...")
    await dispatcher.stop()

    logger.info("Closing database connections...")
    await close_db()


app = FastAPI(
    title="API Notification Delivery System",
    description=(
        "A robust, asynchronous, reliable outbound HTTP notification engine "
        "providing at-least-once delivery guarantees, exponential backoff with full jitter, "
        "strict timeout isolation, idempotency control, and dead letter queue management."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

# Enable CORS for potential admin dashboard integration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=False)
