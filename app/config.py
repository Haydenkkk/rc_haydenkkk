from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Server Settings
    PORT: int = 8888
    HOST: str = "127.0.0.1"

    # Database
    DATABASE_URL: str = "sqlite+aiosqlite:///./notification_system.db"
    SQL_ECHO: bool = False

    # Delivery & Retry Settings
    DEFAULT_MAX_RETRIES: int = 5
    INITIAL_BACKOFF_SECONDS: float = 1.0
    MAX_BACKOFF_SECONDS: float = 60.0
    BACKOFF_MULTIPLIER: float = 2.0
    ENABLE_JITTER: bool = True

    # HTTP Client Timeout Settings
    HTTP_CONNECT_TIMEOUT: float = 3.0
    HTTP_READ_TIMEOUT: float = 10.0
    HTTP_WRITE_TIMEOUT: float = 10.0
    HTTP_POOL_TIMEOUT: float = 5.0

    # Dispatcher Worker Settings
    DISPATCHER_POLL_INTERVAL: float = 0.5
    DISPATCHER_BATCH_SIZE: int = 20
    MAX_CONCURRENT_DELIVERIES: int = 50

    # Orphan Recovery & Sweeper Settings
    ORPHAN_TASK_TIMEOUT_SECONDS: float = 300.0  # 5 minutes
    ORPHAN_SWEEPER_INTERVAL_SECONDS: float = 60.0  # 1 minute

    # Graceful Shutdown Settings
    GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS: float = 10.0

    model_config = SettingsConfigDict(
        env_prefix="NOTIF_",
        env_file=".env",
        extra="ignore",
    )


settings = Settings()
