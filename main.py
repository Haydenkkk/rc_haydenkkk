import uvicorn

from app.config import settings


def main():
    """Entry point for running the API Notification System."""
    print(f"Starting API Notification System on http://{settings.HOST}:{settings.PORT} ...")
    uvicorn.run("app.main:app", host=settings.HOST, port=settings.PORT, reload=False)


if __name__ == "__main__":
    main()
