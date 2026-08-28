import asyncio

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

app = FastAPI(title="Mock External Providers")

# State tracker for flaky endpoints
call_counts: dict[str, int] = {}


@app.post("/mock/ad/success")
async def ad_callback(request: Request):
    """Simulates a third-party ad tracking callback that succeeds immediately."""
    body = await request.body()
    return JSONResponse(
        status_code=200,
        content={"status": "success", "provider": "AdNetwork", "received_bytes": len(body)},
    )


@app.post("/mock/crm/flaky")
async def crm_flaky_callback(request: Request):
    """
    Simulates a flaky CRM provider:
    Fails with HTTP 503 for the first 2 calls, succeeds on call #3.
    """
    call_counts["crm"] = call_counts.get("crm", 0) + 1
    count = call_counts["crm"]
    if count < 3:
        return JSONResponse(
            status_code=503,
            content={"error": "CRM database overloaded, retry later", "attempt": count},
        )
    return JSONResponse(
        status_code=200,
        content={"status": "updated", "provider": "CRM", "attempt": count},
    )


@app.post("/mock/inventory/auth-fail")
async def inventory_auth_fail(request: Request):
    """Simulates an authentication error (HTTP 401), which is non-retryable."""
    return JSONResponse(
        status_code=401,
        content={"error": "Invalid API token", "provider": "InventorySync"},
    )


@app.post("/mock/partner/timeout")
async def partner_timeout(request: Request):
    """Simulates a hung external service that causes connection / read timeout."""
    await asyncio.sleep(15.0)
    return JSONResponse(status_code=200, content={"status": "too_late"})


@app.get("/mock/stats")
async def get_stats():
    """Returns call counts for debugging and verification."""
    return {"call_counts": call_counts}


@app.post("/mock/reset")
async def reset_stats():
    """Reset call counts."""
    call_counts.clear()
    return {"status": "reset"}


if __name__ == "__main__":
    uvicorn.run("mock_server:app", host="127.0.0.1", port=9000, reload=False)
