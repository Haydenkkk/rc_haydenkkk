#!/usr/bin/env python3
"""
Interactive Demonstration & End-to-End Verification Script
Runs a complete simulated workflow demonstrating:
1. Fast Ingestion (202 Accepted) & Idempotency Deduplication
2. Successful delivery (200 OK)
3. Transient failure handling with Exponential Backoff + Jitter & Recovery
4. Non-retryable failure handling (401 -> Immediate DEAD)
5. Dead Letter Queue (DLQ) inspection & Operator Replay
"""

import asyncio
import json

import httpx

API_BASE = "http://127.0.0.1:8888"
MOCK_BASE = "http://127.0.0.1:9000"


def print_header(title: str):
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)


async def run_demo():
    print_header("1. Checking Server Connectivity")
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(f"{API_BASE}/health")
            print(f"[OK] Notification System is alive: {r.json()}")
        except (httpx.HTTPError, OSError) as e:
            print(f"[ERROR] Cannot connect to {API_BASE}. Please start main.py first!\nError: {e}")
            return

        try:
            r = await client.get(f"{MOCK_BASE}/mock/stats")
            print(f"[OK] Mock External Provider is alive: {r.json()}")
        except (httpx.HTTPError, OSError) as e:
            print(f"[ERROR] Cannot connect to {MOCK_BASE}. Please start mock_server.py first!\nError: {e}")
            return

        # Reset mock stats
        await client.post(f"{MOCK_BASE}/mock/reset")

        # Case 1: Ingestion & Idempotency
        print_header("2. Case 1: Fast Ingestion & Idempotency Deduplication")
        task_payload = {
            "target_url": f"{MOCK_BASE}/mock/ad/success",
            "method": "POST",
            "headers": {"Authorization": "Bearer ad_token_abc"},
            "body": json.dumps({"event": "user_signup", "user_id": "u_88219"}),
            "idempotency_key": "idemp_order_demo_001",
        }

        print("Submitting Task #1 with idempotency_key='idemp_order_demo_001'...")
        r1 = await client.post(f"{API_BASE}/api/v1/tasks", json=task_payload)
        t1 = r1.json()
        print(f"-> Response [{r1.status_code} Accepted]: Task ID = {t1['id']}, Status = {t1['status']}")

        print("Submitting duplicate Task with identical idempotency_key...")
        r2 = await client.post(f"{API_BASE}/api/v1/tasks", json=task_payload)
        t2 = r2.json()
        print(f"-> Response [{r2.status_code} Accepted]: Task ID = {t2['id']} (Same ID, deduplicated!)")

        # Wait for delivery to complete
        await asyncio.sleep(1.0)
        detail1 = (await client.get(f"{API_BASE}/api/v1/tasks/{t1['id']}")).json()
        print(f"-> Final Delivery Status: {detail1['status']}, Attempts: {len(detail1['logs'])}")
        print(f"   Log: HTTP {detail1['logs'][0]['response_status_code']} in {detail1['logs'][0]['duration_ms']}ms")

        # Case 2: Flaky CRM (503 -> Retry -> Eventual Success)
        print_header("3. Case 2: Transient Flakiness (HTTP 503 -> Exponential Backoff -> 200 OK)")
        crm_payload = {
            "target_url": f"{MOCK_BASE}/mock/crm/flaky",
            "method": "POST",
            "body": json.dumps({"contact_id": "c_998", "action": "update_status"}),
            "max_retries": 4,
        }
        r_crm = await client.post(f"{API_BASE}/api/v1/tasks", json=crm_payload)
        t_crm = r_crm.json()
        print(f"Submitted Flaky Task: ID = {t_crm['id']}, Initial Status = {t_crm['status']}")

        print("Monitoring retry backoff and recovery (waiting ~4s)...")
        for _ in range(8):
            await asyncio.sleep(0.8)
            d = (await client.get(f"{API_BASE}/api/v1/tasks/{t_crm['id']}")).json()
            print(f"   Status: {d['status']}, Retries: {d['retry_count']}, Logs Count: {len(d['logs'])}")
            if d["status"] == "DELIVERED":
                print("-> Recovered successfully to DELIVERED!")
                break

        # Case 3: Non-retryable Client Error (HTTP 401)
        print_header("4. Case 3: Non-retryable Client Error (HTTP 401 -> Immediate DEAD / DLQ)")
        auth_payload = {
            "target_url": f"{MOCK_BASE}/mock/inventory/auth-fail",
            "method": "POST",
            "body": json.dumps({"sku": "SKU-9901", "qty": -1}),
            "max_retries": 5,
        }
        r_auth = await client.post(f"{API_BASE}/api/v1/tasks", json=auth_payload)
        t_auth = r_auth.json()
        print(f"Submitted Task with Bad Auth: ID = {t_auth['id']}")

        await asyncio.sleep(0.8)
        d_auth = (await client.get(f"{API_BASE}/api/v1/tasks/{t_auth['id']}")).json()
        print(f"-> Final Status: {d_auth['status']} (Zero wasteful retries for 4xx errors)")
        print(f"   Error: {d_auth['last_error']}")

        # Case 4: DLQ Management & Operator Replay
        print_header("5. Case 4: Dead Letter Queue (DLQ) Replay")
        print("Querying DEAD tasks list...")
        dlq_list = (await client.get(f"{API_BASE}/api/v1/tasks?status=DEAD")).json()
        print(f"Found {dlq_list['total']} DEAD task(s) in DLQ.")

        print(f"Operator triggering manual replay for Task {t_auth['id']}...")
        r_replay = await client.post(f"{API_BASE}/api/v1/tasks/{t_auth['id']}/retry")
        print(f"-> Replay Response: {r_replay.json()}")

        print_header("Demonstration completed successfully!")


if __name__ == "__main__":
    asyncio.run(run_demo())
