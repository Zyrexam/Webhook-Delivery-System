import asyncio
import time
import aiohttp
import hmac
import hashlib
import json
from sqlalchemy import text
from app.database import AsyncSessionLocal
from app.redis_client import redis
from app.circuit_breaker import get_circuit_state, try_transition_to_half_open, record_success, record_failure

BACKOFF_BASE = 2
MAX_ATTEMPTS = 5

RETRY_DEQUEUE_SCRIPT = """
    local items = redis.call('ZRANGEBYSCORE', KEYS[1], 0, ARGV[1])
    if #items > 0 then
        redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, ARGV[1])
    end
    return items
"""


def calc_backoff(attempt: int) -> int:
    return BACKOFF_BASE ** attempt


def sign_payload(secret: str, payload: dict) -> str:
    body = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    sig = hmac.new(
        secret.encode(),
        body.encode(),
        hashlib.sha256
    ).hexdigest()
    return f"sha256={sig}"


async def deliver(delivery_id: str):
    async with AsyncSessionLocal() as db:

        result = await db.execute(text("""
            SELECT wd.id, wd.attempt_count, wd.status,
                   s.id AS subscription_id,
                   s.endpoint_url, s.secret, s.is_active,
                   e.payload, e.event_type
            FROM webhook_deliveries wd
            JOIN events e ON e.id = wd.event_id
            LEFT JOIN subscriptions s ON s.id = wd.subscription_id
            WHERE wd.id = :delivery_id
        """), {"delivery_id": delivery_id})

        row = result.fetchone()

        if not row:
            print(f"[worker] {delivery_id} event or delivery not found — skipping")
            return
        if row.subscription_id is None:
            print(f"[worker] {delivery_id} subscription deleted — marking ORPHANED")
            await db.execute(text("""
                UPDATE webhook_deliveries
                SET status = 'ORPHANED',
                    error_message = 'Subscription deleted',
                    updated_at = NOW()
                WHERE id = :id
            """), {"id": delivery_id})
            await db.commit()
            return
        if row.status == "DELIVERED":
            print(f"[worker] already delivered — skipping")
            return
        if not row.is_active:
            print(f"[worker] subscription {row.subscription_id} inactive — skipping {delivery_id}")
            await db.execute(text("""
                UPDATE webhook_deliveries
                SET status = 'FAILED',
                    error_message = 'Subscription disabled',
                    updated_at = NOW()
                WHERE id = :id
            """), {"id": delivery_id})
            await db.commit()
            return

        # --- CIRCUIT BREAKER CHECK ---
        state = await get_circuit_state(db, str(row.subscription_id))

        if state == "OPEN":
            transitioned = await try_transition_to_half_open(db, str(row.subscription_id))
            if transitioned:
                state = "HALF_OPEN"
                print(f"[worker] circuit OPEN→HALF_OPEN — testing {delivery_id}")
            else:
                print(f"[worker] circuit OPEN — delaying {delivery_id}")

            retry_at = time.time() + 60

            try:
                await redis.zadd(
                    "webhook_retry_queue",
                    {delivery_id: retry_at}
                )
            except Exception as e:
                print(f"[worker] Redis zadd failed for circuit OPEN: {e}")

            await db.execute(text("""
                UPDATE webhook_deliveries
                SET status = 'PENDING',
                    error_message = 'Circuit breaker OPEN - waiting',
                    next_attempt_at = NOW() + INTERVAL '60 seconds',
                    updated_at = NOW()
                WHERE id = :id
            """), {"id": delivery_id})

            await db.commit()
            return

        if state == "HALF_OPEN":
            print(f"[worker] circuit HALF_OPEN — testing {delivery_id}")

        result = await db.execute(text("""
            UPDATE webhook_deliveries
            SET status = 'IN_FLIGHT',
                attempt_count = attempt_count + 1,
                updated_at = NOW()
            WHERE id = :id AND status = 'PENDING'
        """), {"id": delivery_id})
        await db.commit()

        if result.rowcount == 0:
            print(f"[worker] {delivery_id} already claimed by another worker — skipping")
            return

        try:
            async with aiohttp.ClientSession() as session:
                payload = {"event_type": row.event_type, "payload": row.payload}
                signature = sign_payload(row.secret, payload)
                async with session.post(
                    row.endpoint_url,
                    json=payload,
                    headers={
                        "Content-Type": "application/json",
                        "X-Webhook-Delivery": delivery_id,
                        "X-Webhook-Attempt": str(row.attempt_count + 1),
                        "X-Webhook-Signature": signature
                    },
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as response:
                    if response.status < 300:
                        await db.execute(text("""
                            UPDATE webhook_deliveries
                            SET status = 'DELIVERED',
                                status_code = :code,
                                delivered_at = NOW(),
                                updated_at = NOW()
                            WHERE id = :id
                        """), {"id": delivery_id, "code": response.status})
                        await db.commit()
                        print(f"[worker] ✓ delivered {delivery_id} → {response.status}")

                        await record_success(db, str(row.subscription_id))
                    else:
                        await handle_failure(
                            db, delivery_id,
                            row.attempt_count + 1,
                            status_code=response.status
                        )

        except Exception as e:
            print(f"[worker] ✗ exception: {e}")
            await record_failure(db, str(row.subscription_id))
            await handle_failure(
                db, delivery_id,
                row.attempt_count + 1,
                error_message=str(e)
            )


async def handle_failure(
    db, delivery_id: str, attempt_count: int,
    status_code: int = None, error_message: str = None
):
    if attempt_count >= MAX_ATTEMPTS:
        await db.execute(text("""
            UPDATE webhook_deliveries
            SET status = 'DEAD',
                status_code = :code,
                error_message = :error,
                updated_at = NOW()
            WHERE id = :id
        """), {"id": delivery_id, "code": status_code, "error": error_message})
        await db.commit()
        print(f"[worker] ✗ max attempts reached — marking DEAD")
        return

    delay = calc_backoff(attempt_count)
    print(f"[worker] ✗ failed — retry in {delay}s (attempt {attempt_count})")

    retry_at = time.time() + delay

    try:
        await redis.zadd("webhook_retry_queue", {delivery_id: retry_at})
    except Exception as e:
        print(f"[worker] Redis zadd failed for retry: {e}")

    await db.execute(text("""
        UPDATE webhook_deliveries
        SET status = 'PENDING',
            status_code = :code,
            error_message = :error,
            next_attempt_at = NOW() + :delay * INTERVAL '1 second',
            updated_at = NOW()
        WHERE id = :id
    """), {"id": delivery_id, "code": status_code, "error": error_message, "delay": delay})
    await db.commit()


async def retry_scheduler():
    print("[scheduler] running...")
    dequeue = redis.register_script(RETRY_DEQUEUE_SCRIPT)
    while True:
        try:
            now = time.time()
            ready = await dequeue(keys=["webhook_retry_queue"], args=[now])

            for delivery_id in ready:
                if isinstance(delivery_id, bytes):
                    delivery_id = delivery_id.decode()
                await redis.lpush("webhook_queue", delivery_id)
                print(f"[scheduler] re-queued {delivery_id}")
        except Exception as e:
            print(f"[scheduler] Redis error: {e}")

        await asyncio.sleep(5)


async def main_loop():
    print("[worker] waiting for jobs...")
    while True:
        try:
            job = await redis.brpop(["webhook_queue"], timeout=5)
            if job:
                _, delivery_id = job
                if isinstance(delivery_id, bytes):
                    delivery_id = delivery_id.decode()
                print(f"[worker] picked up {delivery_id}")
                await deliver(delivery_id)
        except Exception as e:
            print(f"[worker] BRPOP error: {e}")
            await asyncio.sleep(5)


async def main():
    await asyncio.gather(main_loop(), retry_scheduler())


asyncio.run(main())
