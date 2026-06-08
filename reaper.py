import asyncio
from sqlalchemy import text
from database import AsyncSessionLocal
from redis_client import redis

# If a job has been IN_FLIGHT for more than this many seconds
# without completing — assume the worker died
STUCK_THRESHOLD_SECONDS = 60


async def reap():
    async with AsyncSessionLocal() as db:

        # Find all deliveries stuck IN_FLIGHT
        result = await db.execute(text("""
            SELECT id, attempt_count
            FROM webhook_deliveries
            WHERE status = 'IN_FLIGHT'
            AND updated_at < NOW() - INTERVAL ':threshold seconds'
        """.replace(':threshold', str(STUCK_THRESHOLD_SECONDS))))

        stuck = result.fetchall()

        if not stuck:
            print("[reaper] nothing stuck")
            return

        print(f"[reaper] found {len(stuck)} stuck job(s)")

        for row in stuck:
            print(f"[reaper] rescuing {row.id} (attempt {row.attempt_count})")

            # Reset back to PENDING
            await db.execute(text("""
                UPDATE webhook_deliveries
                SET status = 'PENDING',
                    next_attempt_at = NOW()
                WHERE id = :id
            """), {"id": str(row.id)})

            # Push back into Redis queue
            await redis.lpush("webhook_queue", str(row.id))

        await db.commit()
        print(f"[reaper] rescued {len(stuck)} job(s)")


async def main():
    print("[reaper] starting — checks every 30 seconds")
    while True:
        await reap()
        await asyncio.sleep(30)


asyncio.run(main())




