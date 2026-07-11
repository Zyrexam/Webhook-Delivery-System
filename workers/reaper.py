import asyncio
from sqlalchemy import text
from app.database import AsyncSessionLocal
from app.redis_client import redis

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

            # Reset back to PENDING (only if still IN_FLIGHT)
            result = await db.execute(text("""
                UPDATE webhook_deliveries
                SET status = 'PENDING',
                    next_attempt_at = NOW()
                WHERE id = :id AND status = 'IN_FLIGHT'
            """), {"id": str(row.id)})

            if result.rowcount == 0:
                print(f"[reaper] {row.id} no longer IN_FLIGHT — skipping")
                continue

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
