from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timezone

FAILURE_THRESHOLD = 5
COOLDOWN_SECONDS  = 60

async def get_circuit_state(db: AsyncSession, subscription_id: str) -> str:
    """Returns: CLOSED, OPEN, or HALF_OPEN"""
    result = await db.execute(text("""
        SELECT circuit_state, failure_count, circuit_opened_at
        FROM subscriptions
        WHERE id = :id
    """), {"id": subscription_id})

    row = result.fetchone()
    if not row:
        return "CLOSED"

    state, failure_count, opened_at = row

    if state == "OPEN" and opened_at:
        # Check if cooldown has passed → move to HALF_OPEN
        now = datetime.now(timezone.utc)
        seconds_open = (now - opened_at).total_seconds()

        if seconds_open >= COOLDOWN_SECONDS:
            await db.execute(text("""
                UPDATE subscriptions
                SET circuit_state = 'HALF_OPEN'
                WHERE id = :id
            """), {"id": subscription_id})
            await db.commit()
            return "HALF_OPEN"

    return state

async def record_success(db: AsyncSession, subscription_id: str):
    """Reset circuit on success"""
    await db.execute(text("""
        UPDATE subscriptions
        SET circuit_state   = 'CLOSED',
            failure_count   = 0,
            circuit_opened_at = NULL
        WHERE id = :id
    """), {"id": subscription_id})
    await db.commit()
    print(f"[circuit] ✓ closed — {subscription_id}")


async def record_failure(db: AsyncSession, subscription_id: str):
    """Increment failure count, open circuit if threshold hit"""
    result = await db.execute(text("""
        UPDATE subscriptions
        SET failure_count = failure_count + 1
        WHERE id = :id
        RETURNING failure_count
    """), {"id": subscription_id})

    new_count = result.fetchone()[0]
    await db.commit()

    if new_count >= FAILURE_THRESHOLD:
        await db.execute(text("""
            UPDATE subscriptions
            SET circuit_state     = 'OPEN',
                circuit_opened_at = NOW()
            WHERE id = :id
        """), {"id": subscription_id})
        await db.commit()
        print(f"[circuit] ✗ OPEN after {new_count} failures — {subscription_id}")
    else:
        print(f"[circuit] failure {new_count}/{FAILURE_THRESHOLD} — {subscription_id}")