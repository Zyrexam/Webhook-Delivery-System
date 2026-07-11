from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timezone

FAILURE_THRESHOLD = 5
COOLDOWN_SECONDS = 60


async def get_circuit_state(db: AsyncSession, subscription_id: str) -> str:
    """Returns: CLOSED, OPEN, or HALF_OPEN — no side effects"""
    result = await db.execute(text("""
        SELECT circuit_state
        FROM subscriptions
        WHERE id = :id
    """), {"id": subscription_id})

    row = result.fetchone()
    if not row:
        return "CLOSED"

    return row[0]


async def try_transition_to_half_open(db: AsyncSession, subscription_id: str) -> bool:
    """If circuit is OPEN and cooldown passed, transition to HALF_OPEN. Returns True if transitioned."""
    result = await db.execute(text("""
        SELECT circuit_state, circuit_opened_at
        FROM subscriptions
        WHERE id = :id
    """), {"id": subscription_id})

    row = result.fetchone()
    if not row:
        return False

    state, opened_at = row
    if state != "OPEN" or not opened_at:
        return False

    now = datetime.now(timezone.utc)
    seconds_open = (now - opened_at).total_seconds()
    if seconds_open < COOLDOWN_SECONDS:
        return False

    await db.execute(text("""
        UPDATE subscriptions
        SET circuit_state = 'HALF_OPEN'
        WHERE id = :id AND circuit_state = 'OPEN'
    """), {"id": subscription_id})
    await db.commit()
    print(f"[circuit] OPEN→HALF_OPEN — {subscription_id}")
    return True


async def record_success(db: AsyncSession, subscription_id: str):
    """Reset circuit on success"""
    result = await db.execute(text("""
        UPDATE subscriptions
        SET circuit_state   = 'CLOSED',
            failure_count   = 0,
            circuit_opened_at = NULL
        WHERE id = :id
    """), {"id": subscription_id})
    await db.commit()
    if result.rowcount > 0:
        print(f"[circuit] ✓ closed — {subscription_id}")
    else:
        print(f"[circuit] subscription {subscription_id} not found — skipping success")


async def record_failure(db: AsyncSession, subscription_id: str):
    """Increment failure count, open circuit if threshold hit"""
    result = await db.execute(text("""
        UPDATE subscriptions
        SET failure_count = failure_count + 1
        WHERE id = :id
        RETURNING failure_count
    """), {"id": subscription_id})

    row = result.fetchone()
    if row is None:
        print(f"[circuit] subscription {subscription_id} not found — skipping failure")
        await db.commit()
        return

    new_count = row[0]
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
