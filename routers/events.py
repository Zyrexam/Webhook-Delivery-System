import uuid
import json
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from database import get_db
from redis_client import redis
from models import EventCreate

router = APIRouter()

@router.post("/events", status_code=202)
async def ingest_event(event: EventCreate, db: AsyncSession = Depends(get_db)):

    event_id = str(uuid.uuid4())
    await db.execute(text("""
        INSERT INTO events (id, event_type, payload)
        VALUES (:id, :event_type, CAST(:payload AS jsonb))
    """), {
        "id": event_id,
        "event_type": event.event_type,
        "payload": json.dumps(event.payload)
    })

    # Only active subscriptions
    result = await db.execute(text("""
        SELECT id FROM subscriptions
        WHERE event_type = :event_type
        AND is_active = TRUE
    """), {"event_type": event.event_type})

    subscriptions = result.fetchall()

    delivery_ids = []
    for sub in subscriptions:
        delivery_id = str(uuid.uuid4())
        delivery_ids.append(delivery_id)
        await db.execute(text("""
            INSERT INTO webhook_deliveries
                (id, event_id, subscription_id, status)
            VALUES (:id, :event_id, :sub_id, 'PENDING')
        """), {"id": delivery_id, "event_id": event_id, "sub_id": str(sub[0])})

    await db.commit()

    for delivery_id in delivery_ids:
        await redis.lpush("webhook_queue", delivery_id)

    return {"accepted": True, "event_id": event_id}