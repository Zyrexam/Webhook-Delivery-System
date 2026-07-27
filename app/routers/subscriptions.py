import uuid
import secrets
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from pydantic import BaseModel, HttpUrl
from typing import List, Optional
from datetime import datetime
from ..database import get_db

router = APIRouter(prefix="/subscriptions", tags=["subscriptions"])


class SubscriptionCreate(BaseModel):
    endpoint_url: HttpUrl
    event_type: str


class SubscriptionResponse(BaseModel):
    id: str
    endpoint_url: str
    event_type: str
    created_at: datetime
    is_active: bool
    secret: Optional[str] = None
    circuit_state: Optional[str] = None
    failure_count: Optional[int] = None


class SubscriptionUpdate(BaseModel):
    is_active: bool


@router.post("", response_model=SubscriptionResponse, status_code=201)
async def create_subscription(
    body: SubscriptionCreate,
    db: AsyncSession = Depends(get_db)
):
    sub_id = str(uuid.uuid4())
    secret = secrets.token_hex(32)
    await db.execute(text("""
        INSERT INTO subscriptions (id, endpoint_url, event_type, is_active, secret)
        VALUES (:id, :url, :event_type, TRUE, :secret)
    """), {"id": sub_id, "url": str(body.endpoint_url), "event_type": body.event_type, "secret": secret})
    await db.commit()

    return SubscriptionResponse(
        id=sub_id,
        endpoint_url=str(body.endpoint_url),
        event_type=body.event_type,
        created_at=datetime.utcnow(),
        is_active=True,
        secret=secret
    )


@router.get("", response_model=List[SubscriptionResponse])
async def list_subscriptions(
    event_type: Optional[str] = None,
    db: AsyncSession = Depends(get_db)
):

    query = "SELECT id, endpoint_url, event_type, created_at, is_active, circuit_state, failure_count FROM subscriptions WHERE 1=1"
    params = {}

    if event_type:
        query += " AND event_type = :event_type"
        params["event_type"] = event_type

    query += " ORDER BY created_at DESC"
    result = await db.execute(text(query), params)
    rows = result.fetchall()

    return [
        SubscriptionResponse(
            id=str(row[0]),
            endpoint_url=row[1],
            event_type=row[2],
            created_at=row[3],
            is_active=row[4],
            circuit_state=row[5],
            failure_count=row[6]
        )
        for row in rows
    ]


@router.get("/{sub_id}", response_model=SubscriptionResponse)
async def get_subscription(sub_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(text("""
        SELECT id, endpoint_url, event_type, created_at, is_active, circuit_state, failure_count
        FROM subscriptions WHERE id = :id
    """), {"id": sub_id})
    row = result.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Subscription not found")

    return SubscriptionResponse(
        id=str(row[0]), endpoint_url=row[1],
        event_type=row[2], created_at=row[3], is_active=row[4]
    )


@router.patch("/{sub_id}")
async def update_subscription(
    sub_id: str,
    body: SubscriptionUpdate,
    db: AsyncSession = Depends(get_db)
):
    result = await db.execute(text("""
        UPDATE subscriptions SET is_active = :is_active
        WHERE id = :id RETURNING id
    """), {"id": sub_id, "is_active": body.is_active})

    if not result.fetchone():
        raise HTTPException(status_code=404, detail="Subscription not found")

    await db.commit()
    return {"status": "updated", "is_active": body.is_active}


@router.delete("/{sub_id}")
async def delete_subscription(sub_id: str, db: AsyncSession = Depends(get_db)):
    try:
        result = await db.execute(text("""
            DELETE FROM subscriptions WHERE id = :id RETURNING id
        """), {"id": sub_id})
    except IntegrityError:
        await db.rollback()
        raise HTTPException(
            status_code=409,
            detail="Cannot delete subscription with existing deliveries. Disable it instead."
        )

    if not result.fetchone():
        raise HTTPException(status_code=404, detail="Subscription not found")

    await db.commit()
    return {"status": "deleted"}
