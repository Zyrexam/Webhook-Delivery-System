from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
from ..database import get_db

router = APIRouter(prefix="/deliveries", tags=["deliveries"])


class DeliveryResponse(BaseModel):
    id: str
    event_id: str
    subscription_id: str
    status: str
    attempt_count: int
    status_code: Optional[int]
    error_message: Optional[str]
    delivered_at: Optional[datetime]
    created_at: datetime


@router.get("", response_model=List[DeliveryResponse])
async def list_deliveries(
    event_id: Optional[str] = None,
    subscription_id: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db)
):
    query = """
        SELECT id, event_id, subscription_id, status,
               attempt_count, status_code, error_message,
               delivered_at, created_at
        FROM webhook_deliveries WHERE 1=1
    """
    params = {}

    if event_id:
        query += " AND event_id = :event_id"
        params["event_id"] = event_id
    if subscription_id:
        query += " AND subscription_id = :subscription_id"
        params["subscription_id"] = subscription_id
    if status:
        query += " AND status = :status"
        params["status"] = status

    query += " ORDER BY created_at DESC LIMIT :limit OFFSET :offset"
    params["limit"] = limit
    params["offset"] = offset

    result = await db.execute(text(query), params)
    rows = result.fetchall()

    return [
        DeliveryResponse(
            id=str(row[0]), event_id=str(row[1]),
            subscription_id=str(row[2]), status=row[3],
            attempt_count=row[4], status_code=row[5],
            error_message=row[6], delivered_at=row[7],
            created_at=row[8]
        )
        for row in rows
    ]


@router.get("/stats")
async def delivery_stats(db: AsyncSession = Depends(get_db)):
    result = await db.execute(text("""
        SELECT status, COUNT(*) as count, AVG(attempt_count) as avg_attempts
        FROM webhook_deliveries
        GROUP BY status
    """))
    stats = [
        {"status": row[0], "count": row[1], "avg_attempts": round(float(row[2] or 0), 2)}
        for row in result.fetchall()
    ]

    recent = await db.execute(text("""
        SELECT COUNT(CASE WHEN status = 'DELIVERED' THEN 1 END) * 100.0 / COUNT(*)
        FROM (
            SELECT status FROM webhook_deliveries
            ORDER BY created_at DESC LIMIT 100
        ) recent
    """))
    recent_row = recent.fetchone()
    success_rate = recent_row[0] if recent_row and recent_row[0] is not None else 0

    return {
        "status_breakdown": stats,
        "recent_success_rate": round(float(success_rate), 2),
        "total_deliveries": sum(s["count"] for s in stats)
    }
