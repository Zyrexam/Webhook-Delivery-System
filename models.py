from pydantic import BaseModel
from typing import Any

class EventCreate(BaseModel):
    event_type: str      # "payment.succeeded"
    payload: dict[str, Any]  # whatever data the sender includes