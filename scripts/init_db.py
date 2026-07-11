import asyncio
import os
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://webhook_user:webhook_pass@localhost/webhooks")


async def init():
    engine = create_async_engine(DATABASE_URL, echo=False)
    async with engine.begin() as conn:

        # Events table
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS events (
                id          UUID PRIMARY KEY,
                event_type  TEXT NOT NULL,
                payload     JSONB NOT NULL,
                created_at  TIMESTAMPTZ DEFAULT NOW()
            )
        """))

        # Subscriptions table — with HMAC secret + circuit breaker columns
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS subscriptions (
                id                UUID PRIMARY KEY,
                endpoint_url      TEXT NOT NULL,
                event_type        TEXT NOT NULL,
                is_active         BOOLEAN DEFAULT TRUE,
                secret            TEXT,
                circuit_state     TEXT DEFAULT 'CLOSED',
                failure_count     INT DEFAULT 0,
                circuit_opened_at TIMESTAMPTZ,
                created_at        TIMESTAMPTZ DEFAULT NOW()
            )
        """))

        # Webhook deliveries table
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS webhook_deliveries (
                id               UUID PRIMARY KEY,
                event_id         UUID REFERENCES events(id),
                subscription_id  UUID REFERENCES subscriptions(id),
                status           TEXT DEFAULT 'PENDING',
                attempt_count    INT DEFAULT 0,
                status_code      INT,
                error_message    TEXT,
                next_attempt_at  TIMESTAMPTZ DEFAULT NOW(),
                delivered_at     TIMESTAMPTZ,
                updated_at       TIMESTAMPTZ DEFAULT NOW(),
                created_at       TIMESTAMPTZ DEFAULT NOW()
            )
        """))

        print("✓ Tables created")


asyncio.run(init())
