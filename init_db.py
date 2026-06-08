import asyncio
import os
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://webhook_user:webhook_pass@localhost/webhooks")


async def init():
    engine = create_async_engine(DATABASE_URL, echo=True)
    async with engine.begin() as conn:
        # Events table - stores incoming events
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS events (
                id          UUID PRIMARY KEY,
                event_type  TEXT NOT NULL,
                payload     JSONB NOT NULL,
                created_at  TIMESTAMPTZ DEFAULT NOW()
            )
        """))

        # Subscriptions table - where customers register their webhooks
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS subscriptions (
                id           UUID PRIMARY KEY,
                endpoint_url TEXT NOT NULL,
                event_type   TEXT NOT NULL,
                created_at   TIMESTAMPTZ DEFAULT NOW()
            )
        """))

        # Webhook deliveries table - tracks each delivery attempt
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS webhook_deliveries (
                id               UUID PRIMARY KEY,
                event_id         UUID REFERENCES events(id),
                subscription_id  UUID REFERENCES subscriptions(id),
                status           TEXT DEFAULT 'PENDING',
                attempt_count    INT DEFAULT 0,
                next_attempt_at  TIMESTAMPTZ DEFAULT NOW(),
                delivered_at     TIMESTAMPTZ,
                created_at       TIMESTAMPTZ DEFAULT NOW()
            )
        """))

        print("Tables created successfully")

        # Insert a test subscription so we have something to deliver to
        await conn.execute(text("""
            INSERT INTO subscriptions (id, endpoint_url, event_type)
            VALUES
                ('11111111-1111-1111-1111-111111111111', 'https://webhook.site/your-test-id', 'order.placed'),
                ('22222222-2222-2222-2222-222222222222', 'https://webhook.site/your-test-id', 'order.placed')
            ON CONFLICT (id) DO NOTHING
        """))
        print("Added test subscriptions")


asyncio.run(init())
