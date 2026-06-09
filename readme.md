# Webhook Delivery System

A production-inspired webhook delivery system built with FastAPI, Redis, and PostgreSQL.
Guarantees reliable event delivery with automatic retries, exponential backoff, crash recovery, HMAC signature verification, and circuit breaking.

---

## Architecture

![Architecture](webhook_arch_updated.svg)
---

## How It Works

**1. Event Ingestion**
Someone sends a `POST /events`. FastAPI saves the event to Postgres, creates one `webhook_deliveries` row per active subscriber, pushes each delivery ID into Redis, and returns `202 Accepted` immediately — without waiting for delivery.

**2. Delivery Worker**
A background worker does `BRPOP` on the Redis queue. For each job it:
- Checks the circuit breaker state for that subscriber
- Marks the delivery `IN_FLIGHT` in Postgres
- Signs the payload with HMAC-SHA256
- Makes an HTTP POST to the subscriber's endpoint
- On success → marks `DELIVERED`, resets circuit breaker
- On failure → schedules a retry with exponential backoff, records failure

**3. Exponential Backoff**
Failed deliveries go into a Redis sorted set with a future timestamp as score.
A scheduler checks every 5 seconds and moves ready jobs back to the main queue.
Delay doubles each attempt: `2s → 4s → 8s → 16s → 32s`.
After 5 attempts the delivery is marked `DEAD`.

**4. Crash Recovery (Reaper)**
If the worker crashes while a job is `IN_FLIGHT`, that row stays stuck forever.
The reaper runs every 30 seconds, finds any `IN_FLIGHT` rows where `updated_at` is older than 60 seconds, resets them to `PENDING`, and re-queues them.

**5. Idempotency**
Before delivering, the worker checks if status is already `DELIVERED` and skips.
This prevents double delivery if the same job gets picked up twice.

**6. HMAC Signature Verification**
Every webhook payload is signed using `HMAC-SHA256` with a per-subscription secret key.
The signature is sent in the `X-Webhook-Signature` header.
Subscribers verify the signature before trusting the payload — preventing spoofed requests.

```
X-Webhook-Signature: sha256=abc123def456...
X-Webhook-Delivery:  <delivery-uuid>
X-Webhook-Attempt:   1
```

**7. Circuit Breaker**
Prevents repeatedly hitting unhealthy webhook endpoints.

```
CLOSED
  │
  │  Deliver normally
  │
  └─ 5 failures ──▶ OPEN
                    │
                    │  Stop requests
                    │  Wait 60 seconds
                    │
                    ▼
                 HALF_OPEN
                    │
             Test one request
                    │
          ┌─────────┴─────────┐
          ▼                   ▼
       Success              Failure
          │                   │
          ▼                   ▼
       CLOSED               OPEN
```


## Database Schema

```
events
  id            UUID  PK
  event_type    TEXT             e.g. "order.placed"
  payload       JSONB
  created_at    TIMESTAMPTZ

subscriptions
  id                UUID  PK
  endpoint_url      TEXT
  event_type        TEXT
  is_active         BOOLEAN
  secret            TEXT         HMAC signing key
  circuit_state     TEXT         CLOSED | OPEN | HALF_OPEN
  failure_count     INT
  circuit_opened_at TIMESTAMPTZ
  created_at        TIMESTAMPTZ

webhook_deliveries
  id               UUID  PK
  event_id         UUID  FK → events
  subscription_id  UUID  FK → subscriptions
  status           TEXT  PENDING | IN_FLIGHT | DELIVERED | FAILED | DEAD
  attempt_count    INT
  status_code      INT
  error_message    TEXT
  next_attempt_at  TIMESTAMPTZ
  delivered_at     TIMESTAMPTZ
  updated_at       TIMESTAMPTZ
  created_at       TIMESTAMPTZ
```

---

## Setup

**Requirements:** Python 3.11+, Docker

```bash
# 1. Start Postgres and Redis
docker compose up -d

# 2. Install dependencies
pip install -r requirements.txt

# 3. Create tables
python init_db.py

# 4. Start all processes (4 terminals)
uvicorn main:app --reload       # terminal 1 — API
python worker.py                # terminal 2 — delivery worker + retry scheduler
python reaper.py                # terminal 3 — crash recovery
python receiver.py              # terminal 4 — test subscriber
```

---

## API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/events` | Emit an event |
| POST | `/subscriptions` | Register a webhook endpoint |
| GET | `/subscriptions` | List all subscriptions |
| GET | `/subscriptions/{id}` | Get a subscription |
| PATCH | `/subscriptions/{id}` | Enable / disable |
| DELETE | `/subscriptions/{id}` | Remove subscription |
| GET | `/deliveries` | Delivery history (filterable) |
| GET | `/deliveries/stats` | Success rates and attempt counts |

---

## Example Usage

```bash
# Register a subscriber — returns secret key for HMAC verification
curl -X POST http://localhost:8000/subscriptions \
  -H "Content-Type: application/json" \
  -d '{"endpoint_url": "http://localhost:9000/webhook", "event_type": "order.placed"}'

# Send an event
curl -X POST http://localhost:8000/events \
  -H "Content-Type: application/json" \
  -d '{"event_type": "order.placed", "payload": {"order_id": "ORD123", "amount": 99.99}}'

# Check delivery status
curl http://localhost:8000/deliveries

# Filter by event
curl http://localhost:8000/deliveries?event_id=<event_id>

# View stats
curl http://localhost:8000/deliveries/stats

# Disable a subscription
curl -X PATCH http://localhost:8000/subscriptions/<id> \
  -H "Content-Type: application/json" \
  -d '{"is_active": false}'
```

---

## HMAC Signature — How It Works

### Step 1 — Register a subscription, get your secret

When you call `POST /subscriptions`, the response includes a `secret` key.
**This is shown only once — save it immediately.**

```json
{
  "id": "427781f1-20a0-4bd3-a1d5-76545ae7b010",
  "endpoint_url": "http://localhost:9000/webhook",
  "event_type": "order.placed",
  "is_active": true,
  "secret": "a3f9c2e1b7d6e4f8a1c3d5e7f9b2a4c6d8e0f2a4b6c8d0e2f4a6b8c0d2e4f6a8"
}
```

### Step 2 — How the system signs the payload

For every delivery, the worker generates a signature like this:

```python
import hmac, hashlib, json

def sign_payload(secret: str, payload: dict) -> str:
    # Serialize payload deterministically
    body = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    
    # HMAC-SHA256 sign it
    sig = hmac.new(secret.encode(), body.encode(), hashlib.sha256).hexdigest()
    
    return f"sha256={sig}"
```

The signature goes into the request header:
```
X-Webhook-Signature: sha256=abc123def456...
X-Webhook-Delivery:  87b74510-56a7-4b21-bda6-076f2b144224
X-Webhook-Attempt:   1
```

### Step 3 — How your receiver verifies it

Paste your secret into the receiver and verify every incoming request:

```python
import hmac, hashlib, json
from fastapi import Request, HTTPException

WEBHOOK_SECRET = "paste_your_secret_here"  # from Step 1

def verify_signature(secret: str, payload: dict, signature: str) -> bool:
    body = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    expected = "sha256=" + hmac.new(
        secret.encode(),
        body.encode(),
        hashlib.sha256
    ).hexdigest()
    # compare_digest prevents timing attacks
    return hmac.compare_digest(expected, signature)

@app.post("/webhook")
async def receive_webhook(request: Request):
    body = await request.json()
    sig  = request.headers.get("x-webhook-signature", "")

    if not verify_signature(WEBHOOK_SECRET, body, sig):
        raise HTTPException(status_code=401, detail="Invalid signature")

    # safe to process — payload is genuine
    print(f"✅ Verified! Event: {body.get('event_type')}")
    return {"status": "ok"}
```

### Why `hmac.compare_digest` and not `==`?

A normal `==` comparison stops at the first mismatched character.
An attacker can measure response time to guess the signature byte by byte — a **timing attack**.
`compare_digest` always takes the same time regardless of where the mismatch is.

### Full flow in one picture


```text
Sender                  Webhook System                Subscriber
  │                           │                             │
  │  POST /events             │                             │
  ├──────────────────────────▶│                             │
  │                           │                             │
  │                           │ Create HMAC-SHA256          │
  │                           │ using subscriber secret     │
  │                           │                             │
  │                           │ POST webhook                │
  │                           │ + X-Webhook-Signature       │
  │                           ├────────────────────────────▶│
  │                           │                             │
  │                           │                             │ Verify signature
  │                           │                             │ Reject if invalid
  │                           │                             │
  │                           │          200 OK             │
  │                           │◀────────────────────────────┤
  │                           │                             │
```
---
## Key Design Decisions

**Why Redis sorted set for retries?**
Delayed jobs are stored with their retry timestamp as the score. A scheduler polls every 5 seconds and moves ready jobs to the live queue. No cron jobs, no polling Postgres.

**Why Postgres as source of truth?**
We always commit to Postgres before pushing to Redis. If Redis goes down, the reaper rebuilds the queue from `PENDING` rows. Nothing is lost.

**Why at-least-once and not exactly-once?**
Exactly-once delivery across a network requires distributed transactions — too expensive. We guarantee at-least-once and use idempotency checks to skip already-delivered jobs safely.

**Why a circuit breaker?**
Without it, a permanently dead subscriber receives retry attempts on every single event indefinitely. The circuit breaker stops hammering dead endpoints, preserves worker capacity for healthy subscribers, and automatically recovers when the endpoint comes back.

**Why HMAC-SHA256 signatures?**
Any party who knows a subscriber's URL could send fake webhooks. Signing with a shared secret lets subscribers verify the payload came from this system — not a spoofed source. `hmac.compare_digest` prevents timing attacks.

---

## Tech Stack

- **FastAPI** — async Python web framework
- **PostgreSQL** — persistent state, delivery tracking
- **Redis** — job queue (LIST) + delay queue (sorted set)
- **SQLAlchemy (async)** — database ORM
- **aiohttp** — async HTTP client for webhook delivery
- **Docker Compose** — local infrastructure