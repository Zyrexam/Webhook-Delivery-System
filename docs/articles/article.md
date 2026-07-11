# Building a Reliable Webhook Delivery System: The 8 Decisions That Mattered

> *"Your payment is confirmed! Here's your receipt."* — Three seconds later, the customer is screaming in DMs because they got charged twice.

That's the moment you realize: **sending webhooks is easy. Delivering them reliably is brutally hard.**

When I set out to build a webhook delivery system, I thought it'd be straightforward — accept an event, make an HTTP call, done. But the real world has other plans. Networks blink. Workers crash. Subscribers go down at 3 AM. And somehow, every single event needs to arrive exactly when it should, not a moment sooner, not a moment later.

After weeks of building, breaking, and rebuilding, here are the **8 engineering decisions** that shaped our system. Each one tells the same story: **We hit a wall. We looked at our options. We chose the one that didn't break at 2 AM.**

---

## The Setup: What We're Building

A webhook delivery system is the postal service of the internet. Someone sends an event (like `order.placed`), and we need to deliver it to every subscriber who's listening for that event — via HTTP POST to their endpoint.

```python
# What the caller sends
POST /events
{
  "event_type": "order.placed",
  "payload": { "order_id": "ORD-123", "amount": 99.99 }
}

# What we do with it
→ Find all subscribers for "order.placed"
→ Create a delivery record for each
→ Push each delivery to a queue
→ Worker picks it up and makes the HTTP call
→ Retry if it fails, give up if it never works
```

Simple, right? Let me show you where it gets complicated.

---

## Decision #1: Stop Making the Sender Wait

**The Problem:**

In the naive version, we deliver webhooks **inside the HTTP request**. The caller sends an event, and we synchronously call every subscriber before responding.

```python
@app.post("/events")
async def create_event(event):
    # Save event
    # Find subscribers
    # For each subscriber: make HTTP call... and wait
    # Only now return a response
```

This is a disaster. One slow subscriber holds up everyone. If the subscriber takes 30 seconds, the caller's connection times out. The caller retries. Now we have **duplicate events**. And if we're calling 50 subscribers? That's 50 sequential HTTP calls before the caller gets a response.

**The Options:**

1. **Synchronous delivery** — Simple code, terrible user experience. Caller blocks until every subscriber responds. One slow subscriber ruins it for everyone.

2. **FastAPI BackgroundTasks** — Runs delivery in a background thread. Better, but if the process crashes, the event is gone forever. No durability.

3. **Message queue** — Decouple ingestion from delivery. Save the event, push to a queue, respond immediately. A worker picks up the queue independently.

**What We Chose:**

**Redis LIST with BRPOP** — We save the event to Postgres first (source of truth), create delivery rows, push delivery IDs to a Redis list, and return `202 Accepted` instantly.

```python
@router.post("/events", status_code=202)
async def ingest_event(event, db):
    # 1. Save to Postgres first
    event_id = str(uuid.uuid4())
    await db.execute("INSERT INTO events ...")
    
    # 2. Create delivery rows for each subscriber
    delivery_ids = []
    for sub in active_subscriptions:
        delivery_id = str(uuid.uuid4())
        await db.execute("INSERT INTO webhook_deliveries ...")
        delivery_ids.append(delivery_id)
    
    await db.commit()
    
    # 3. Push to Redis queue (only after Postgres commit)
    for delivery_id in delivery_ids:
        await redis.lpush("webhook_queue", delivery_id)
    
    # 4. Return immediately
    return {"accepted": True, "event_id": event_id}
```

Meanwhile, the worker blocks on `BRPOP` waiting for jobs:

```python
while True:
    job = await redis.brpop(["webhook_queue"], timeout=5)
    if job:
        _, delivery_id = job
        await deliver(delivery_id)  # Make the actual HTTP call
```

**Why This Works:**

The sender gets `202 Accepted` immediately — no waiting. If the worker crashes, the delivery is safe in Postgres (as `PENDING`). When the worker comes back, it picks up where it left off. Nothing is lost.

---

## Decision #2: At-Least-Once, Not Exactly-Once

**The Problem:**

Here's a fun scenario: The worker sends the HTTP request. The subscriber receives it, processes it successfully, and sends back `200 OK`. But the `200 OK` response is **lost in transit** (network hiccup, proxy timeout, cosmic rays).

The worker thinks it failed. It retries. Now the subscriber gets the same webhook **twice**.

**The Options:**

1. **At-most-once** — Send once and forget. Simplest. But events go missing on network blips. When you're processing payments or password resets, "oops, we lost it" is not acceptable.

2. **Exactly-once** — The holy grail. Distributed transactions, two-phase commit, idempotency keys with guaranteed deduplication. Theoretically ideal, practically **expensive and slow**.

3. **At-least-once with idempotency** — Accept that retries happen. Make retries safe. Before processing any delivery, check if it was already completed.

**What We Chose:**

**At-least-once + idempotency check.** Before the worker touches a delivery, it checks the database:

```python
row = await db.execute("""
    SELECT status FROM webhook_deliveries WHERE id = :delivery_id
""")
if row.status == "DELIVERED":
    print("Already delivered — skipping")
    return  # Safe. Don't double-process.
```

Every webhook also includes a unique `X-Webhook-Delivery` header (a UUID). Subscribers can use this to deduplicate on their side too.

**Why Not Exactly-Once?**

Because exactly-once delivery across a network requires distributed transactions. In practice, **nobody actually does this**. Stripe doesn't. GitHub doesn't. SendGrid doesn't. They all use at-least-once + idempotency.

The math is simple: Exactly-once adds enormous complexity for marginal benefit. At-least-once with a database check solves 99% of the problem.

> **Engineering lesson:** Don't let perfect be the enemy of reliable.

---

## Decision #3: Wait Longer Each Time

**The Problem:**

The subscriber's server is down. Maybe they're deploying a new version. Maybe their database crashed. Maybe a Kubernetes pod is restarting. Whatever the reason, the first delivery failed.

If we retry immediately, it fails again. If we retry in a tight loop, we **DDoS** the subscriber. But if we wait too long, time-sensitive events miss their window.

**The Options:**

1. **Fixed delay** (retry every 30 seconds) — Predictable, but wasteful. If the server recovers in 5 seconds, we wait 30. If it needs 2 minutes, we hammer it with useless retries.

2. **Immediate retry, then give up** — Too aggressive. Doesn't give transient failures time to resolve.

3. **Exponential backoff** — Wait 2s, then 4s, then 8s, then 16s, then 32s. Gives the system time to recover while not dragging on forever.

**What We Chose:**

**Exponential backoff: 2^attempt seconds.**

```
Attempt 1 → wait 2s
Attempt 2 → wait 4s
Attempt 3 → wait 8s
Attempt 4 → wait 16s
Attempt 5 → wait 32s  → Then mark DEAD
```

```python
BACKOFF_BASE = 2
MAX_ATTEMPTS = 5

def calc_backoff(attempt: int) -> int:
    return BACKOFF_BASE ** attempt  # 2, 4, 8, 16, 32
```

The tricky part was **where to store delayed retries**. We couldn't put them back in the main Redis queue — they'd be picked up immediately. We needed a "waiting room."

**The Trick: Redis Sorted Set**

Failed deliveries go into a Redis Sorted Set with `score = current_time + delay`. A scheduler runs every 5 seconds and moves ready jobs back to the main queue:

```python
async def retry_scheduler():
    while True:
        now = asyncio.get_event_loop().time()
        ready = await redis.zrangebyscore("webhook_retry_queue", 0, now)
        
        for delivery_id in ready:
            await redis.zrem("webhook_retry_queue", delivery_id)
            await redis.lpush("webhook_queue", delivery_id)
        
        await asyncio.sleep(5)
```

This is Redis's `ZRANGEBYSCORE` — an O(log N) operation. No cron jobs. No polling Postgres. No external scheduler.

After 5 attempts, the delivery is marked `DEAD` and we stop trying. Closure.

---

## Decision #4: The Circuit Breaker

**The Problem:**

One subscriber goes down permanently. A server crash. A configuration error. A forgotten AWS bill. Whatever the reason, this subscriber is **never** going to respond.

Without protection, every new event triggers 5 retry attempts against this dead endpoint. If we process 100 events an hour, that's **500 failed HTTP calls** per hour — all wasted, all blocking worker capacity.

**The Options:**

1. **No protection** — Simple. Wastes resources. A single dead subscriber can consume all worker capacity.

2. **Manual suspension** — An admin notices the problem and PATCH-es the subscription to inactive. Requires human attention. Doesn't work at 3 AM.

3. **Circuit breaker pattern** — Automatically detect repeated failures, stop sending, and periodically test if the endpoint has recovered.

**What We Chose:**

**A three-state circuit breaker** stored directly in the `subscriptions` table:

```python
FAILURE_THRESHOLD = 5
COOLDOWN_SECONDS = 60
```

**CLOSED** (happy path): Every delivery goes through.

**OPEN** (after 5 consecutive failures): The worker instantly re-queues the delivery with a 60-second delay. No HTTP call is made.

**HALF_OPEN** (after 60 seconds): One delivery is allowed through as a test. If it succeeds → CLOSED. If it fails → back to OPEN.

```python
state = await get_circuit_state(db, subscription_id)

if state == "OPEN":
    # Don't even try. Re-queue for later.
    await redis.zadd("webhook_retry_queue", {delivery_id: retry_at})
    return

if state == "HALF_OPEN":
    # Allow one through as a probe
    pass  # Will succeed → CLOSED, fail → OPEN
```

The beauty of this approach: **No events are dropped.** Deliveries destined for an OPEN circuit are just delayed. When the circuit recovers, they flow through naturally.

---

## Decision #5: Every Worker Dies Eventually

**The Problem:**

The worker picks up a delivery, marks it `IN_FLIGHT` in Postgres, and starts making the HTTP call. Then: **crash.** Power outage. OOM killer. Accidental `kill -9`.

That delivery row is now stuck as `IN_FLIGHT` forever. It will never be retried, never marked DELIVERED. The event is **silently lost**.

**The Options:**

1. **Hope it doesn't crash** — I mean, you could. But it will crash. Everything crashes eventually.

2. **Store everything in Redis** — If Redis is persistent. But we chose Postgres as source of truth exactly for durability.

3. **A reaper process** — A separate watcher that scans for stuck deliveries and rescues them.

**What We Chose:**

**The Reaper** — A standalone process that runs independently.

```python
async def reap():
    stuck = await db.execute("""
        SELECT id FROM webhook_deliveries
        WHERE status = 'IN_FLIGHT'
        AND updated_at < NOW() - INTERVAL '60 seconds'
    """)
    
    for row in stuck:
        await db.execute("""
            UPDATE webhook_deliveries
            SET status = 'PENDING'
            WHERE id = :id
        """)
        await redis.lpush("webhook_queue", str(row.id))
    
    await db.commit()
```

Every 30 seconds, it finds deliveries stuck in `IN_FLIGHT` for more than 60 seconds, resets them to `PENDING`, and pushes them back into the queue.

**Why 60 seconds?** Our HTTP timeout is 10 seconds. 60 seconds is 6x the timeout — plenty of margin. If a delivery is genuinely in-flight (worker is alive, just slow), the worker updates `updated_at` on every tick, keeping the Reaper away.

**Why a separate process?** If the main worker crashes, the Reaper doesn't care — it's independent. This is the Erlang/OTP supervisor pattern: one process watches another.

---

## Decision #6: Trust No One

**The Problem:**

Anyone who knows a subscriber's endpoint URL can send fake webhooks. If I know your Slack webhook URL, I can post spam that looks like it came from our system. No authentication = no trust.

**The Options:**

1. **No verification** — Dead simple. Completely insecure.

2. **IP whitelisting** — Only allow requests from known IPs. Doesn't work in the cloud (dynamic IPs). IP spoofing exists.

3. **Bearer token in URL** — `https://subscriber.com/webhook?token=secret`. Tokens leak in server logs, referrer headers, and browser history. Awful idea.

4. **HMAC-SHA256 signature** — Sign the payload with a shared secret. Subscribers verify using the secret. Industry standard.

**What We Chose:**

**HMAC-SHA256 with per-subscription secrets.**

When a subscription is created, we generate a 64-character hex secret:

```python
secret = secrets.token_hex(32)  # 64 chars of randomness
```

Every webhook request includes three headers:

```
X-Webhook-Signature: sha256=a1b2c3d4e5f6...
X-Webhook-Delivery:  87b74510-56a7-4b21-bda6-076f2b144224
X-Webhook-Attempt:   1
```

The signature is computed over the deterministic JSON serialization of the payload:

```python
def sign_payload(secret: str, payload: dict) -> str:
    body = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    sig = hmac.new(
        secret.encode(),
        body.encode(),
        hashlib.sha256
    ).hexdigest()
    return f"sha256={sig}"
```

Subscribers verify using `hmac.compare_digest()` — **not `==`**:

```python
def verify_signature(secret, payload, signature):
    body = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    expected = "sha256=" + hmac.new(
        secret.encode(), body.encode(), hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, signature)
```

**Why `compare_digest` and not `==`?**

A normal `==` comparison stops at the first mismatched character. An attacker can measure the response time to guess the signature byte-by-byte — a **timing attack**. `compare_digest` always takes the same time regardless of where the mismatch is. This is defense-in-depth, and it matters.

---

## Decision #7: Two Databases Are Better Than One

**The Problem:**

We need two contradictory things:
- **Durability**: Never lose an event
- **Speed**: Process thousands of deliveries per second

A single database can't do both perfectly.

**The Options:**

1. **Postgres only** — ACID guarantees. But polling Postgres for jobs is slow. `LISTEN/NOTIFY` helps but is clunky at scale.

2. **Redis only** — Blazing fast. But if Redis crashes without persistence, we lose queued deliveries. Even with RDB/AOF persistence, you can lose seconds of data.

3. **Both** — Postgres for durability. Redis for speed. Each plays to its strength.

**What We Chose:**

**Postgres as source of truth. Redis as queue.**

The flow is strict:

```
Event arrives
  → 1. INSERT into Postgres events table ✅
  → 2. INSERT delivery rows into Postgres ✅
  → 3. PUSH delivery IDs into Redis list
  → 4. Return 202 Accepted
```

Postgres commit happens **before** Redis push. If Redis crashes between steps 3 and 4, the delivery is safely in Postgres as `PENDING`. The Reaper will find it and re-queue it when Redis comes back.

If Postgres crashes before step 1/2, the sender gets an error and can retry.

**Why this ordering matters:**

```
Postgres: I have the data. I will never forget it.
Redis: I move the data fast. If I forget, Postgres has the backup.
```

---

## Decision #8: Why 202? (Not 200, Not 201)

**The Problem:**

When a sender calls `POST /events`, what HTTP status code should we return?

**The Options:**

1. **200 OK** — Generic success. Doesn't communicate anything meaningful.

2. **201 Created** — Standard for REST resource creation. Implies the resource (the event) is fully available. But the event isn't delivered yet — just accepted.

3. **202 Accepted** — The HTTP standard for "we got your request and will process it later." Perfect for async workflows.

**What We Chose:**

**202 Accepted.**

```python
@router.post("/events", status_code=202)
async def ingest_event(event, db):
    # ... save and queue ...
    return {"accepted": True, "event_id": event_id}
```

202 tells the sender:
- ✅ Your event was received and stored
- ✅ Deliveries have been queued
- ⏳ Actual delivery happens asynchronously
- ❌ Don't retry — we got it

The sender gets back an `event_id` UUID. They can track delivery status later via `GET /deliveries?event_id=<uuid>`.

---

## What the Architecture Looks Like

Here's the complete flow from start to finish:

```
                    ┌───────────────────────┐
                    │       Sender          │
                    └──────────┬────────────┘
                               │ POST /events
                               ▼
                    ┌───────────────────────┐
                    │     FastAPI App       │
                    │  (main.py + routers)  │
                    │                       │
                    │ 1. Save event to PG   │────▶ PostgreSQL
                    │ 2. Create deliveries  │────▶ PostgreSQL
                    │ 3. Push to Redis      │────▶ Redis Queue
                    │ 4. Return 202         │
                    └───────────────────────┘

    ┌────────────────────┐    ┌────────────────────┐    ┌────────────────────┐
    │     Worker         │    │  Retry Scheduler   │    │      Reaper        │
    │  (worker.py)       │    │  (worker.py)       │    │  (reaper.py)       │
    │                    │    │                    │    │                    │
    │ BRPOP from Redis   │    │ Every 5s: move     │    │ Every 30s: find    │
    │ Check circuit      │    │ delayed jobs       │    │ stuck IN_FLIGHT    │
    │ Sign with HMAC     │    │ back to queue      │    │ jobs, reset them   │
    │ HTTP POST endpoint │    │                    │    │                    │
    └────────┬───────────┘    └────────────────────┘    └────────────────────┘
             │
             │ HTTP POST + HMAC Signature
             ▼
    ┌────────────────────┐
    │    Subscriber      │
    │  (your endpoint)   │
    │                    │
    │ Verify signature   │
    │ Process payload    │
    │ Return 200         │
    └────────────────────┘
```

---

## The Lessons (In One Place)

| # | What We Learned | In One Sentence |
|---|---|---|
| 1 | **Decouple with a queue** | Never make the sender wait for your deliveries. |
| 2 | **At-least-once is enough** | Exactly-once is too expensive. Check for duplicates instead. |
| 3 | **Backoff exponentially** | Retry fast when it helps, slow when it doesn't. |
| 4 | **Use a circuit breaker** | One dead subscriber shouldn't kill your whole system. |
| 5 | **Expect crashes** | When your worker dies, the Reaper saves the day. |
| 6 | **Sign everything** | HMAC-SHA256 with timing-safe comparison. No exceptions. |
| 7 | **Use two databases** | Postgres for truth, Redis for speed. Each plays its role. |
| 8 | **Use 202 Accepted** | Tell the sender "we got it" without promising delivery yet. |

---

## The Tech Stack

- **FastAPI** — Async Python web framework
- **PostgreSQL 16** — Persistent state, delivery tracking
- **Redis 7** — Job queue (LIST) + delay queue (Sorted Set)
- **SQLAlchemy 2.0** — Async database ORM
- **aiohttp** — Async HTTP client for webhook delivery
- **Docker Compose** — Local infrastructure

---

## Want to Build It Yourself?

The full source code is on GitHub. Clone it, run `docker compose up`, and you have a fully functional webhook delivery system running locally.

```bash
docker compose up -d                    # Postgres + Redis
pip install -r requirements.txt
python init_db.py                       # Create tables
uvicorn main:app --reload               # API server (terminal 1)
python worker.py                        # Worker (terminal 2)
python reaper.py                        # Reaper (terminal 3)
python webhook_receiver.py              # Test subscriber (terminal 4)
```

Then register a subscriber and send an event:

```bash
# Register
curl -X POST http://localhost:8000/subscriptions \
  -H "Content-Type: application/json" \
  -d '{"endpoint_url": "http://localhost:9000/webhook", "event_type": "order.placed"}'

# Send an event
curl -X POST http://localhost:8000/events \
  -H "Content-Type: application/json" \
  -d '{"event_type": "order.placed", "payload": {"order_id": "ORD-123", "amount": 99.99}}'
```

---

*Built with Python, Postgres, Redis, and too much coffee.*

*If you found this useful, follow for more deep dives into the engineering decisions behind the systems we rely on every day.*
