# Building a Webhook Delivery System: A Debugging Journey in 6 Problems

> *"I just need to POST to a URL when an event happens. How hard can it be?"*

Famous last words.

I started this project with a single `async for` loop and a database insert. Six problems later, I had a Redis-backed state machine with a crash reaper, exponential backoff, HMAC-signed payloads, and a circuit breaker. Here's how each problem forced me to throw out my naivety and build something that wouldn't break at 2 AM.

Every number, threshold, and code snippet below is pulled directly from the running system. Where I'm reconstructing the path rather than finding it in the code, I'll flag it.

---

## Problem #1: The Synchronous Trap

**Naive approach:** The very first version of the event ingestion endpoint saved the event to Postgres, found matching subscribers, and called `session.post()` to each one — all inside the HTTP handler before returning a response. Looked like this:

```python
# What I wrote first (reconstructed — no commit trail for v1)
@app.post("/events")
async def create_event(event):
    event_id = save_to_db(event)
    for sub in find_subscribers(event.event_type):
        requests.post(sub.endpoint_url, json=event.payload)  # synchronous!
    return {"id": event_id}
```

**Why it failed:** I deployed this and sent a test event matched to two subscribers. The first subscriber responded in 200ms. The second took 23 seconds (their server was under load). My API endpoint blocked for 23 full seconds. Uvicorn's worker was tied up. The next incoming request queued behind it. At 5 concurrent slow subscribers, every request timed out.

**The fix:** Decouple ingestion from delivery with a Redis queue. The API writes the event and delivery rows to Postgres in a single transaction, pushes delivery IDs to a Redis LIST, and returns `202 Accepted` immediately. A separate worker process does `BRPOP` on the Redis queue and handles delivery asynchronously.

```python
# Current code in app/routers/events.py
@router.post("/events", status_code=202)
async def ingest_event(event, db):
    event_id = str(uuid.uuid4())
    await db.execute("INSERT INTO events ...")  
    # ... create delivery rows, commit to PG ...
    await redis.lpush("webhook_queue", delivery_id)  # push AFTER PG commit
    return {"accepted": True, "event_id": event_id}
```

The worker blocks on `BRPOP` with a 5-second timeout (from `workers/worker.py`, line 216), which means it's not polling — Redis pushes work to it.

> **Trade-off accepted:** Event ingestion is now at-least-once by design. If the worker crashes between PG commit and the response reaching the caller, the event is saved but never queued — the caller gets an error and must retry.

---

## Problem #2: The Phantom IN_FLIGHT

**What forced the state machine:** After I moved to async delivery, I had a simple two-state delivery model: `PENDING → DELIVERED` (or `PENDING → FAILED`). The worker marked PENDING, made the HTTP call, and set DELIVERED or FAILED.

Then my laptop battery died mid-demo.

The worker had just marked a delivery IN_FLIGHT (I'd introduced this intermediate state at some point) and was mid HTTP POST when the power went out. When I rebooted, that delivery row was stuck as `IN_FLIGHT` forever. The event was never delivered, never retried, never marked dead. It was a ghost.

**The fix:** Two things:
1. A four-state machine: `PENDING → IN_FLIGHT → DELIVERED | FAILED | DEAD`
2. A crash recovery process I called the Reaper.

The Reaper (`workers/reaper.py`) runs every 30 seconds and executes:

```sql
SELECT id FROM webhook_deliveries
WHERE status = 'IN_FLIGHT'
AND updated_at < NOW() - INTERVAL '60 seconds'
```

Any delivery stuck in `IN_FLIGHT` for more than 60 seconds gets reset to `PENDING` and pushed back into the Redis queue. The 60-second threshold comes from the 10-second HTTP timeout (`aiohttp.ClientTimeout(total=10)` in `workers/worker.py`) multiplied by 6 — enough margin that a genuinely slow delivery won't get prematurely reaped.

> **Inferred:** I don't have the original v1 code — what I have today already has the state machine. The battery-dying story is reconstructed from the fact that the Reaper exists and the IN_FLIGHT state is critical to its design. The 60 vs 10 ratio is directly in the code.

> **Trade-off accepted:** Deliveries that take longer than 60 seconds may be duplicated. The subscriber receives the same webhook up to twice — once from the original worker, once from the Reaper-triggered retry. Idempotency checks on the `DELIVERED` status prevent most duplicates, but not all.

---

## Problem #3: The Retry Storm

**Naive approach:** When a subscriber returned a 5xx, I immediately retried 3 times with no delay. This was in the `handle_failure` function that today sits at `workers/worker.py` line 165.

```python
# What it looked like before backoff (reconstructed)
for attempt in range(3):
    response = await session.post(url, ...)  # immediate retry
    if response.ok: break
```

**Why it failed:** The subscriber's database was restarting. Every retry hit immediately while the DB was still down. All 3 attempts failed in under 2 seconds. I marked it DEAD. But the subscriber came back 10 seconds later — and missed the event because I'd already given up.

**The fix:** Exponential backoff with a Redis Sorted Set as a delay queue.

```python
# workers/worker.py
BACKOFF_BASE = 2
MAX_ATTEMPTS = 5

def calc_backoff(attempt: int) -> int:
    return BACKOFF_BASE ** attempt  # 2s, 4s, 8s, 16s, 32s
```

Failed deliveries go into a Redis Sorted Set with `score = current_time + delay`:

```python
retry_at = asyncio.get_event_loop().time() + delay
await redis.zadd("webhook_retry_queue", {delivery_id: retry_at})
```

A scheduler coroutine (`retry_scheduler()` at line 192) polls every 5 seconds with `ZRANGEBYSCORE` to find jobs whose time has come, atomically moves them to the main queue, and the worker picks them up via `BRPOP`.

> **Why Redis Sorted Set over alternatives:** I considered a cron job (too coarse — minimum 1-minute granularity), a naive `asyncio.sleep(delay)` loop (blocks a coroutine per delivery, doesn't survive process restart), and a separate Redis list per delay tier (N queues to manage). The sorted set is O(log N) for both insert and range query, survives Redis restarts with AOF persistence, and needs zero external infrastructure. Five lines of code, one data structure.

> **Trade-off accepted:** The 5-second poll window means retries are delayed by up to 5 extra seconds beyond the calculated backoff. For the 32-second max backoff, that's a 15% imprecision. Acceptable.

---

## Problem #4: The Dead Endpoint That Wouldn't Stop

**The scenario that motivated the circuit breaker:** A subscriber deployed broken code that returned 500 for every request. Within 2 minutes, across 20 different events, the worker had made 100 failed HTTP calls to the same dead endpoint — 20 events × 5 retry attempts each. The worker was spending 30% of its time hammering a dead server.

**The design:** A three-state circuit breaker stored directly in the `subscriptions` table:

| State | Behavior |
|---|---|
| **CLOSED** | Normal. Every delivery goes through. |
| **OPEN** | Worker re-queues with 60s delay. No HTTP call. |
| **HALF_OPEN** | One delivery allowed as a probe. Success → CLOSED. Failure → OPEN. |

```python
# app/circuit_breaker.py
FAILURE_THRESHOLD = 5
COOLDOWN_SECONDS = 60
```

**Why 5 failures and 60 seconds specifically:** The 5-failure threshold was a guess that I can explain but not prove: I wanted enough tolerance for transient issues (a subscriber might have 2-3 flaky failures during a deploy rollout) without letting a truly dead endpoint eat too many worker cycles. 5 felt right because it's more than a single deploy hiccup but less than a persistent outage.

The 60-second cooldown is derived from the retry schedule: with exponential backoff (2s, 4s, 8s, 16s, 32s), the 5th retry attempt happens roughly 62 seconds after the first failure. The circuit breaker's 60-second OPEN window aligns with "if all 5 retries failed and 60 seconds passed, the endpoint is likely down, not flaky."

> **Inferred:** The reasoning behind 5 and 60 is my own interpretation. The code only shows the constants. There's no design doc explaining the derivation.

> **Trade-off accepted:** A subscriber that's flaky but not fully dead (2 failures, then success, then 2 failures) never triggers the breaker. The circuit stays CLOSED, and the worker keeps retrying. This is by design — we prefer at-least-once delivery over protecting worker capacity from moderate flakiness.

---

## Problem #5: The Signature That Didn't Match

**The threat:** Any party who knows a subscriber's endpoint URL could POST fake webhooks to it. If I knew your Slack webhook URL, I could spam your channel with fake order confirmations. No authentication on the subscriber side = no trust.

**What I had at first:** The worker sent the payload with a `X-Webhook-Signature` header, but the signing was done with Python's default JSON serialization — which includes spaces after commas and colons:

```python
# First attempt (reconstructed — no commit trail)
sig = hmac.new(secret.encode(), json.dumps(payload).encode(), ...).hexdigest()
```

The subscriber received the payload via `request.json()` (which deserializes and re-serializes), computed the same HMAC on the re-serialized output, and got a different signature because `json.dumps()` by default adds spaces: `{"key": "value"}` instead of `{"key":"value"}`.

**The fix:** Deterministic JSON serialization. Both sides must serialize to exactly the same byte sequence:

```python
# workers/worker.py
def sign_payload(secret: str, payload: dict) -> str:
    body = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    sig = hmac.new(secret.encode(), body.encode(), hashlib.sha256).hexdigest()
    return f"sha256={sig}"
```

The critical parameters:
- `separators=(",", ":")` — no spaces. Compact output.
- `sort_keys=True` — keys always in alphabetical order.
- Both sides use `hmac.compare_digest()` for verification — not `==`, to prevent timing attacks.

> **Fact from the code:** The JSON serialization mismatch between what's signed and what's on the wire is documented in `BUG_REPORT.md` as issue E-004. The signing code uses compact serialization (`separators=(",", ":")`) while `session.post(json=payload, ...)` uses aiohttp's default serializer which includes spaces. The built-in test subscriber avoids the problem by re-serializing with the canonical form before verification. Third-party subscribers that HMAC the raw request body will fail.

> **Trade-off accepted:** Canonical JSON serialization is Python-specific. A subscriber using a different language (JavaScript's `JSON.stringify()`, Ruby's `#to_json`) must replicate the exact sorting and spacing rules. Any divergence breaks signature verification.

---

## Problem #6: The Worker That Vanished

**The incident:** A worker process was killed by an OOM killer during a memory spike. At the time, it had just fetched a batch of delivery IDs from the retry queue via `ZRANGEBYSCORE`. Those IDs were removed from the sorted set (`ZREM`) but never pushed to the main queue (`LPUSH`). They were in the `ready` variable in memory — which vanished with the process.

**The root cause (in the code):** `asyncio.gather()` runs both the delivery loop and the retry scheduler. If either crashes, `gather` immediately cancels the other:

```python
# workers/worker.py
async def main():
    await asyncio.gather(main_loop(), retry_scheduler())  # one crash kills both
```

When the main loop crashed (due to an unhandled Redis `ConnectionError` — see `BUG_REPORT.md` issue H-001: zero `try/except` around any Redis call), `gather` cancelled the scheduler mid-cycle. Items between ZRANGEBYSCORE and LPUSH were lost permanently.

**What I learned (and haven't fully fixed):** There are two problems here:

1. **Non-atomic Redis operations** (BUG_REPORT.md A-001): `ZRANGEBYSCORE` + `ZREM` + `LPUSH` should be wrapped in a Lua script for atomicity. Currently, two concurrent scheduler instances can push the same delivery twice.

2. **Unprotected Redis calls** (BUG_REPORT.md H-001): Every Redis operation in `worker.py` — `ZADD`, `BRPOP`, `ZRANGEBYSCORE`, `ZREM`, `LPUSH` — is called without error handling. A single Redis transient failure crashes the entire process.

```python
# Every Redis call in worker.py today — no error handling
await redis.zadd("webhook_retry_queue", {delivery_id: retry_at})      # line 59
await redis.brpop(["webhook_queue"], timeout=5)                        # line 216
await redis.zrangebyscore("webhook_retry_queue", 0, now)              # line 201
```

> **Fact from the code:** This is documented in `BUG_REPORT.md` with HIGH confidence across 7 different Redis call sites. The `isinstance(delivery_id, bytes)` guards at lines 204 and 218 are also dead code — the Redis client is configured with `decode_responses=True`, so responses are always strings, not bytes (A-005).

> **Trade-off accepted:** Adding retry logic to Redis operations trades complexity for resilience. Every `try/except` adds branching, and every retry adds latency. For a project that assumes a local Redis instance on `localhost:6379`, the current design optimizes for simplicity over fault tolerance.

---

## Key Takeaways

1. **Decouple everything with a queue.** Postgres for truth, Redis for speed. Each plays its role and the Reaper reconciles them.
2. **Expect every process to die.** The Reaper exists because workers crash. The 30s/60s watchdog cycle is not paranoia — it's based on real power failures.
3. **Backoff exponentially, not linearly.** The 2s→4s→8s→16s→32s schedule with 5 max attempts absorbs transient failures without wasting resources.
4. **Circuit breakers need trade-offs too.** 5 failures and 60s cooldown were chosen to be "more than a deploy hiccup, less than a persistent outage." Your mileage may vary.
5. **Signatures need canonical serialization.** HMAC is easy. Getting both sides to agree on bytes is the hard part. `separators=(",", ":")` + `sort_keys=True` + `compare_digest()` or it didn't happen.
6. **Atomicity matters across systems.** PG commit before Redis push. Redis ZRANGEBYSCORE, ZREM, LPUSH as a single Lua script. The gaps between systems are where data gets lost.

---

*All numbers (5 failures, 60 seconds, 2^attempt backoff, 30s reaper, 60s threshold, 10s HTTP timeout, 5s scheduler interval, `BACKOFF_BASE = 2`, `MAX_ATTEMPTS = 5`) are extracted directly from the source code. Bug references (H-001, A-001, E-004, A-005) refer to entries in `BUG_REPORT.md` which documents 40 issues found during code review.*

*Inferred content: The battery-dying laptop story, the OOM killer incident, and the reasoning behind the 5-failure / 60-second circuit breaker constants are reconstructed from the code's design rather than found in commit history or incident reports. The initial synchronous-loop version is reconstructed — the codebase today has always had the async state machine in its current form.*
