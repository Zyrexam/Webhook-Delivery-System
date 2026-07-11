# Bug Report: Second-Pass Audit

## Issues NOT in the first report — additional 30+ findings

Generated: 2026-06-19

---

## Fixes Applied

| ID | Severity | Change |
|---|---|---|
| C01 | Critical | Added `AND status = 'IN_FLIGHT'` guard + `rowcount` check to reaper UPDATE — prevents resetting DELIVERED deliveries |
| C02 | Critical | Replaced `asyncio.get_event_loop().time()` with `time.time()` (Unix timestamps) for Redis retry scores — retries survive restarts |
| C03 | Critical | Separated read (`get_circuit_state`) from write (`try_transition_to_half_open`) — no more hidden side-effect commit |
| R01 | High | Added `AND status = 'PENDING'` + `rowcount` check to IN_FLIGHT UPDATE — prevents duplicate delivery by concurrent workers |
| E06 | Medium | Batched Redis LPUSH via `*delivery_ids` (part of H-005 fix) — API no longer does N sequential round-trips per event |

---

## Table of Contents

1. [Critical Findings](#1-critical-findings)
2. [Race Conditions & Concurrency (New)](#2-race-conditions--concurrency-new)
3. [Circuit Breaker Deep Analysis](#3-circuit-breaker-deep-analysis)
4. [Authentication & Authorization Deep Analysis](#4-authentication--authorization-deep-analysis)
5. [Data & Transaction Integrity](#5-data--transaction-integrity)
6. [SSRF & Redirect Following](#6-ssrf--redirect-following)
7. [API Design & Contract Issues](#7-api-design--contract-issues)
8. [Resilience & Operational Issues](#8-resilience--operational-issues)
9. [Edge Cases & State Handling](#9-edge-cases--state-handling)
10. [Miscellaneous](#10-miscellaneous)

---

## 1. Critical Findings

<!-- C03 — FIXED: Separated read (get_circuit_state) from write (try_transition_to_half_open) — no more hidden side-effect commit -->



[Back to ToC](#table-of-contents)

---

## 2. Race Conditions & Concurrency (New)


### R02 — Reaper pushes to Redis before checking UPDATE rowcount → phantom jobs even with status guard

| Field | Value |
|---|---|
| **File** | `reaper.py:30-44` |
| **Function** | `reap()` |
| **Severity** | **High** |
| **Confidence** | **HIGH** |

**Why it is a bug:**
The reaper loop pushes delivery_id to Redis at line 42 BEFORE committing the DB UPDATE at line 44. If the commit fails (any reason), the DB changes roll back but the Redis push is already done. The DB still shows IN_FLIGHT, but Redis has the delivery_id. On the next reaper cycle, the delivery is selected again and pushed again. Accumulates phantom jobs.

Additionally, there's no `rowcount` check after the UPDATE. If the delivery was already processed (status changed between SELECT and UPDATE), the UPDATE affects 0 rows. The code still pushes to Redis unconditionally.

**Reproduction:**
Trigger a DB commit failure at line 44 (constraint violation, connection loss). Redis holds phantom delivery_ids.

**Worst-case production impact:**
Phantom jobs accumulate in Redis across reaper cycles. Workers waste CPU processing phantom deliveries (SELECT + JOIN + check → skip). Redis memory fills with dead entries.

---

### R03 — Worker and reaper can deadlock on overlapping row updates

| Field | Value |
|---|---|
| **File** | `worker.py:104-111` and `reaper.py:34-39` |
| **Functions** | `deliver()` and `reap()` |
| **Severity** | **Medium** |
| **Confidence** | **Medium** |

**Why it is a bug:**
Both the worker and reaper UPDATE `webhook_deliveries` rows concurrently. The worker updates one row at a time (IN_FLIGHT). The reaper updates multiple rows (PENDING reset) sequentially in a single transaction.

If the worker holds a row lock on delivery X (IN_FLIGHT update, committed) and the reaper holds row lock on delivery Y (PENDING update, not yet committed), and then:
- Worker tries to UPDATE delivery Y → blocked by reaper's lock
- Reaper tries to UPDATE delivery X → blocked by worker's lock (reaper reads latest committed data, and X is now DELIVERED... wait, the reaper UPDATE has no status check, so it would try to update X regardless)

Actually, the deadlock scenario is narrow but possible:
1. Worker A: IN_FLIGHT on X, not yet committed (slow commit due to network latency)
2. Reaper: IN_FLIGHT on Y, not yet committed (inside the for loop, before commit at line 44)
3. Worker B (another delivery): tries to IN_FLIGHT on Y → blocked by reaper's lock
4. Reaper moves to next iteration: tries to IN_FLIGHT on X → blocked by Worker A's lock
5. Deadlock detected by PostgreSQL → one transaction aborted

**Reproduction:**
Unlikely in practice but possible under load with many overlapping deliveries and a slow database.

**Worst-case production impact:**
Transaction aborted. Worker or reaper receives deadlock error. Worker's delivery attempt fails (HTTP may have succeeded already, but DB update fails → re-delivery). Reaper's entire batch rolls back.

---

[Back to ToC](#table-of-contents)

---

## 3. Circuit Breaker Deep Analysis

### Full execution trace:

```
Subscription.create()
  → circuit_state = 'CLOSED', failure_count = 0, circuit_opened_at = NULL

For each deliver() call:
  1. get_circuit_state(sub_id)
     → SELECT circuit_state, failure_count, circuit_opened_at
     → if CLOSED: return 'CLOSED' (no write)
     → if HALF_OPEN: return 'HALF_OPEN' (no write)
     → if OPEN:
         → if NOW() - opened_at >= 60s:
             → UPDATE SET circuit_state = 'HALF_OPEN'
             → COMMIT ← SIDE EFFECT! (C03)
             → return 'HALF_OPEN'
         → else: return 'OPEN'
  
  2. CLOSED or HALF_OPEN → proceed to HTTP POST
  
  3a. HTTP success (status < 300):
      → record_success(sub_id)
        → UPDATE SET circuit_state = 'CLOSED', failure_count = 0, circuit_opened_at = NULL
        → COMMIT
      → circuit is CLOSED
  
  3b. HTTP failure (status >= 300 or exception):
      → record_failure(sub_id)
        → UPDATE SET failure_count = failure_count + 1 WHERE id = :id RETURNING failure_count
        → COMMIT (Transaction 1)
        → if failure_count >= 5:
            → UPDATE SET circuit_state = 'OPEN', circuit_opened_at = NOW()
            → COMMIT (Transaction 2)
      → handle_failure(...)
```

### Additional issues found:

#### CB01 — HALF_OPEN allows unlimited concurrent test requests (stampede)

Already noted in V1. Multiple workers all see HALF_OPEN and all proceed. No coordination. Target endpoint receives full traffic during recovery test.

#### CB02 — `record_success` unconditionally resets circuit, overriding manual intervention

Already noted in V1 (D-003). Operator manually sets `circuit_state = 'OPEN'` → next successful delivery resets to CLOSED.

**NEW — CB03: `record_success` also resets `failure_count = 0` without condition.** If the circuit was in HALF_OPEN and a delivery succeeds, `failure_count` goes to 0. But what if there were accumulated failures from BEFORE the circuit opened? (e.g., failure_count was 7 when circuit opened at threshold 5). Those extra 2 failures are also forgotten. The circuit should only reset `failure_count` to 0, which is what it does — the extra failures between threshold and recovery are forgotten. This is arguably correct behavior for a circuit breaker (recovery means "we believe the system is healthy now").

#### CB04 — No circuit state in `GET /subscriptions/{id}` response

Already noted (C-001 in V1).

#### CB05 — No API to reset circuit breaker

No endpoint allows manually closing an open circuit. The only way to reset it is:
1. Wait for cooldown (60s) + a successful delivery (auto-reset via `record_success`)
2. Direct DB UPDATE

Missing API surface: `POST /subscriptions/{id}/reset-circuit` or `PATCH /subscriptions/{id}` with a `reset_circuit` option.

#### CB06 — Circuit breaker state is in the database with no caching

Every delivery does a DB round-trip just to check if the circuit is open. For CLOSED circuits (99.9% of cases), this is a wasted query. An in-memory cache with TTL (e.g., 5 seconds) would eliminate 99.9% of circuit state queries.

#### CB07 — `get_circuit_state` returns `'CLOSED'` for missing subscriptions

If a subscription is deleted but a delivery row still references it, `get_circuit_state` at line 17-18 returns `'CLOSED'`. The worker proceeds to deliver. The HTTP POST uses `row.endpoint_url` from the JOIN query (which would have failed first since the subscription is gone). So this path is only reached if the subscription exists for the JOIN but is deleted between the JOIN and the circuit breaker check. In that case, `record_failure` at line 152 would crash (N-002 in V1).

#### CB08 — Circuit cooldown is measured by server-side `datetime.now(timezone.utc)`, not delivery time

The cooldown starts from `circuit_opened_at = NOW()` (database time). The cooldown check uses `datetime.now(timezone.utc)` (Python server time). If server and database clocks drift by even a few seconds, the cooldown period can be slightly shorter or longer than intended.

---

[Back to ToC](#table-of-contents)

---

## 4. Authentication & Authorization Deep Analysis

### Affected endpoints (complete list):

| Endpoint | Method | File | Impact of no auth |
|---|---|---|---|
| `/events` | POST | `routers/events.py:12` | Anyone emits events → PG+Redis OOM |
| `/subscriptions` | POST | `routers/subscriptions.py:34` | Anyone registers SSRF targets |
| `/subscriptions` | GET | `routers/subscriptions.py:57` | Anyone reads all subscriber URLs |
| `/subscriptions/{id}` | GET | `routers/subscriptions.py:88` | Anyone reads specific subscription |
| `/subscriptions/{id}` | PATCH | `routers/subscriptions.py:105` | Anyone disables any subscription |
| `/subscriptions/{id}` | DELETE | `routers/subscriptions.py:123` | Anyone permanently deletes any subscription |
| `/deliveries` | GET | `routers/deliveries.py:24` | Anyone reads full delivery history |
| `/deliveries/stats` | GET | `routers/deliveries.py:70` | Anyone reads system-wide statistics |
| `/` | GET | `main.py:15` | Info disclosure (list of all routes) |

### Security Impact Matrix:

**Total service takeover scenario:**
1. Attacker discovers API endpoint
2. `DELETE /subscriptions/*` all existing subscriptions → denial of service
3. `POST /subscriptions` with attacker-controlled endpoint → intercept all future events
4. Every event payload is forwarded to attacker's server (data exfiltration)
5. `GET /deliveries` reads error messages with internal information
6. `POST /events` with huge payloads → OOM/RDoS

**No defense layers:**
- No API key/token
- No auth middleware 
- No rate limiting
- No IP allowlisting
- No request size limits
- No audit logging (can't trace who did what)

### Additional auth findings:

#### A01 — No authorization scoping

Even if auth were added, there's no way to restrict a client to only their own subscriptions. `DELETE /subscriptions/{id}` would check "is this your subscription?" — currently it doesn't.

#### A02 — No request signing or idempotency key for `POST /events`

Without idempotency keys, duplicate event submission is undetectable. If a client times out waiting for the response (even though the event was committed), they retry and create a duplicate event.

#### A03 — No CSRF protection

Not applicable for a backend API (no browser session), but worth noting if this were ever fronted by a browser-based admin panel.

---

[Back to ToC](#table-of-contents)

---

## 5. Data & Transaction Integrity

### T01 — `DELETE /subscriptions/{id}` creates orphaned deliveries (when it succeeds)

| Field | Value |
|---|---|
| **File** | `routers/subscriptions.py:124-132` |
| **Function** | `delete_subscription()` |
| **Severity** | **High** |
| **Confidence** | **High** |

**Why it is a bug:**
When `DELETE /subscriptions/{id}` succeeds (no FK violation because no deliveries exist YET), the subscription is removed from the database. But any deliveries already queued in Redis or in the worker's in-flight processing still reference the deleted subscription.

- If the delivery was already fetched by the worker (the JOIN at worker.py:31-40 already executed), the worker has `row.endpoint_url` and `row.secret` in memory. It can still deliver. The delivery row in the DB becomes an orphan (references a deleted subscription FK). Wait — the FK constraint prevents the DELETE if deliveries exist. So this case doesn't happen.

- If the delivery is still in Redis (not yet picked up by worker), the worker's INNER JOIN with `subscriptions` will fail (subscription doesn't exist). `result.fetchone()` returns None at line 42. Worker prints "not found — skipping" and silently drops the delivery. **The event is lost.**

The DELETE succeeds only when there are zero delivery rows for that subscription. This means:
- No events were ever emitted for this subscription → clean deletion ✓
- Events were emitted, deliveries were created, but they were all deleted or the FK was somehow bypassed → orphaned delivery_ids in Redis, worker silently drops them

**Worst-case production impact:**
Quiet data loss. The delivery row exists in the DB but the worker can't process it (JOIN fails). The event is lost because the subscription was deleted between event creation and worker processing.

---

### T02 — No SQLAlchemy `session.rollback()` in `get_db()` on exception

| Field | Value |
|---|---|
| **File** | `database.py:13-15` |
| **Function** | `get_db()` |
| **Code path** | `async with AsyncSessionLocal() as session: yield session` |
| **Severity** | **Medium** |
| **Confidence** | **High** |

**Why it is a bug:**
When an exception occurs during request processing, the session context manager's `__aexit__` is called. SQLAlchemy's `AsyncSession.__aexit__` does NOT automatically roll back on exception — it only closes the session. If the session was used for writes that weren't committed (e.g., after a failed commit), the connection is returned to the pool with an open transaction. The next request using that connection may see stale/incorrect data or encounter the previous transaction's locks.

**Reproduction:**
1. Request starts: session acquired
2. `db.execute(UPDATE ...)` — modification is in the session's transaction
3. `db.commit()` fails (e.g., constraint violation)
4. Exception propagates
5. Session `__aexit__` closes the session but doesn't roll back the underlying transaction
6. Connection returned to pool with transaction still open
7. Next request on same connection: sees old snapshot, or gets "no transaction in progress" errors

**Note:** SQLAlchemy 2.0's `async_sessionmaker` with no `expire_on_commit` and no explicit `close_resets_only` may or may not rollback. The safe pattern is:
```python
async def get_db():
    async with AsyncSessionLocal() as session:
        try:
            yield session
        except:
            await session.rollback()
            raise
```

---

### T03 — Worker stores raw exception strings in the database (information disclosure + non-deterministic)

| Field | Value |
|---|---|
| **File** | `worker.py:156` |
| **Function** | `deliver()` exception handler |
| **Code path** | `error_message=str(e)` |
| **Severity** | **Medium** |
| **Confidence** | **High** |

**Why it is a bug:**
`str(e)` on an exception can produce:
- Sensitive information: file paths, hostnames, IP addresses, DNS names
- Non-deterministic data: timestamps, error codes that depend on exact failure moment
- Very long messages: some exceptions (e.g., aiohttp.ClientConnectorError) include the entire endpoint URL and connection parameters

These are stored in `webhook_deliveries.error_message` and served via `GET /deliveries`. The `X-Webhook-Delivery` header includes the delivery_id, so an attacker who knows a delivery_id can retrieve the error details.

**Reproduction:**
Point subscription to `http://nonexistent-internal-server.internal.company`, emit event, check `GET /deliveries?subscription_id=...` → error_message includes the internal hostname.

---

### T04 — Event payload serialized twice in `sign_payload` and `session.post`

| Field | Value |
|---|---|
| **File** | `worker.py:18-25,118,121` |
| **Functions** | `sign_payload()`, `deliver()` |
| **Severity** | **Low** |
| **Confidence** | **High** |

**Why it is a bug:**
`sign_payload(row.secret, payload)` at line 118 calls `json.dumps(payload, sort_keys=True)` internally to canonicalize for HMAC. Then `session.post(json=payload, ...)` at line 121 serializes the SAME payload dict again for the HTTP body. The payload dict is serialized to JSON twice for every single delivery.

For large payloads (1MB+), this doubles serialization time and memory allocation.

---

[Back to ToC](#table-of-contents)

---

## 6. SSRF & Redirect Following

### S01 — aiohttp follows HTTP redirects by default → SSRF amplification via redirect

| Field | Value |
|---|---|
| **File** | `worker.py:119` |
| **Function** | `deliver()` |
| **Code path** | `session.post(row.endpoint_url, ...)` — no redirect configuration |
| **Severity** | **High** |
| **Confidence** | **High** |

**Why it is a bug:**
aiohttp's default `ClientSession` follows redirects (up to `max_redirects=30`). An attacker can:
1. Create a subscription pointing to `https://attacker-controlled.com/redirect`
2. The attacker's server returns `302 Location: http://169.254.169.254/latest/meta-data/`
3. aiohttp follows the redirect → the worker's HTTP client connects to the cloud metadata endpoint
4. The response (containing cloud credentials) is stored in `status_code` (success) or `error_message` (truncated failure)
5. Attacker reads delivery details via `GET /deliveries`

The SSRF bypasses any URL validation done at subscription creation time (Pydantic's `HttpUrl` validator), because the redirect URL is never validated.

**Reproduction:**
```bash
# 1. Create subscription with attacker endpoint
curl -X POST http://localhost:8000/subscriptions \
  -d '{"endpoint_url": "https://myserver.com/redirect-to-meta", "event_type": "ssrf"}'

# 2. On myserver.com/redirect-to-meta: return 302 → http://169.254.169.254/latest/meta-data/

# 3. Emit event
curl -X POST http://localhost:8000/events \
  -d '{"event_type": "ssrf", "payload": {}}'

# 4. Worker POSTs to myserver.com, follows redirect to AWS metadata
# 5. The metadata response is logged or stored
```

**Worst-case production impact:**
Attacker reads cloud instance metadata including IAM credentials, user data, SSH keys. Complete cloud account compromise.

---

### S02 — Subscription `endpoint_url` accepts `https://` URLs pointing to internal / private IPs

| Field | Value |
|---|---|
| **File** | `routers/subscriptions.py:17` |
| **Model** | `SubscriptionCreate.endpoint_url: HttpUrl` |
| **Severity** | **High** |
| **Confidence** | **High** |

**Why it is a bug:**
Pydantic's `HttpUrl` validates URL format (scheme, host format, port range) but does NOT validate against private IP ranges. The following are all valid:
- `http://127.0.0.1:5432` (local PostgreSQL)
- `http://10.0.0.1:8000` (internal API)
- `http://[::1]:6379` (local Redis — though Redis speaks its own protocol, so the HTTP POST would fail)
- `https://internal-admin.example.internal/` (internal service, resolvable via internal DNS)

**Reproduction:**
```bash
curl -X POST http://localhost:8000/subscriptions \
  -d '{"endpoint_url": "http://localhost:5432", "event_type": "test"}'
# → 201 Created. Worker will POST to local PostgreSQL.
```

---

### S03 — No URL allowlist or blocklist for subscription endpoints

There's no configuration mechanism for allowed URL patterns, blocked hosts, or allowed IP ranges.

---

[Back to ToC](#table-of-contents)

---

## 7. API Design & Contract Issues

### A01 — No pagination on `GET /subscriptions`

| Field | Value |
|---|---|
| **File** | `routers/subscriptions.py:57-85` |
| **Function** | `list_subscriptions()` |
| **Severity** | **Medium** |
| **Confidence** | **High** |

**Why it is a bug:**
The list endpoint returns ALL subscriptions without pagination. With thousands of subscriptions:
- Memory: entire result set loaded into Python at line 72
- Network: potentially megabytes of JSON sent in a single response
- DB: no LIMIT clause → potentially millions of rows fetched

**Reproduction:**
Insert 10000 subscriptions. `GET /subscriptions` returns them all in one response.

---

### A02 — Non-UUID strings in path/query parameters cause 500 errors

| Field | Value |
|---|---|
| **Files** | `routers/subscriptions.py:105,107,123` `routers/deliveries.py:26-27` |
| **Parameters** | `sub_id`, `event_id`, `subscription_id` (UUID columns, string parameters) |
| **Severity** | **Medium** |
| **Confidence** | **High** |

**Why it is a bug:**
`GET /subscriptions/not-a-uuid` → FastAPI binds `"not-a-uuid"` to the `sub_id` parameter and passes it to the SQL query. asyncpg sends this to PostgreSQL's UUID column. PostgreSQL raises `invalid input syntax for type uuid: "not-a-uuid"`. The exception is unhandled → FastAPI returns 500 Internal Server Error with the PG error message.

Should return 400 Bad Request with a clear message about invalid UUID format.

**Affected endpoints:**
- `GET /subscriptions/{sub_id}` — 500 on non-UUID
- `PATCH /subscriptions/{sub_id}` — 500 on non-UUID
- `DELETE /subscriptions/{sub_id}` — 500 on non-UUID
- `GET /deliveries?event_id=xxx` — 500 on non-UUID
- `GET /deliveries?subscription_id=xxx` — 500 on non-UUID

---

### A03 — `GET /deliveries` accepts `status` filter with no validation against allowed values

| Field | Value |
|---|---|
| **File** | `routers/deliveries.py:47-49` |
| **Parameter** | `status: Optional[str]` |
| **Severity** | **Low** |
| **Confidence** | **High** |

**Why it is a bug:**
`status` can be any string. Filtering `?status=INVALID` returns empty (no matching rows). But `?status=%27%3BDROP%20TABLE%20webhook_deliveries%3B--` is properly parameterized and safe from SQL injection. The issue is that the API doesn't validate against the known status values (PENDING, IN_FLIGHT, DELIVERED, FAILED, DEAD). A typo in the client silently returns empty results.

---

### A04 — No `operation_id` or `summary` on API endpoints

OpenAPI docs are auto-generated but have no descriptions. Endpoints show up as `ingest_event`, `create_subscription`, etc. in the auto-generated OpenAPI spec. No human-readable descriptions.

---

[Back to ToC](#table-of-contents)

---

## 8. Resilience & Operational Issues

### O01 — No graceful shutdown (SIGTERM handling)

| Field | Value |
|---|---|
| **Files** | `worker.py:228`, `reaper.py:55` |
| **Functions** | `main()` (module level) |
| **Code path** | `asyncio.run(main())` |
| **Severity** | **High** |
| **Confidence** | **High** |

**Why it is a bug:**
`asyncio.run(main())` does not install signal handlers for SIGTERM or SIGINT. When the process receives SIGTERM (e.g., during deployment, scaling down, or Kubernetes pod termination):
- Worker: actively delivers an HTTP POST. The running task is cancelled mid-flight. The aiohttp session is interrupted. The DB transaction (IN_FLIGHT) may or may not be committed.
- Reaper: mid-loop through stuck deliveries. Some DB updates were applied but not committed. Some Redis pushes may have happened.

**Worker SIGTERM impact:**
1. Worker is delivering delivery X (HTTP POST in progress)
2. SIGTERM → asyncio cancels the task → `deliver()` is interrupted
3. The IN_FLIGHT commit at line 111 may have succeeded (DB says IN_FLIGHT)
4. The HTTP POST may or may not have reached the subscriber
5. The delivery stays IN_FLIGHT → reaper rescues it after 60s → re-delivery
6. If HTTP POST already succeeded, subscriber gets duplicate

**Reaper SIGTERM impact:**
1. Reaper has processed 10 of 50 stuck deliveries (DB updated, Redis pushed, not committed)
2. SIGTERM → `reap()` interrupted
3. DB transaction is rolled back → those 10 deliveries go back to IN_FLIGHT
4. Redis pushes already happened → phantom jobs
5. Next reaper cycle (after restart) reselects all 50 → pushes again → duplicates

---

### O02 — No health check endpoint

| Field | Value |
|---|---|
| **File** | All route files |
| **Severity** | **Medium** |
| **Confidence** | **High** |

**Why it is a bug:**
No `/health` or `/ready` endpoint for:
- Kubernetes liveness/readiness probes
- Load balancer health checks
- Docker HEALTHCHECK
- Deployment health verification without checking route listing (`GET /`)

---

### O03 — No request timeout middleware

| Field | Value |
|---|---|
| **File** | `main.py` |
| **Severity** | **Medium** |
| **Confidence** | **Medium** |

**Why it is a bug:**
FastAPI/Uvicorn has no global request timeout configured. A slow database query (sequential scan on large delivery table), a Redis connection hang, or a blocked pool acquisition can hold the request indefinitely. The HTTP client eventually times out, but the server-side connection is consumed until the OS TCP timeout.

---

### O04 — Print-based logging throughout

| Field | Value |
|---|---|
| **Files** | `worker.py`, `reaper.py`, `circuit_breaker.py` |
| **All print statements** | inconsistent format, no timestamps, no log levels |
| **Severity** | **Medium** |
| **Confidence** | **High** |

**Why it is a bug:**
All logging uses `print()` with no:
- Timestamps
- Log levels (INFO, WARNING, ERROR)
- Structured format (JSON)
- Logger names
- Context correlation IDs

Make patterns used:
- `[worker]` / `[scheduler]` / `[reaper]` / `[circuit]` — manual prefixes, inconsistent
- `print(f"[worker] ✓ delivered {delivery_id} → {response.status}")` — success with unicode checkmark
- `print(f"[worker] ✗ exception: {e}")` — failure with unicode X

No way to filter by severity, no way to trace a specific delivery across components, no way to aggregate logs in a log management system.

---

### O05 — No `pool_pre_ping` on database engine

| Field | Value |
|---|---|
| **File** | `database.py:9` |
| **Confidence** | **Medium** |
| **Severity** | **Medium** |

**Why it is a bug:**
`create_async_engine(DATABASE_URL, echo=False)` — no `pool_pre_ping=True`. Connections returned to the pool that have been closed/dropped (e.g., network hiccup, database restart, firewall timeout) are not verified before reuse. The first operation on a stale connection raises `InterfaceError` or `OperationalError`, causing the request to fail unnecessarily.

With `pool_pre_ping=True`, SQLAlchemy runs `SELECT 1` before handing out a connection, ensuring it's alive.

---

### O06 — No connection pool timeout on database engine

| Field | Value |
|---|---|
| **File** | `database.py:9` |
| **Confidence** | **Medium** |
| **Severity** | **Medium** |

**Why it is a bug:**
When the pool is exhausted, `async_sessionmaker()` blocks indefinitely waiting for a connection (default `pool_timeout=30` in SQLAlchemy 1.x, but in 2.x with async, the behavior is effectively wait forever). Under load spikes, requests queue up waiting for pool slots. With no timeout, the queue grows unboundedly. HTTP connections to the API also hang.

---

[Back to ToC](#table-of-contents)

---

## 9. Edge Cases & State Handling

### E01 — Worker has no check for `DEAD` status in idempotency guard

| Field | Value |
|---|---|
| **File** | `worker.py:47` |
| **Function** | `deliver()` |
| **Code path** | `if row.status == "DELIVERED": print("already delivered — skipping"); return` |
| **Severity** | **Medium** |
| **Confidence** | **High** |

**Why it is a bug:**
The idempotency check only checks for `DELIVERED` status. If a delivery somehow has `DEAD` status (max attempts reached) but gets re-queued (e.g., reaper reset, manual operation), the worker will NOT skip it. It will attempt delivery again, even though the system already gave up on this delivery after 5 attempts.

Also: `FAILED` is not checked. If a delivery has a status of `FAILED` (not currently a valid status in the code, but the schema accepts any text), it would also be re-processed.

The statuses that should be "terminal" and skipped are: `DELIVERED`, `DEAD`. The statuses that should be processed are: `PENDING`. `IN_FLIGHT` should not be seen by the worker (should only be set by the worker itself).

---

### E02 — IN_FLIGHT UPDATE at line 104-111 has no `WHERE status = 'PENDING'` guard

(R01 — FIXED: IN_FLIGHT UPDATE now has `AND status = 'PENDING'` guard)"

---

### E03 — No validation that `attempt_count` doesn't overflow

`attempt_count` is an `INT` in PostgreSQL (4 bytes, signed, max 2^31-1 = ~2.1 billion). The worker increments it by 1 each time. In practice, MAX_ATTEMPTS=5 prevents more than 5 increments. But if a bug bypasses MAX_ATTEMPTS (e.g., reaper resetting without count check, R01 (now fixed) causing double increments), the count could grow. Not realistic to overflow, but the schema has no explicit bound beyond INT.

---

### E04 — `GET /deliveries/stats` queries ALL rows for status breakdown

| Field | Value |
|---|---|
| **File** | `routers/deliveries.py:72-79` |
| **Function** | `delivery_stats()` |
| **Code path** | `SELECT status, COUNT(*) as count, AVG(attempt_count) FROM webhook_deliveries GROUP BY status` |
| **Severity** | **Medium** |
| **Confidence** | **High** |

**Why it is a bug:**
`GROUP BY status` with no `WHERE` clause scans the ENTIRE `webhook_deliveries` table. With millions of rows, this is a full sequential scan that can take seconds. Each call to `/deliveries/stats` triggers this expensive query.

The query also has no index on `status`, so PostgreSQL must do a sequential scan and then sort/hash for the GROUP BY.

---

### E05 — `GET /deliveries/stats` query for `recent_success_rate` orders by `created_at` with no index

| Field | Value |
|---|---|
| **File** | `routers/deliveries.py:82-88` |
| **Function** | `delivery_stats()` |
| **Code path** | `ORDER BY created_at DESC LIMIT 100` |
| **Severity** | **Medium** |
| **Confidence** | **High** |

No index on `created_at`. `ORDER BY ... DESC LIMIT N` without an index requires a full table sort (or a scan + top-N sort). The `LIMIT 100` means PostgreSQL can use a top-N heap sort (O(N log M) where M=100, N=table size), but it still requires scanning every row.

---

<!-- E06 — FIXED: Batched Redis LPUSH via `*delivery_ids` (fixed as part of H-005) -->



[Back to ToC](#table-of-contents)

---

## 10. Miscellaneous

### M01 — `worker.py` module-level `asyncio.run(main())` prevents importing

| Field | Value |
|---|---|
| **File** | `worker.py:228` |
| **Severity** | **Low** |
| **Confidence** | **High** |

`asyncio.run(main())` at module level executes when the file is imported. Prevents importing `deliver()`, `handle_failure()`, `sign_payload()`, or `calc_backoff()` for testing or re-use. The standard Python idiom would be:
```python
if __name__ == "__main__":
    asyncio.run(main())
```

Same issue in `reaper.py:55` and `init_db.py:61`.

---

### M02 — No `__init__.py` for `routers/` package

The `routers/` directory works without `__init__.py` in Python 3.3+ (namespace packages). But not having it can cause issues with some tooling and is considered non-standard. The imports in `main.py` (`from routers.events import router`) work fine.

---

### M03 — Hardcoded `"webhook_queue"` and `"webhook_retry_queue"` Redis keys

| Field | Value |
|---|---|
| **Files** | `worker.py`, `reaper.py`, `routers/events.py` |
| **Severity** | **Low** |
| **Confidence** | **High** |

Redis key names are hardcoded string literals across 3 files:
- `"webhook_queue"` — main delivery queue
- `"webhook_retry_queue"` — delayed retry queue

Any misspelling in one file would create a separate queue and cause silent data loss. Should be module-level constants.

---

### M04 — Redundant `str()` calls on UUID values

Multiple places call `str(uuid.uuid4())` — `str()` on a UUID object produces the standard 36-char hex string with dashes. asyncpg accepts both `uuid.UUID` objects and their string representations for UUID columns. Passing the `uuid.UUID` directly would be cleaner and avoid unnecessary string allocation.

Affected lines: `routers/events.py:15,36`, `routers/subscriptions.py:39`.

---

### M05 — `json.dumps(CAST(:payload AS jsonb))` — redundant `CAST`

In `routers/events.py:18-19`:
```sql
INSERT INTO events (id, event_type, payload)
VALUES (:id, :event_type, CAST(:payload AS jsonb))
```
The `CAST(:payload AS jsonb)` is explicit JSONB casting. Since the column IS `JSONB`, PostgreSQL would auto-cast from text. This works but the explicit cast is redundant given the column type. Not a bug, just style.

---

## Summary of New Findings

| ID | Severity | Summary | Found in |
|---|---|---|---|---|
<!-- C03 | **Critical** | FIXED: `get_circuit_state` hidden commit flushes session prematurely | `circuit_breaker.py:28-34` -->
| R02 | **High** | Reaper pushes to Redis before commit — phantom jobs | `reaper.py:30-44` |
| R03 | **Medium** | Worker/reaper potential PG deadlock | `worker.py:104`, `reaper.py:34` |
| CB03 | **Medium** | `record_success` wipes accumulated failures unconditionally | `circuit_breaker.py:42` |
| CB07 | **Medium** | `get_circuit_state` returns CLOSED for missing subs | `circuit_breaker.py:17-18` |
| CB08 | **Low** | Server-DB clock drift affects cooldown timing | `circuit_breaker.py:24-25` |
| A01-03 | **Critical** | No auth on any endpoint — total service takeover | All routers |
| S01 | **High** | aiohttp follows redirects → SSRF amplification | `worker.py:119` |
| S02 | **High** | `endpoint_url` accepts internal/private IPs | `routers/subscriptions.py:17` |
| T01 | **High** | Subscription deletion orphans pending deliveries | `routers/subscriptions.py:124-132` |
| T02 | **Medium** | No session rollback on exception in `get_db()` | `database.py:13-15` |
| T03 | **Medium** | Raw exception strings stored in DB (info leak) | `worker.py:156` |
| T04 | **Low** | Double JSON serialization in worker | `worker.py:118,121` |
| O01 | **High** | No graceful SIGTERM handling — interrupted deliveries | `worker.py:228`, `reaper.py:55` |
| O02 | **Medium** | No health check endpoint | All |
| O03 | **Medium** | No request timeout middleware | `main.py` |
| O04 | **Medium** | Print-based logging throughout | Multiple |
| O05 | **Medium** | No `pool_pre_ping` — stale connections | `database.py:9` |
| O06 | **Medium** | No connection pool timeout | `database.py:9` |
| E01 | **Medium** | No DEAD status check in worker idempotency | `worker.py:47` |
| E04 | **Medium** | Stats query scans entire delivery table | `routers/deliveries.py:72-79` |
<!-- E06 | **Medium** | FIXED: No batch Redis push for multi-sub events | `routers/events.py:46-47` -->
| A01 | **Medium** | No pagination on `GET /subscriptions` | `routers/subscriptions.py:57-85` |
| A02 | **Medium** | Non-UUID params cause 500 instead of 400 | Multiple |
| M01 | **Low** | Module-level `asyncio.run()` prevents importing | `worker.py:228`, `reaper.py:55` |

---

*End of second-pass audit. 30+ new issues across 10 categories.*
