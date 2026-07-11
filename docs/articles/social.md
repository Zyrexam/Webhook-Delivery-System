# Social Media Posts — Webhook Delivery System Article

---

## 🐦 Twitter / X Thread (20 posts)

---

**Post 1:**
Building a webhook delivery system sounds simple:
→ Accept event
→ Make HTTP call
→ Done

But the real world has other plans. Networks blink. Workers crash. Subscribers die at 3 AM.

Here are 8 decisions that saved us at 2 AM 🧵👇

**Post 2:**
Decision 1: Don't make the sender wait.

Naive approach: deliver webhooks inside the HTTP request. One slow subscriber = everyone waits. Timeouts cause retries. Retries cause duplicates.

Our fix: Write to Postgres, push to Redis queue, return 202 Accepted instantly. Worker picks it up async.

**Post 3:**
Decision 2: At-least-once delivery.

"Exactly-once" is the holy grail. It's also prohibitively expensive (distributed transactions over HTTP? No thanks).

We chose at-least-once + idempotency check: before processing, check if status == "DELIVERED". If yes, skip.

Stripe, GitHub, SendGrid all do this.

**Post 4:**
Decision 3: Exponential backoff.

Retry immediately = DDoS your subscriber. Wait too long = delayed events.

Our formula: 2^attempt seconds (2s → 4s → 8s → 16s → 32s). Max 5 attempts, then mark DEAD.

The key trick? Redis Sorted Set as a delay queue. ZRANGEBYSCORE is O(log N). Genius.

**Post 5:**
Decision 4: The circuit breaker.

One dead subscriber can consume ALL your worker capacity. Every event triggers 5 retries against a dead endpoint.

3 states: CLOSED → OPEN (after 5 failures) → HALF_OPEN (after 60s cooldown).

No events dropped — just delayed until recovery. Beautiful.

**Post 6:**
Decision 5: The Reaper.

Workers crash. It's not "if" — it's "when." A crashed worker leaves IN_FLIGHT deliveries stuck forever.

Solution: A standalone process that every 30s rescues deliveries stuck in IN_FLIGHT for >60s. Resets to PENDING, re-queues to Redis.

Erlang/OTP supervisor pattern in Python.

**Post 7:**
Decision 6: HMAC-SHA256 signatures.

Anyone who knows a subscriber's URL can send fake webhooks. No auth = no trust.

Per-subscription 64-char hex secret. HMAC-SHA256 signed payloads. Verification uses compare_digest() — NOT == — to prevent timing attacks.

Defense-in-depth matters.

**Post 8:**
Decision 7: Two databases.

Postgres: source of truth. Never loses data.
Redis: fast queue. Might lose data on crash.

The trick: Postgres commit BEFORE Redis push. If Redis crashes, Postgres has the backup. The Reaper rebuilds the queue.

Each plays to its strength.

**Post 9:**
Decision 8: 202 Accepted.

Not 200 OK. Not 201 Created.

202 tells the caller: "We got your event. It's queued. Check back later." It's the standard for async processing. Use it.

**Post 11 (Summary):**
The 8 decisions in one thread:

1️⃣ Async queue, not sync delivery
2️⃣ At-least-once + idempotency
3️⃣ Exponential backoff
4️⃣ Circuit breaker pattern
5️⃣ Crash Reaper process
6️⃣ HMAC-SHA256 signatures
7️⃣ Postgres truth + Redis speed
8️⃣ 202 Accepted status code

---

## 💼 LinkedIn Post

Building a Reliable Webhook Delivery System: The 8 Decisions That Mattered

Sending webhooks is easy. Delivering them reliably is brutally hard.

Networks fail. Workers crash. Subscribers go offline at 3 AM. And somehow, every event needs to arrive exactly when it should.

When we built our webhook delivery system with FastAPI, Redis, and PostgreSQL, we had to make 8 critical engineering decisions — each one a tradeoff between reliability, complexity, and speed:

1️⃣ Async queue (Redis LIST + BRPOP) instead of synchronous delivery
2️⃣ At-least-once with idempotency instead of exactly-once
3️⃣ Exponential backoff (2s→4s→8s→16s→32s) instead of fixed retries
4️⃣ Circuit breaker (CLOSED→OPEN→HALF_OPEN) to protect against dead subscribers
5️⃣ A crash Reaper process to rescue stuck deliveries
6️⃣ HMAC-SHA256 signatures with timing-safe comparison
7️⃣ Dual storage: Postgres for truth, Redis for speed
8️⃣ 202 Accepted for async ingestion

The full deep dive covers each decision — including the problem we faced, the options we considered, and why we chose what we did.


[Link to article]

#Webhooks #SystemDesign #Python #FastAPI #Redis #PostgreSQL #Engineering

---

## 📱 Short-Form Quotes (Instagram / Threads / Bluesky)

**Quote 1:**
"Exactly-once delivery across a network requires distributed transactions. In practice, nobody actually does this. Stripe doesn't. GitHub doesn't. SendGrid doesn't. At-least-once with idempotency solves 99% of the problem."

**Quote 2:**
"One dead subscriber shouldn't kill your whole system. A circuit breaker with 3 states — CLOSED, OPEN, HALF_OPEN — stops hammering dead endpoints and preserves worker capacity for healthy subscribers."

**Quote 3:**
"If Redis crashes, Postgres has the backup. If Postgres is down, you have bigger problems. Two databases, each playing to its strength: one for durability, one for speed."

**Quote 4:**
"Use hmac.compare_digest(), not ==. A normal comparison stops at the first mismatched byte. An attacker can measure response time to guess your signature byte-by-byte. compare_digest always takes the same time."

---

## 📧 Newsletter Blurb (200 words)

**Subject:** The 8 decisions that saved our webhook system at 2 AM

Sending webhooks is easy. Delivering them reliably is brutally hard. Networks blink, workers crash, and subscribers go down at the worst possible times.

When we built our webhook delivery system, we hit 8 design decisions that shaped everything. Here's the short version:

1. **Async queue** — Redis LIST with BRPOP. Never make the sender wait.
2. **At-least-once delivery** — With idempotency checks. Exactly-once is too expensive.
3. **Exponential backoff** — 2^attempt seconds. Give transient failures time to resolve.
4. **Circuit breaker** — Three states. One dead subscriber doesn't kill the system.
5. **Crash Reaper** — A watcher process that rescues stuck deliveries.
6. **HMAC signatures** — With timing-safe comparison. Trust no one.
7. **Dual storage** — Postgres for truth, Redis for speed.
8. **202 Accepted** — The right HTTP status for async workflows.

Read the full article for code snippets and architecture diagrams.

[Link to full article]

---

## 🎯 Hashtags

#Webhooks #SystemDesign #Python #FastAPI #Redis #PostgreSQL #BackendEngineering #SoftwareArchitecture #DistributedSystems #Async #Engineering #Programming #DevOps #CircuitBreaker #HMAC #TechDeepDive
