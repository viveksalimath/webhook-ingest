# Engineering Solution & Architecture Report

## 1. What Was Broken, and Why

### 1.1. Check-then-Act Race & Non-Atomic Ingestion (Duplicate Records & Account Drifts)
* **Symptom:** Duplicate call records in the dashboard, and account call-counts drifted higher than the actual number of calls.
* **Root Cause:**
  1. `Service.Ingest` used a "check-then-act" pattern: calling `s.store.EventExists` before inserting. Under concurrent webhook deliveries for the same `event_id`, all concurrent requests observed `exists == false`.
  2. The `events` database table lacked a `UNIQUE` constraint or unique index on `event_id` (only a standard non-unique index existed in `001_init.sql`).
  3. Database operations (`InsertEvent`, `UpsertCall`, `IncrementAccountStats`) were executed as three independent, non-transactional statements.
  4. Consequently, concurrent deliveries inserted duplicate event rows, executed `IncrementAccountStats` multiple times, and called `s.cache.Record` multiple times, causing double counting. Furthermore, partial failures left the database in inconsistent states.

### 1.2. HTTP Request Context Passed to Background Goroutines (Recordings Never Processed)
* **Symptom:** Calls landed, but their recordings never got marked processed with zero error logs.
* **Root Cause:**
  1. In `Service.Ingest`, the background goroutine spawned for recording transcoding was passed `ctx` directly from `r.Context()`.
  2. As soon as the HTTP handler returned `200 OK`, `net/http` cancelled the request context.
  3. `processRecording` simulated transcoding work with `time.Sleep(50 * time.Millisecond)`. By the time `s.store.MarkRecordingProcessed(ctx, rec.CallID)` executed, `ctx.Err()` was `context.Canceled`, causing `pgx` to abort the SQL query immediately.
  4. The returned error was discarded in an empty `if err != nil {}` block without logging, masking the failure completely.

### 1.3. Uncoordinated Background Tasks on Deployment (In-Flight Work Disappeared)
* **Symptom:** Every time the service deployed, in-flight work disappeared.
* **Root Cause:**
  1. Background tasks were launched as detached goroutines (`go func() { ... }()`) with no lifecycle tracking or wait groups.
  2. On `SIGTERM`/`SIGINT`, `main()` called `srv.Shutdown()` (which only waits for active HTTP connections to close) and exited immediately.
  3. Any asynchronous recording jobs currently transcoding or queued were abruptly terminated when the process was killed.

### 1.4. Concurrent Data Race in In-Memory Stats Cache
* **Symptom:** Potential runtime crash (`fatal error: concurrent map writes`) and corrupted statistics under concurrent load.
* **Root Cause:** `Cache.Record` mutated the underlying map `c.m` and pointer values without acquiring `c.mu.Lock()`, racing with readers using `Cache.Get` (`c.mu.RLock()`).

### 1.5. Cold-Start Cache Desynchronization
* **Symptom:** On service restart or deployment, `GET /accounts/{id}/stats` returned zeroes despite historical data existing in Postgres.
* **Root Cause:** `Cache` initialized with an empty map and had no read-through fallback to the durable `account_stats` database table.

---

## 2. Deduplication Strategy: Defense & Comparison

### Chosen Strategy: Database-Level Uniqueness with Atomic ACID Transaction
1. Added migration `002_events_unique_event_id.sql` creating `UNIQUE INDEX idx_events_event_id_unique ON events (event_id)`.
2. Encapsulated ingestion in `Store.IngestEvent` within a single Postgres transaction (`pgx.Tx`):
   - `INSERT INTO events (...) VALUES (...) ON CONFLICT (event_id) DO NOTHING RETURNING id;`
   - If no ID is returned (`pgx.ErrNoRows`), the transaction commits/aborts cleanly and returns `inserted = false`. The service logs `"duplicate delivery ignored"` and returns an idempotent `200 OK` without touching `calls`, `account_stats`, or spawning recording jobs.
   - If inserted (`inserted = true`), the call is upserted and `account_stats` is incremented within the same transaction.

### Why This Approach Over Alternatives:
* **Vs. Application-Level Locking / Mutexes:** In-memory or application mutexes only work on a single instance. In a production multi-replica deployment behind a load balancer, concurrent duplicate deliveries land on different pods, bypassing local locks entirely.
* **Vs. Redis Deduplication (`SET NX EX`):**
  - *Dual-Write Failure / False Deduplication:* If a key is set in Redis (`SET event_id 1 NX EX 86400`) and the subsequent Postgres write fails (e.g. database timeout, deadlock, or crash), the HTTP request returns 500. When the telephony provider retries, Redis rejects the retry as a duplicate, resulting in **permanent data loss**.
  - *Eviction & Expiration Risks:* Under memory pressure or after TTL expiry, Redis keys can be evicted, leading to duplicate writes if late redeliveries occur.
  - *Lack of Cross-System Atomicity:* Redis cannot participate in an ACID transaction with PostgreSQL without complex Two-Phase Commit (2PC) or Saga orchestrators.
* **Conclusion:** PostgreSQL serves as the authoritative, durable source of truth. Using a database unique constraint with atomic transactions guarantees **strictly exactly-once side-effects under at-least-once delivery**.

---

## 3. Scaling to 10,000 Webhooks / Second

Handling 10,000 webhooks/sec (~864 million events/day) requires transitioning from synchronous relational writes to an asynchronous, stream-oriented architecture:

```
[ Telephony Provider ]
         │
         ▼ (HTTPS POST)
[ Ingest Edge Layer (Stateless Go API Gateway) ]
         │
         ├─► [ Redis Fast-Path Filter (SET NX EX 3600) ] (Drops immediate hot retries)
         │
         └─► [ Distributed Log (Apache Kafka / AWS Kinesis / Redis Streams) ]
                   │ (Partitioned by account_id)
                   ▼
         [ Scalable Worker Pool (Consumer Group) ]
                   │
                   ├─► Micro-Batch DB Inserts (pgx.CopyFrom / batch unnest)
                   ├─► Real-Time Counters in Redis (HINCRBY)
                   └─► Async Recording Job Queue (S3 + Temporal / Celery)
```

1. **Decouple Ingestion from Database Writes (Distributed Message Queue):**
   - The edge HTTP ingest service performs only payload validation and publishes the event to a partitioned message broker (e.g., Apache Kafka, Apache Pulsar, or AWS Kinesis).
   - Ingest latency drops to <5ms, returning `202 Accepted` immediately without blocking on relational database I/O.

2. **Partitioning by `account_id`:**
   - Partitioning the message stream by `account_id` ensures that all events for a specific account are processed sequentially by a dedicated worker. This completely eliminates database row-lock contention on `account_stats`.

3. **Micro-Batching Database Writes:**
   - Consumer workers buffer events in memory for 50–100ms or up to 500 items, then flush to Postgres using `pgx.CopyFrom` or multi-row `INSERT ... ON CONFLICT DO NOTHING` statements. This reduces database write IOPS by over 95%.

4. **Tiered Caching & Counter Aggregation:**
   - Maintain hot account statistics in Redis via atomic `HINCRBY`.
   - The `/accounts/{id}/stats` endpoint reads directly from Redis with sub-millisecond latency.
   - A background sync process periodically checkpoints durable aggregated counts into Postgres.

5. **Decoupled Heavy Processing & DLQ:**
   - Recording transcoding jobs are offloaded to object storage (S3/GCS) and managed via a distributed workflow engine (e.g., Temporal or Celery workers) with Dead Letter Queues (DLQ) and exponential backoff retry policies.
