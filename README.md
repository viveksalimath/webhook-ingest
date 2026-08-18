# Webhook Ingestion Service (`webhook-ingest`)

A high-performance, fault-tolerant Go service that receives call-completion webhooks from telephony providers, reliably stores them, processes call recordings asynchronously, and maintains real-time per-account call statistics with strictly idempotent processing.

---

## 📋 Overview of What Was Fixed

Last week's operations report detailed three critical production symptoms:
1. **Duplicate call records & account stats drifting higher than actual calls**
2. **Call recordings never getting marked processed (with zero logs)**
3. **In-flight work disappearing upon deployment/restart**

### Root Causes & Technical Fixes

| Symptom / Defect | Root Cause | Fix Implemented |
| :--- | :--- | :--- |
| **Duplicate accounting on redeliveries** | Check-then-act race (`EventExists` before insert), non-unique `event_id` column, and non-transactional database operations. | Added migration `002_events_unique_event_id.sql` (`UNIQUE INDEX` on `event_id`). Wrapped event insert (`ON CONFLICT DO NOTHING`), call upsert, and account stats increment inside a single PostgreSQL ACID transaction (`Store.IngestEvent`). |
| **Recordings never marked processed** | Background goroutine was passed `r.Context()` from HTTP handler. When HTTP 200 was sent, context was cancelled, causing `MarkRecordingProcessed` to fail with `context canceled`. Error was silently swallowed. | Decoupled background context using a managed worker pool with timeout. Added structured logging (`s.log.Error`) and worker queue. |
| **In-flight work lost on deployment** | Detached goroutines (`go func() { ... }()`) were killed abruptly when `main()` exited after HTTP shutdown. | Implemented graceful shutdown with `sync.WaitGroup` and `Service.Shutdown(ctx)` to drain in-flight recording jobs when `SIGTERM`/`SIGINT` is received. |
| **Concurrent map read/write data race** | `stats.Cache.Record` mutated `c.m` without holding `c.mu.Lock()`. | Synchronized map operations with `c.mu.Lock()` / `defer c.mu.Unlock()` to ensure thread-safety under heavy concurrent load. |
| **Cold-cache returning zeroes after restart** | `stats.Cache` started empty on startup with no read-through fallback. | Added cold-cache read-through in `Service.Stats` that queries durable numbers from PostgreSQL `account_stats` on cache misses. |

---

## 🚀 How to Run

### Prerequisites
- **Go 1.25+** (or Docker & Docker Compose)
- **PostgreSQL 16**
- **Redis 7**

---

### Option A: Running with Docker Compose (Recommended)

Start PostgreSQL, Redis, and the service in containers:
```bash
docker compose up -d --build
```

To wipe volumes, apply all migrations (`001_init.sql` & `002_events_unique_event_id.sql`), and start fresh:
```bash
make reset
```

Check service health:
```bash
curl http://localhost:8080/healthz
# -> ok
```

---

### Option B: Running Locally

If you are running Postgres and Redis locally on your host:

1. **Configure Environment Variables:**
   ```bash
   cp .env.example .env
   ```
   *(Default: `DATABASE_URL=postgres://webhook:webhook@localhost:5432/webhook?sslmode=disable`, `REDIS_ADDR=localhost:6379`, `HTTP_ADDR=:8080`)*

2. **Apply Migrations:**
   ```bash
   psql -U webhook -d webhook -f migrations/001_init.sql
   psql -U webhook -d webhook -f migrations/002_events_unique_event_id.sql
   ```

3. **Start the Service:**
   ```bash
   go run ./cmd/server
   ```

---

## 🧪 Running Tests

The test suite runs with Go's race detector enabled and includes comprehensive regression tests for concurrent duplicate deliveries, async recording processing, graceful worker pool shutdown, and cold cache read-through:

```bash
# Run all tests with race detector
go test -v -count=1 -race ./...
```

---

## 📊 Live Operations & Test Dashboard

An interactive operations dashboard is embedded into the service:

👉 Open **[http://localhost:8080/](http://localhost:8080/)** or **[http://localhost:8080/dashboard](http://localhost:8080/dashboard)** in your browser.

### Dashboard Features:
- **Live System KPI Metrics**: Total Ingested Events, Unique Calls, Processed Recordings, Total Duration, Active Accounts.
- **Interactive Webhook Simulator**:
  - *Send Ingestion Webhook*: Ingest a single call event.
  - *Send Redelivery (Idempotency Test)*: Send duplicate delivery and verify zero double counting.
  - *Fire 20 Concurrent Duplicates (Race Test)*: Blast 20 simultaneous duplicate requests in parallel.
- **Account Stats Consistency Inspector**: Inspect real-time account stats and verify in-memory vs database consistency.
- **Real-Time Live Call Feed**: Auto-refreshing table displaying calls, durations, and live updating recording transcoding badges.

---

## 📡 API Reference

### 1. Ingest Call Webhook
`POST /webhooks/calls`

```bash
curl -X POST http://localhost:8080/webhooks/calls \
  -H "Content-Type: application/json" \
  -d '{
    "event_id": "evt_01H8XK2M9P",
    "call_id": "call_9f2ab31c",
    "account_id": "acc_123",
    "status": "completed",
    "duration_sec": 143,
    "recording_url": "https://recordings.example.com/9f2ab31c.wav",
    "occurred_at": "2026-08-18T09:12:00Z"
  }'
```
**Response (`200 OK`):**
```json
{
  "status": "accepted"
}
```
*Note: Redeliveries of the same `event_id` return `200 OK` idempotently without modifying database records or double-incrementing statistics.*

---

### 2. Get Account Statistics
`GET /accounts/{account_id}/stats`

```bash
curl http://localhost:8080/accounts/acc_123/stats
```
**Response (`200 OK`):**
```json
{
  "account_id": "acc_123",
  "call_count": 1,
  "total_duration_sec": 143
}
```

---

### 3. Health Check
`GET /healthz`

```bash
curl http://localhost:8080/healthz
# Output: ok
```

---

## 📂 Project Layout

```
cmd/server/           Entrypoint and graceful shutdown lifecycle wiring
internal/config/      Environment configuration parser
internal/store/       PostgreSQL repository with atomic transaction support
internal/stats/       Thread-safe in-memory per-account statistics cache
internal/ingest/      Webhook ingestion service and async worker pool
internal/httpapi/     HTTP router, handlers, and embedded live dashboard
internal/redisclient/ Redis client connection helper
internal/testutil/    Shared test harness and database cleanup helpers
migrations/           Database migrations (001_init.sql, 002_events_unique_event_id.sql)
SOLUTION.md           Detailed architectural report and 10k/sec scaling design
```

---

## 📖 Design Decisions & Scaling Report

For a detailed analysis of all defects, defense of PostgreSQL ACID transactions over Redis deduplication, and architectural blueprints for scaling to 10,000 webhooks/sec, please refer to:

👉 **[`SOLUTION.md`](./SOLUTION.md)**
