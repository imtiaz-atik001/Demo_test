# Standalone NestJS BullMQ Worker Demo

Industry-grade conversation processing worker built with **NestJS + BullMQ + Redis**.

## Why BullMQ with Distributed Locks for your requirement

Your requirements:
- Many workers on the same queue.
- Same message processed by exactly one worker.
- While conversation `abc` is being processed, no other worker processes `abc` concurrently.
- Keep ordering for DB writes per conversation.
- ACK + retry behavior and horizontal + vertical scaling.

**This demo solves it with:**
1. **BullMQ Queue** — guarantees one job goes to exactly one worker.
2. **Redis Distributed Lock (`SET NX EX`)** — guarantees only one worker holds the lock for a given `conversationId` at any time.
3. **Sequence Tracker (Redis)** — guarantees messages are processed in strict `sequence` order per conversation.
4. **Built-in Retry + DLQ** — BullMQ handles exponential backoff, max retries, and dead-lettering.
5. **Per-conversation parallelism** — different conversations are processed concurrently; same conversation is serialized by the lock.

### Kafka vs BullMQ for this pattern

| Requirement | Kafka | BullMQ (this demo) |
|-------------|-------|-------------------|
| One message → one worker | ✅ Consumer groups | ✅ Queue semantics |
| Conversation exclusivity | ✅ Partition key | ✅ Distributed lock |
| Same conv → same worker | ✅ Partition ownership | ⚠️ Weak affinity (lock-based) |
| Per-conversation ordering | ✅ Within partition | ✅ Sequence tracker |
| In-worker conversation concurrency | ❌ Sequential per partition | ✅ `concurrency` setting |
| Retry / DLQ | Custom retry topics | ✅ Built-in, excellent |
| Horizontal scaling | Add partitions + consumers | Add workers |
| Vertical scaling | Limited by partition count | ✅ Higher concurrency per worker |

**Verdict:** If you need **rich retry/DLQ out of the box** and **higher per-worker concurrency**, BullMQ + distributed locks is a strong pragmatic choice. Kafka is better for extreme throughput streaming; BullMQ is better for job-queue semantics with conversation-level locking.

---

## Architecture

### Queue Design
- `conversation-events` — main queue
- `conversation-events-dlq` — dead letter queue

### Retry Design
- BullMQ native **exponential backoff** (1s, 2s, 4s, 8s...)
- `maxRetries` configurable (default: 5)
- After max retries → moved to DLQ automatically

### Processing Flow
1. Job arrives for `conversation-abc`, `sequence=5`.
2. **Sequence check:** verify `sequence === lastProcessed + 1`.
   - If already processed → idempotent skip.
   - If ahead of order → throw `OutOfOrderError` → retried until predecessor completes.
3. **Lock acquisition:** `SET lock:conversation:abc <workerId> NX EX 30`.
   - If locked by another worker → throw `LockContentionError` → retried.
4. **Process the job** (simulated DB write with configurable delay).
5. On success → update sequence tracker, release lock, ACK (job completes).
6. On failure → release lock, throw error → BullMQ retries.
7. After max retries → move to DLQ.

### Lock Heartbeat
While processing, the worker extends the lock TTL every `LOCK_TTL_SECONDS / 2` seconds so long-running jobs don't lose the lock.

---

## Local Run

Prerequisites:
- Node.js 20+
- Docker Desktop

### 1) Start Redis

```bash
docker compose up -d
```

Redis is available at `localhost:6379`.
Redis Insight UI is available at `http://localhost:5540`.

### 2) Install dependencies

```bash
npm install
```

### 3) Start workers

Open separate terminals and run:

```bash
npm run worker -- worker-1
npm run worker -- worker-2
npm run worker -- worker-3
```

You can start as many workers as you want:

```bash
npm run worker -- worker-4
npm run worker -- worker-5
```

### 4) Produce demo load

```bash
npm run simulate:produce -- 20 100
```

This creates 20 conversations × 100 sequential messages each (2000 jobs).

### 5) Stress simulation (one command)

```bash
npm run stress -- 3 30 200
```

Arguments = `<workers> <conversations> <messagesPerConversation>`.

This will:
1. Spawn 3 worker processes
2. Produce 30 conversations × 200 messages each
3. Wait for the queue to drain
4. Generate a stress report

### 6) View report

```bash
npm run report:consumers
```

This parses worker metrics and queue stats, then prints + saves to `consumer-logs/stress-report.md`.

---

## Configuration (`.env`)

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_URL` | `redis://localhost:6379` | Redis connection string |
| `QUEUE_NAME` | `conversation-events` | Main queue name |
| `DLQ_NAME` | `conversation-events-dlq` | DLQ queue name |
| `PROCESSING_DELAY_MS` | `120` | Simulated DB write latency |
| `MAX_RETRIES` | `5` | Max retry attempts before DLQ |
| `WORKER_CONCURRENCY` | `5` | Concurrent jobs per worker |
| `LOCK_TTL_SECONDS` | `30` | Redis lock TTL (auto-extended while processing) |
| `WORKER_ID` | `worker-<random>` | Unique worker identifier |

---

## Scaling Guidance

### Horizontal Scaling
- Add more worker instances (processes or containers).
- All workers connect to the same Redis + same queue.
- BullMQ automatically distributes jobs across workers.

### Vertical Scaling
- Increase `WORKER_CONCURRENCY` per worker.
- Different conversations are processed in parallel.
- Same conversation remains serialized by the distributed lock.

---

## Observability

### Worker Logs
Each worker logs:
- `START` — when a job begins processing
- `ACK` — successful completion with latency
- `NACK` — failure with reason
- `DLQ` — after max retries exceeded
- Periodic `health` — queue stats (waiting, active, delayed, completed, failed)
- Periodic `stats` — throughput, latency percentiles (p50, p95, p99)

### Metrics Files
`consumer-logs/<worker-id>.jsonl` contains structured JSON lines:
- `type: start` — worker startup
- `type: metrics` — periodic stats snapshot
- `type: end` — worker shutdown

### Report
`consumer-logs/stress-report.md` contains:
- Global queue stats
- Aggregated worker stats
- Per-worker breakdown table (success, fail, retry, DLQ, lock contention, latency)

---

## Future: Go Implementation

This design maps directly to Go:
- **Queue:** Use Redis + a custom job queue (or Go-based BullMQ alternative like `asynq`)
- **Lock:** Redis `SET NX EX` via `go-redis`
- **Sequence Tracker:** Redis `GET`/`SET` via `go-redis`
- **Worker:** Goroutines with a semaphore for concurrency control
