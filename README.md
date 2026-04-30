# Standalone NestJS Worker Demo (Kafka)

This project is a **standalone worker service** (no dependency on your existing app) to simulate industry-style conversation processing.

## Why Kafka for your requirement

Your requirement is:
- Many workers on same channel/topic.
- Same message must be processed by only one worker.
- If conversation `abc` is being processed, no other worker should process `abc` concurrently.
- Keep ordering for DB writes per conversation.
- Need ACK + retry behavior and horizontal scaling.

Kafka is better than BullMQ for this case because:
1. Native consumer groups guarantee one message goes to one worker instance.
2. Using message `key = conversationId` guarantees all events of one conversation go to same partition, preserving order.
3. Partition ownership guarantees only one consumer in a group reads a partition at a time.
4. Horizontal scaling is strong with partitions + consumer groups.
5. Retries + DLQ patterns are production standard.

BullMQ is excellent for job queues and delayed jobs, but for strict partitioned ordering + stream-scale throughput, Kafka is usually the stronger choice.

## Architecture in this demo

- Main topic: `conversation.events`
- Retry topics: `conversation.events.retry.1s`, `conversation.events.retry.5s`
- DLQ topic: `conversation.events.dlq`
- Partition count target: `100` (configurable via `PARTITIONS`)
- Partition key: `conversationId`
- Consumer group: `conversation-worker-group`
- Manual commit (`autoCommit=false`) = ACK occurs after successful handling or controlled reroute

At startup, the worker creates missing topics and also increases existing topic partitions up to the configured `PARTITIONS` value.

Flow:
1. Worker consumes event from main topic.
2. Simulates DB write.
3. On success: commit offset (ACK).
4. On failure: publish to retry topic and commit original offset.
5. After max retries: publish to DLQ and commit offset.

## Local run

Prerequisites:
- Node.js 20+
- Docker Desktop

### 1) Start Kafka (Redpanda)

```bash
docker compose up -d
```

Kafka endpoint for app is `localhost:19092`.

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

This sends 20 conversations x 100 messages each.

### 5) Stress simulation (one command)

```bash
npm run stress -- 3 30 200
```

Arguments = `<workers> <conversations> <messagesPerConversation>`.

## Scaling guidance

### Horizontal scaling

- Increase number of worker instances.
- Ensure topic partitions >= max active workers for good utilization.
- Keep same `KAFKA_GROUP_ID` for workers of same service.

### Vertical scaling

- Increase CPU/memory per worker pod/VM.
- Keep per-partition processing sequential.
- If needed, increase partitions and instance count instead of high in-process concurrency.

## ACK / Retry best practice notes

- ACK means committing Kafka offset after processing is safely handled.
- At-least-once is default practical guarantee; design handler idempotency by `messageId`.
- For exactly-once effects on DB, use idempotent keys and transaction/outbox style in real systems.

## Future (Go implementation)

This design maps directly to Go clients (`segmentio/kafka-go` or `confluent-kafka-go`) with same partition-key and consumer-group strategy.
