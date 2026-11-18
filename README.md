# ChatFlow — A Distributed Chat System

ChatFlow is a real-time, room-based chat system built for CS6650 (Distributed Systems). Clients connect over WebSocket. Messages go through a message queue to a consumer tier, which pushes them back out to every client in the room and saves them to PostgreSQL. An analytics API reads from the database.

This repo holds two generations of the system:

| Folder | Design | Stack |
|---|---|---|
| [ChatFlow-4-(new design)/](ChatFlow-4-(new%20design)/) | **New design (current).** Spring Boot, AWS SQS FIFO, async broadcast workers, room-partitioned consumers | Java 17, Spring Boot, AWS SQS, ALB, RDS PostgreSQL 17 |
| [ChatFlow-3(old design)/](ChatFlow-3(old%20design)/) | **Old design.** Plain Java servers, RabbitMQ topic exchange, Redis deduplication | Java, Java-WebSocket, Jetty, RabbitMQ, Redis, RDS PostgreSQL |

---

## Table of Contents

1. [Architecture Comparison](#1-architecture-comparison)
2. [Architecture — ChatFlow-4 (New Design)](#2-architecture--chatflow-4-new-design)
3. [Architecture — ChatFlow-3 (Old Design)](#3-architecture--chatflow-3-old-design)
4. [ChatFlow-4 Architecture Walkthrough](#4-chatflow-4-architecture-walkthrough)
5. [Repository Layout](#5-repository-layout)

---

## 1. Architecture Comparison

### At a glance

| Concern | ChatFlow-3 (old) | ChatFlow-4 (new) |
|---|---|---|
| **Server framework** | Raw `Java-WebSocket` server (port 8080) plus an embedded Jetty HTTP server (port 8081) in one process | One Spring Boot app on port 8080 serving the WebSocket, internal broadcast, health and `/metrics` endpoints |
| **Message broker** | Self-hosted RabbitMQ on EC2: one topic exchange `chat.exchange`, 20 durable queues `room.1..20` | Managed AWS SQS: 20 FIFO queues `chatflow-room-01..20.fifo`, `MessageGroupId = roomId` |
| **Publish path** | Synchronous `basicPublish` on the WebSocket thread, using a pooled channel | Asynchronous publish on a 20-thread pool. The client gets a `RECEIVED` ack right away. A Resilience4j circuit breaker and an in-memory DLQ protect SQS |
| **Client ack** | No success ack; errors only | `{"status":"RECEIVED","messageId":...}` for every accepted message |
| **Deduplication** | Redis `SET NX EX` on `seen:<messageId>` (24 h TTL) | No Redis. SQS FIFO `MessageDeduplicationId` plus `INSERT … ON CONFLICT (message_id) DO NOTHING` |
| **Consumer threading** | N AMQP threads, rooms assigned round-robin, `basicQos(1)`. Broadcast runs **inline** with up to 3 retries before the ack | One SQS long-poll thread per room. Broadcast is **decoupled** into per-room in-memory queues drained by 4 workers per room |
| **Broadcast fan-out** | Consumer POSTs to **one** configured server URL | Consumer POSTs to **every** server in parallel (`CompletableFuture`), so clients on any ALB target receive the message |
| **Server-side broadcast** | Loop over every connection in the request thread | Per-room single-thread executor (bounded to 500) keeps room order; `ConcurrentWebSocketSessionDecorator` makes concurrent sends safe |
| **Horizontal scaling** | One consumer process polls all 20 rooms | Room-partitioned consumers: `--app.consumer.room-start=11` gives C1 rooms 1–10 and C2 rooms 11–20, with no coordination |
| **DB write path** | `DbWriterPool`: JDBC `addBatch` (batch 100, flush 100 ms) plus a `user_room_activity` upsert in the same transaction | `DbWriterService`: write-behind queue (100K) feeding 5 writers, each issuing one **multi-row `VALUES`** INSERT per batch, with retry and linear backoff |
| **Schema** | `messages` (VARCHAR ids, TIMESTAMPTZ) plus a `user_room_activity` table | A single `messages` table with compact types (`SMALLINT room_id`, `INT user_id`, `BIGINT sent_at` epoch-ms) and 4 covering indexes |
| **Analytics API** | `/metrics` on port 8081 runs full-table aggregates **sequentially**, with no time window | `GET /metrics?roomId&userId&startTime&endTime&topN` runs **8 queries in parallel** and serves Q1–Q4 from index-only scans where possible |
| **Observability** | Consumer `/health` on 8081 and `/stats/report` on 8083 | Consumer `/health` exposes `sqs.*`, `broadcast.*`, `db.*` and `stats.*` (msg/s over 1/10/60 s windows, top rooms and users). Collected by `monitoring/*.sh` |
| **Infrastructure** | EC2 for the servers, consumer, RabbitMQ and Redis; RDS; ALB | EC2 for 2 servers and 2 consumers; managed SQS; RDS; ALB with sticky sessions (no broker or cache hosts to run) |

### Why the redesign

1. **Broadcast blocked consumption.** In ChatFlow-3, and in the first SQS version, the consumer thread waited on the HTTP broadcast before it acknowledged each message. Consumption was therefore capped at roughly `1 / broadcast latency` per thread, and backlog built up in the broker (more than 10K messages per room queue). ChatFlow-4 moves the broadcast onto worker threads. The poll thread only does O(1) enqueues before it deletes from SQS.
2. **Broadcast went to only one server.** The ChatFlow-3 consumer called a single `part1.broadcast.url`. Behind an ALB with 2 or more servers, clients connected to the other servers never saw the message. ChatFlow-4 fans out to all servers in parallel.
3. **Fewer moving parts.** Managed SQS FIFO replaces the RabbitMQ and Redis hosts. Per-room ordering comes from message groups. Database idempotency replaces the Redis seen-set.
4. **One consumer process limited scaling.** Room partitioning lets consumers scale out with the same jar and no coordination. This removed the CPU contention that made 80 broadcast threads on one t3.micro counterproductive.
5. **Reads were expensive.** The old `/metrics` scanned the whole table sequentially. The new endpoint runs bounded-window queries in parallel on purpose-built covering indexes.

### Trade-offs ChatFlow-4 accepts

- **Best-effort ordering of broadcasts within a room.** With 4 workers per room, HTTP delivery order is not guaranteed. Each message carries `sent_at` so clients can sort. Server-side room executors still serialize whatever order they receive.
- **The SQS message is deleted after in-memory enqueue, not after the DB commit.** If a consumer crashes, messages still in its broadcast or DB buffers are lost. The design favors throughput over strict durability.
- **The server acks before SQS confirms.** A server crash while messages wait in the in-memory DLQ loses them.
- **Broadcast drops under overload.** Per-room broadcast queues use `offer()`. When full, the real-time push is dropped (`broadcast.failed`), but the DB write still happens.

### Measured results

| | ChatFlow-3 (RabbitMQ) | ChatFlow-4: A3 baseline (SQS, inline broadcast) | ChatFlow-4: A4 (O1 + O2) |
|---|---|---|---|
| 500K messages | ~2,040 msg/s DB-write rate, degrading under sustained load (1M run averaged ~1,394 msg/s; 30-min endurance fell to ~600 msg/s) | **990 msg/s** end-to-end (505 s) | **1,684 msg/s** end-to-end (297 s), **1.70×** |
| Broker backlog | n/a | ~10K per room queue (~200K total) | Single digits per queue |
| Broadcast drop rate | n/a | n/a | ~0% (18 cold-start connect timeouts out of 500K) |
| DB success | 100% | 100% | 100% |

> The ChatFlow-3 and ChatFlow-4 numbers come from different infrastructure (us-east-1 vs us-west-2, different instance sizes) and different measurement points, so compare them only directionally. The A3 → A4 comparison within ChatFlow-4 is like-for-like. Sources: [ChatFlow-3 load_test_results.md](ChatFlow-3(old%20design)/load-tests/load_test_results.md), [ChatFlow-4 A4-review.md](ChatFlow-4-(new%20design)/A4-review.md), [ChatFlow-4 load-tests/REPORT.md](ChatFlow-4-(new%20design)/load-tests/REPORT.md).

---

## 2. Architecture — ChatFlow-4 (New Design)

```
                         ┌──────────────────────────────┐
                         │  Load-test client (client-v3) │  40 warmup + 120 main sender threads
                         │  or JMeter (WS + HTTP mix)    │
                         └──────────────┬───────────────┘
                     ws://…/chat/{room} │ ▲  RECEIVED ack / broadcast frames
                     GET  /metrics      │ │
                                        ▼ │
                         ┌──────────────────────────────┐
                         │   AWS ALB (sticky sessions)   │  idle timeout 300 s, health check /health
                         └───────┬──────────────┬───────┘
                                 │              │
                 ┌───────────────▼───┐      ┌───▼───────────────┐
                 │ EC2-S1 server-v3  │      │ EC2-S2 server-v3  │   Spring Boot :8080
                 │ ChatWebSocketHandler     │                   │   - validate, ack, register room session
                 │ SqsPublisher (async, CB+DLQ)                 │   - async SQS publish (20 threads)
                 │ InternalBroadcastController (20 room execs)  │   - /internal/broadcast/{roomId}
                 │ MetricsController (8 parallel queries)       │   - /metrics (read side)
                 └───────┬───────────┘      └───────┬───────────┘
                         │ SendMessage               │
                         │ group=roomId, dedup=UUID  │
                         ▼                           ▼
         ┌────────────────────────────────────────────────────────────┐
         │        AWS SQS — 20 FIFO queues chatflow-room-01..20.fifo   │
         └──────────────┬──────────────────────────────┬──────────────┘
           rooms 01–10  │ long-poll (20 s, 10 msgs)     │  rooms 11–20
                        ▼                              ▼
     ┌──────────────────────────────────┐ ┌──────────────────────────────────┐
     │ EC2-C1 consumer-v4  :8081        │ │ EC2-C2 consumer-v4  :8081        │
     │  room-start=1, threads=10        │ │  room-start=11, threads=10       │
     │                                  │ │                                  │
     │  SqsConsumerService (1 thr/room) │ │  (identical jar, different args) │
     │   ├─► BroadcastWorkerService     │ │                                  │
     │   │    20 room queues (cap 5000) │ │                                  │
     │   │    × 4 workers/room          │ │                                  │
     │   │    └─► BroadcastClient ──────┼─┼──► POST /internal/broadcast/{r}  │
     │   │         (parallel to S1, S2) │ │     to EVERY server              │
     │   ├─► DbWriterService            │ │                                  │
     │   │    LinkedBlockingQueue(100K) │ │                                  │
     │   │    5 writers, multi-row INSERT│ │                                 │
     │   ├─► StatsAggregatorService     │ │                                  │
     │   └─► SQS DeleteMessage          │ │                                  │
     └───────────────┬──────────────────┘ └───────────────┬──────────────────┘
                     │ INSERT … ON CONFLICT DO NOTHING     │
                     ▼                                     ▼
              ┌──────────────────────────────────────────────────┐
              │  RDS PostgreSQL 17 (db.t3.micro, private VPC)     │
              │  messages + 4 covering indexes                    │◄── server-v3 /metrics reads
              └──────────────────────────────────────────────────┘
```

**Components**

| Component | Code | Role |
|---|---|---|
| server-v3 | [ChatFlow-4-(new design)/server-v3/](ChatFlow-4-(new%20design)/server-v3/) | WebSocket ingress, SQS producer, internal broadcast endpoint, analytics read API |
| consumer-v4 | [ChatFlow-4-(new design)/consumer-v4/](ChatFlow-4-(new%20design)/consumer-v4/) | SQS consumer, async broadcast workers, write-behind DB pipeline, live stats |
| client-v3 | [ChatFlow-4-(new design)/client-v3/](ChatFlow-4-(new%20design)/client-v3/) | Multithreaded WebSocket load generator; calls `/metrics` at the end of a run |
| database | [ChatFlow-4-(new design)/database/](ChatFlow-4-(new%20design)/database/) | `schema.sql`, `indexes.sql`, `setup.sh`, `verify.sql`, [DESIGN.md](ChatFlow-4-(new%20design)/database/DESIGN.md) |
| load-tests | [ChatFlow-4-(new design)/load-tests/](ChatFlow-4-(new%20design)/load-tests/) | Batch tuning, 500K/1M runs, JMeter baseline and stress plans plus HTML reports |
| monitoring | [ChatFlow-4-(new design)/monitoring/](ChatFlow-4-(new%20design)/monitoring/) | Scripts that poll consumer `/health` and RDS CloudWatch metrics into CSV |

---

## 3. Architecture — ChatFlow-3 (Old Design)

```
                    ┌───────────────────────────────┐
                    │ Load-test client (server-2/    │  ConnectionPool (1 WS per room),
                    │ client): SenderWorker ×64,     │  RetryWorker ×4, MetricsCollector
                    │ warmup 25 × 200                │
                    └───────────────┬───────────────┘
                                    │ ws://…:8080/chat/{roomId}
                                    ▼
                    ┌───────────────────────────────┐
                    │          AWS ALB              │
                    └───────┬───────────────┬───────┘
                            ▼               ▼
        ┌──────────────────────────────┐   ┌──────────────────────────────┐
        │ Server instance (server-2)   │   │ Server instance N             │
        │  ChatWebSocketServer :8080   │   │  (same)                       │
        │   parse → validate →         │   └──────────────────────────────┘
        │   MessageEnvelope(UUID,…)    │
        │   basicPublish (sync,        │
        │   pooled channels, persistent)│
        │  Jetty HttpServerManager :8081│
        │   /internal/broadcast        │◄─────────────────────────────┐
        │   /health  /metrics (DB)     │                              │
        └──────────────┬───────────────┘                              │
                       │ routing key room.<id>                        │
                       ▼                                              │
        ┌────────────────────────────────────────────┐                │
        │ RabbitMQ (EC2)  chat.exchange (topic)       │                │
        │  queues room.1 … room.20 (durable,          │                │
        │  TTL 600 s, max 100K, reject-publish)        │                │
        └──────────────┬─────────────────────────────┘                │
                       │ basicConsume, manual ack, basicQos(1)        │
                       ▼                                              │
        ┌────────────────────────────────────────────┐                │
        │ consumer-v3 (single process)                │                │
        │  ConsumerPool: N ConsumerThreads,           │                │
        │  rooms assigned round-robin                 │                │
        │   ├─► DeduplicationService ── Redis (EC2)   │                │
        │   │     seen:<id>  SET NX EX 86400          │                │
        │   ├─► RoomManager.process()  (blocking)     │                │
        │   │     ├─► WebSocketBroadcaster ───────────┼── HTTP POST ───┘
        │   │     │     (ONE server URL, retry ×3)    │
        │   │     ├─► DbWriterPool (10 thr, q 500K)   │
        │   │     │     JDBC addBatch(100) + activity upsert
        │   │     └─► StatsAggregatorPool (4 thr)     │
        │   └─► ACK / NACK(requeue) / DISCARD         │
        │  HealthCheckServer :8081  StatsServer :8083 │
        └──────────────┬─────────────────────────────┘
                       ▼
        ┌────────────────────────────────────────────┐
        │ RDS PostgreSQL                              │
        │  messages  +  user_room_activity            │
        │  idx (room,ts) (user,ts) (ts) (type,ts)     │
        └────────────────────────────────────────────┘
```

**Old-design issues found during the review**

- `HttpServerManager` builds a **new, empty** `ConcurrentHashMap` and passes it to `BroadcastServlet` in place of the WebSocket server's live `roomMapping`, so `/internal/broadcast` finds no sessions ([HttpServerManager.java](ChatFlow-3(old%20design)/server-2/server/src/main/java/http/HttpServerManager.java)).
- The consumer broadcasts to a single server URL, so in a multi-server deployment some clients miss messages.
- The broadcast and its retries (3 × 500 ms) run before `basicAck` with `basicQos(1)`. One slow server stalls every room assigned to that consumer thread.
- Database and RabbitMQ credentials are hard-coded in source and config files. Move them to environment variables before sharing the repo.

---

## 4. ChatFlow-4 Architecture Walkthrough

This section follows one message from a client keystroke to a stored row and a broadcast frame, then covers the read path.

### Step 1 — Client connects and sends

The client opens `ws://<alb>/chat/{roomId}`. The ALB uses a sticky LB cookie, so a session stays on one server instance. Each frame is JSON:

```json
{"userId":"42","username":"alice","message":"hello","timestamp":"2026-04-06T04:05:28Z","messageType":"TEXT","roomId":5}
```

In load tests, [ChatClient.java](ChatFlow-4-(new%20design)/client-v3/src/main/java/com/chatflow/client/ChatClient.java) runs a warmup phase (40 threads × 1,000 messages) and then a main phase (120 threads). A single `MessageGenerator` thread fills per-room queues.

### Step 2 — Server validates, registers the session and acks

[ChatWebSocketHandler.java](ChatFlow-4-(new%20design)/server-v3/src/main/java/com/chatflow/server/handler/ChatWebSocketHandler.java)

1. On connect, the raw session is wrapped in a `ConcurrentWebSocketSessionDecorator` (30 s send limit, 512 KB buffer, `DROP` overflow). The client's ack and the consumer's broadcasts arrive on different threads, and the decorator serializes them without manual locks. A slow client loses frames rather than being disconnected.
2. The frame is parsed and validated: `userId` 1–100000, `username` 3–20 alphanumeric characters, `message` 1–500 characters, `roomId` 1–20, and `messageType ∈ {TEXT, JOIN, LEAVE}`.
3. **Room membership:** `JOIN` and `TEXT` register the session in `roomSessions[roomId]`. `LEAVE` removes it.
4. A `QueueMessage` is built with a server-assigned UUID `messageId`, `serverId` and `clientIp`.
5. The server sends `{"status":"RECEIVED","messageId":…}` **before** publishing to SQS, which keeps SQS latency (10–30 ms) off the client's critical path.
6. It then calls `sqsPublisher.publishAsync(msg)`.

### Step 3 — Async, circuit-protected publish to SQS

[SqsPublisher.java](ChatFlow-4-(new%20design)/server-v3/src/main/java/com/chatflow/server/sqs/SqsPublisher.java)

- The publish runs on a 20-thread executor and sends to `chatflow-room-{RR}.fifo` with `MessageGroupId = roomId` (ordered per room) and `MessageDeduplicationId = messageId` (SQS-level dedup within 5 minutes).
- A **Resilience4j circuit breaker** wraps it. If more than 50% of the last 10 calls fail, the circuit opens for 30 s, then allows 3 half-open trial calls.
- The fallback places the message in an **in-memory DLQ** (capacity 10,000). A background thread retries the DLQ every 5 s and stops draining at the first failure.

### Step 4 — Consumer polls its room slice

[SqsConsumerService.java](ChatFlow-4-(new%20design)/consumer-v4/src/main/java/com/chatflow/consumer/SqsConsumerService.java)

- Each consumer owns the rooms `roomStart … roomStart + threads - 1`, with **one long-poll thread per room** (`waitTimeSeconds=20`, up to 10 messages). Because exactly one consumer polls each FIFO queue, consumers never compete and need no coordination. This is **Optimization 2: horizontal scaling by room partitioning**.

  ```bash
  java -jar consumer-1.0.0.jar --app.consumer.threads=10                          # C1: rooms 1–10
  java -jar consumer-1.0.0.jar --app.consumer.threads=10 --app.consumer.room-start=11  # C2: rooms 11–20
  ```

- For each message, the poll thread does only **O(1) work**:
  1. `broadcastWorkerService.enqueue(roomId, body)` (non-blocking `offer`)
  2. `dbWriterService.enqueue(msg)` (`put` into the write buffer)
  3. `statsAggregator.record(msg)` (lock-free counters)
  4. `sqsClient.deleteMessage(...)`

### Step 5 — Async broadcast workers fan out to every server

[BroadcastWorkerService.java](ChatFlow-4-(new%20design)/consumer-v4/src/main/java/com/chatflow/consumer/BroadcastWorkerService.java) and [BroadcastClient.java](ChatFlow-4-(new%20design)/consumer-v4/src/main/java/com/chatflow/consumer/BroadcastClient.java)

This is **Optimization 1: decoupling the broadcast from SQS consumption**.

- There are 20 per-room `LinkedBlockingQueue<BroadcastTask>` queues (capacity 5,000 each), each drained by `workers-per-room` threads (default 4).
- Sizing: one worker sustains about 31 msg/s at about 32 ms of HTTP round-trip, and per-room arrival is about 102 msg/s, so ⌈102/31⌉ = 4 workers.
- Each worker calls `BroadcastClient.broadcast()`, which POSTs to `/internal/broadcast/{roomId}` on **all** configured servers at once (`HttpClient.sendAsync` with a 200 ms connect timeout and 2 s request timeout). The broadcast counts as successful if any server returns 200.
- If a room queue is full, `offer()` fails and `broadcast.failed` is incremented. The message is still persisted.

Why two consumers rather than more threads on one: on a 2-vCPU t3.micro, 80 blocking broadcast workers plus 20 pollers caused heavy context switching. Effective HTTP latency rose from about 32 ms to about 130 ms and the drop rate stayed at 58%. Splitting the work across two EC2s (40 workers each) brought the drop rate to about 0%.

### Step 6 — Server pushes to room members, in order

[InternalBroadcastController.java](ChatFlow-4-(new%20design)/server-v3/src/main/java/com/chatflow/server/controller/InternalBroadcastController.java)

- The endpoint returns `200 OK` immediately and hands the work to that room's **single-thread executor** (20 executors, each with a bounded queue of 500 and `DiscardPolicy`). Each room keeps its order on the server, different rooms run concurrently, and memory stays bounded.
- The executor calls `ChatWebSocketHandler.broadcastToRoom()`, which iterates the room's `CopyOnWriteArrayList` of decorated sessions and sends the frame.

### Step 7 — Write-behind persistence

[DbWriterService.java](ChatFlow-4-(new%20design)/consumer-v4/src/main/java/com/chatflow/consumer/db/DbWriterService.java) and [MessageRepository.java](ChatFlow-4-(new%20design)/consumer-v4/src/main/java/com/chatflow/consumer/db/MessageRepository.java)

- Writes go through a shared `LinkedBlockingQueue<QueueMessage>` (capacity 100,000) consumed by 5 writer threads. Each writer runs this loop:
  - `poll(flushIntervalMs = 500)` waits for the first message,
  - `drainTo(batch, batchSize - 1)` takes whatever else is already queued,
  - and the batch is written as **one multi-row INSERT**:

    ```sql
    INSERT INTO messages (message_id, room_id, user_id, username, message, message_type, server_id, client_ip, sent_at)
    VALUES (?,?,?,?,?,?,?,?,?), (?,?,?,?,?,?,?,?,?), …
    ON CONFLICT (message_id) DO NOTHING
    ```

- A multi-row `VALUES` statement is one statement, one round-trip and one WAL flush per batch. `ON CONFLICT DO NOTHING` makes SQS at-least-once redelivery harmless.
- A failed batch is retried up to 3 times with linear backoff (1 s, 2 s). If it still fails, the batch is counted in `db.failed`.
- HikariCP `maximum-pool-size=10` covers 5 writers plus headroom for retries, with a 3 s connection timeout so writes fail fast during an RDS restart.
- Batch tuning ([batch-tuning/results.md](ChatFlow-4-(new%20design)/load-tests/batch-tuning/results.md)): at around 1K msg/s the real batch size stays at about 1.1–1.4 whatever the configured size, because the writers drain faster than SQS fills the queue. Batching only kicks in under backpressure; one dirty-DB run reached an average of 153 rows per batch.

### Step 8 — Database schema and indexes

[schema.sql](ChatFlow-4-(new%20design)/database/schema.sql) and [indexes.sql](ChatFlow-4-(new%20design)/database/indexes.sql); design notes in [DESIGN.md](ChatFlow-4-(new%20design)/database/DESIGN.md)

| Column | Type | Note |
|---|---|---|
| `id` | `BIGSERIAL` PK | surrogate key |
| `message_id` | `VARCHAR(36) UNIQUE` | idempotency key |
| `room_id` | `SMALLINT` | 1–20 |
| `user_id` | `INT` | 1–100,000 |
| `username`, `message`, `message_type` | `VARCHAR(20)`, `TEXT`, `VARCHAR(8)` | |
| `server_id`, `client_ip` | `VARCHAR(50)`, `VARCHAR(45)` | diagnostics |
| `sent_at` | `BIGINT` | epoch ms (client timestamp) |
| `created_at` | `TIMESTAMPTZ DEFAULT NOW()` | write-lag measurement |

| Index | Serves |
|---|---|
| `(room_id, sent_at)` | Q1: messages in a room within a time range |
| `(user_id, sent_at)` | Q2: a user's history within a time range |
| `(sent_at, user_id)` | Q3: `COUNT(DISTINCT user_id)` in a window (index-only scan) |
| `(user_id, room_id, sent_at)` | Q4: rooms a user was active in, with last activity (index-only scan) |

### Step 9 — Live stats and health

[StatsAggregatorService.java](ChatFlow-4-(new%20design)/consumer-v4/src/main/java/com/chatflow/consumer/stats/StatsAggregatorService.java) and [HealthController.java](ChatFlow-4-(new%20design)/consumer-v4/src/main/java/com/chatflow/consumer/HealthController.java)

- Per-room and per-user counters use `ConcurrentHashMap<Integer, LongAdder>`. A sliding timestamp deque is pruned every second by one scheduled thread, which computes msg/s over 1 s, 10 s and 60 s windows.
- `GET :8081/health` returns `sqs.{consumed}`, `broadcast.{enqueued, sent, failed, queueDepth}`, `db.{enqueued, inserted, failed, queueDepth, batches, avgBatchSize, avgInsertMs}` and `stats.{totalMessages, msgPerSec*, topRooms, topUsers}`.
- [monitoring/](ChatFlow-4-(new%20design)/monitoring/) scripts (`monitor-consumer.sh`, `watch-consumers.sh`, `collect-final.sh`, `monitor-rds.sh`) poll these endpoints and CloudWatch into CSV files for the reports.

### Step 10 — Read path: the analytics API

[MetricsController.java](ChatFlow-4-(new%20design)/server-v3/src/main/java/com/chatflow/server/metrics/MetricsController.java) and [MessageQueryService.java](ChatFlow-4-(new%20design)/server-v3/src/main/java/com/chatflow/server/metrics/MessageQueryService.java)

`GET /metrics?roomId=&userId=&startTime=&endTime=&topN=` (times are epoch ms; the default window is the last hour). The controller runs **8 queries in parallel** on a dedicated 8-thread pool, so response time is set by the slowest query rather than the sum:

| Core | Analytics |
|---|---|
| Q1 room messages in window (LIMIT 1000) | A1 messages per minute |
| Q2 user history in window (LIMIT 1000) | A2 top-N users |
| Q3 distinct active users in window | A3 top-N rooms |
| Q4 rooms per user, with last activity | A4 total message count |

The server's HikariCP pool is 10 so that all 8 queries get a connection. With a pool of 5, `/metrics` timed out during A4 Round 1. The endpoint returns `503` on `DataAccessException`. The load-test client calls it at the end of a run with the test's own start and end times. JMeter plans ([load-tests/jmeter/](ChatFlow-4-(new%20design)/load-tests/jmeter/)) mix 70% `/metrics` reads with 30% WebSocket writes.

### Separation of responsibilities

| Concern | Owner | Why |
|---|---|---|
| Ingest, validate, ack | server-v3 | Keep the client's critical path short |
| Durable hand-off | SQS FIFO | Managed, per-room ordering, buffers bursts (about 220K messages were absorbed during the A4 run) |
| Real-time fan-out | consumer-v4 workers → every server | Decoupled from consumption; reaches clients on any ALB target |
| Writes | consumer-v4 `DbWriterService` | Write-behind keeps the DB off the broadcast path |
| Reads | server-v3 `/metrics` | The server owns reads and the consumer owns writes, each with its own pool size |

### Running it

For the full checklist (AWS access, purging SQS, truncating `messages`, start order and monitoring), see [QUICK-START.md](ChatFlow-4-(new%20design)/QUICK-START.md). Short version:

```bash
# 1. DB (once)
cd "ChatFlow-4-(new design)/database" && ./setup.sh           # needs RDS_HOST / RDS_USER / RDS_PASS

# 2. Servers (EC2-S1, EC2-S2)
java -jar server-v2-1.0.0.jar --app.server-id=server-s1   # artifactId is still server-v2

# 3. Consumers (EC2-C1, EC2-C2)
java -jar consumer-1.0.0.jar --app.consumer.threads=10
java -jar consumer-1.0.0.jar --app.consumer.threads=10 --app.consumer.room-start=11

# 4. Load
cd "ChatFlow-4-(new design)/client-v3" && mvn -q package -DskipTests && java -jar target/client-part1-1.0.0.jar

# 5. Watch
curl -s http://<consumer>:8081/health | python3 -m json.tool
```

Between test runs, let all 20 SQS queues drain to 0 and run `TRUNCATE TABLE messages`. Residual SQS messages from an aborted run doubled the arrival rate and caused an OOM in A4 Round 2.

---

## 5. Repository Layout

```
.
├── README.md                      ← this file
├── ChatFlow-3(old design)/
│   ├── Chatflow-3.md              system overview of the old design
│   ├── server-2/
│   │   ├── server/                Java-WebSocket :8080 + Jetty :8081, RabbitMQ publisher
│   │   ├── client/                load-test client (ConnectionPool, Sender/Retry workers)
│   │   └── common/                ChatMessage, MessageEnvelope, MessageValidator
│   ├── consumer-v3/               RabbitMQ consumer, Redis dedup, DbWriterPool, stats
│   ├── database/setup.sql
│   ├── load-tests/                load_test_results.md
│   └── monitoring/
└── ChatFlow-4-(new design)/
    ├── server-v3/                 Spring Boot WS + SQS publisher + /internal/broadcast + /metrics
    ├── consumer-v4/               SQS poller + BroadcastWorkerService + DbWriterService + stats
    ├── client-v3/                 load-test client + metrics report
    ├── database/                  schema.sql, indexes.sql, setup.sh, verify.sql, DESIGN.md
    ├── load-tests/                REPORT.md, batch-tuning/, jmeter/ (plans + HTML reports)
    ├── monitoring/                consumer / RDS monitoring scripts
    ├── A4-review.md               optimization rounds and results
    ├── PROJECT_CONTEXT.md, plan.md, QUICK-START.md
```
