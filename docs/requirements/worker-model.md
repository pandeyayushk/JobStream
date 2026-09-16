# Worker Model Requirements

## 1. Purpose

Defines the lifecycle, concurrency, registration, and processing loop of distributed worker nodes that acquire jobs from queues and coordinate their execution.

---

## 2. Canonical Acquisition Flow: Reference to Entity

A Worker interacts with the queue and repository as follows:
```
Queue (Redis List) ──(pop JobId)──> Worker ──(fetch Job)──> JobRepository
                                      │
                         (invoke JobExecutor)
                                      ▼
                               Execution Engine
```
1. Worker executes blocking dequeue (`dequeue(queueName)`) to acquire next `JobId`.
2. Worker fetches the full `Job` record from `JobRepository`.
3. Worker marks job status as `PROCESSING` in `JobRepository`.
4. Worker delegates execution to the `JobExecutor` abstraction.
5. Worker updates `JobRepository` with the outcome (`COMPLETED` or `FAILED`).

---

## 3. Functional Requirements

### 3.1 Worker Identity & Metadata (`WorkerInfo`)
- Each worker instance generates a unique `WorkerId` (UUID-based).
- Tracks worker metadata: host, process ID, assigned queues, concurrency level, start time, last heartbeat timestamp.
- Worker states: `STARTING`, `RUNNING`, `STOPPING`, `STOPPED`.

### 3.2 Processing Loop & Concurrency
- Configurable worker thread pool concurrency (N worker threads per process).
- Threads execute independent polling loops on assigned queues.
- Defensive loop: unhandled exceptions during job processing must never terminate worker threads.

### 3.3 Worker Registry & Heartbeat Liveness (`WorkerRegistry`)
- Workers register in Redis on startup (`jobstream:worker:{workerId}`).
- Send periodic heartbeats (default: every 10 seconds) updating a Redis key with TTL (default: 30 seconds).
- Heartbeat expiration indicates worker death/unresponsiveness.
- Explicit deregistration upon graceful shutdown.

### 3.4 Graceful Shutdown
- Workers trap termination signals (`SIGTERM`, `SIGINT`).
- Shutdown protocol:
  1. Stop accepting new `JobId`s from queues.
  2. Await in-flight job executions to complete (up to a configurable timeout, e.g. 30 seconds).
  3. Deregister from `WorkerRegistry`.
  4. Release connection pool resources and terminate cleanly.

---

## 4. Separation from Execution Logic

- **Decoupling Rule:** The `Worker` is purely an infrastructure and coordination component.
- The `Worker` does **not** contain business logic, payload parsing, or `switch (job.getType())` statements.
- The `Worker` depends on the `JobExecutor` abstraction (Strategy pattern, introduced in Phase 5).
- In Phase 4, worker tests use an explicit test boundary/stub to verify the loop without coupling to production execution architecture.

---

## 5. Non-Functional Requirements

- Thread-safe coordination across concurrent worker threads.
- Predictable, bounded memory consumption.

---

## 6. Dependencies

- Queue System (Phase 3: `JobQueue`)
- Persistence Layer (Phase 2: `JobRepository`)
- Domain Model (Phase 1: `Job`, `JobId`, `JobStatus`)

---

## 7. Phase Ownership

- **Phase 4 (Worker Foundation):** Implements `Worker`, `WorkerRegistry`, heartbeat manager, and graceful shutdown.
