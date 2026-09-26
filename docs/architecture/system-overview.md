# System Overview

JobStream is a Redis-backed distributed job queue designed to provide reliable, decoupled, and scalable background job processing.

> **Note:** Phases 1 through 5 are implemented. Phase 5 provides type-based executor dispatch. Retry belongs to Phase 6; later operational subsystems remain planned.

---

## 1. High-Level Subsystems

The JobStream system consists of the following decoupled subsystems:

1. **Job Domain (`job`, `payload`)**
   - Core domain model representing job identity (`JobId`), full lifecycle contract (`JobStatus`), and immutable input data (`Payload`).
   - Pure domain logic with zero external or infrastructure dependencies.
2. **Persistence Layer (`persistence`, `serialization`)**
   - Storage repository abstraction (`JobRepository`) providing durable storage mapping `JobId -> Job record`.
   - Serialization layer converting domain objects to portable JSON for storage in Redis strings/hashes.
3. **Queue System (`queue`, `priority`, `schedule`)**
   - FIFO queue mechanism holding lightweight references (`JobId`).
   - Decoupled from full job storage; enqueuing places `JobId` onto the queue, and dequeuing retrieves `JobId`.
   - Priority queues and time-delayed scheduling.
4. **Worker System (`worker`)**
   - Manages worker registration, heartbeat-based liveness detection, graceful shutdown, and bounded concurrent processing.
   - Dequeues `JobId`, fetches the authoritative job from `JobRepository`, claims `QUEUED` jobs, and coordinates execution through `ExecutorRegistry`.
5. **Execution Engine (`executor`)**
   - `ExecutorRegistry` resolves a `JobExecutor` by job type. The executor returns an `ExecutionResult`; Worker persists the resulting lifecycle state.
6. **Reliability Layer (`retry`)**
   - Handles transient failures with configurable retry policies, non-blocking backoff scheduling, and Dead-Letter Queue (DLQ) quarantine for exhausted failures.
7. **Management & Observability (`cli`, `api`, `metrics`)**
   - Command-Line Interface (CLI) and REST HTTP API for system interaction, monitoring, job submission, and DLQ management.
   - Real-time counters, throughput gauges, and health probes.
8. **Configuration & Packaging (`config`)**
   - 12-Factor centralized configuration and multi-stage container packaging.

---

## 2. Core Architectural Flow & Data Flow

### Architecture Baseline: Reference-Based Queueing

To prevent data duplication and maintain a single source of truth:
- **Persistence:** Maps `JobId → Job record`. All state mutations, status updates, and retry counts occur here.
- **Queue:** Maps `Queue → JobId`. The queue holds only lightweight `JobId` references.

```
                ┌──────────────────────────────────┐
                │          JobRepository           │
                │         (Redis Strings)          │
                │          JobId → Job             │
                └────────┬────────────────▲────────┘
                         │                │
           1. Save Job   │                │ 5. Update Status
                         ▼                │    (PROCESSING / COMPLETED / FAILED)
Producer ────────> [ JobQueue ] ────────> [ Worker ] ────────> [ ExecutorRegistry ]
                                                                  ↓
                                                             [ JobExecutor ]
                                                                  ↓
                                                          [ ExecutionResult ]
                                                                  ↓
                                                          [ JobRepository ]
   │               (Redis List)              │                        │
   │               Queue → JobId             │ 4. Execute             │
   │                     │                   └────────────────────────┘
   │ 2. Push JobId       │ 3. Pop JobId
   └─────────────────────┘
```

### Complete Step-by-Step Data Flow

1. **Job Creation:** Producer instantiates a `Job` with unique `JobId`, `type`, and immutable `Payload` (status: `PENDING`).
2. **Persistence & Enqueue:**
   - `QueueCoordinator` creates the `QUEUED` version of the supplied job and delegates to `JobSubmissionStore`.
   - `RedisJobSubmissionStore` atomically persists that job, moves its status-index entry, and pushes its `JobId` to the target queue with Redis `MULTI`/`EXEC`.
3. **Worker Acquisition:**
   - `Worker` polls the configured queue to acquire the next `JobId` (the Redis queue uses a timed blocking dequeue).
   - Worker fetches the full `Job` record from `JobRepository`.
   - Worker transitions job status to `PROCESSING` in `JobRepository`.
4. **Worker execution:** Worker verifies the loaded job is `QUEUED`, persists `PROCESSING`, looks up an executor by `Job.type()`, and invokes it. A missing executor, failure result, or thrown `Throwable` leads to `FAILED`; failures are recorded in metadata as `failure.reason`. A success result leads to `COMPLETED`. Executor failures do not terminate the Worker.
5. **Later phases:** Phase 6 introduces retry policy, scheduling, and retry exhaustion handling. Retry, timeout, dead-letter behavior, and recovery of `PROCESSING` jobs are outside Phase 5.

---

## 3. Key Architectural Principles

1. **Dependency Direction Inward:**
   - The core domain (`job`, `payload`) has zero dependencies on infrastructure, configuration, or execution frameworks.
   - High-level orchestrators depend on abstractions; infrastructure implements abstractions.
2. **Separation of Responsibilities:**
   - `JobQueue` transports `JobId`; `JobRepository` owns authoritative Job state.
   - Worker coordinates acquisition and execution; `ExecutorRegistry` resolves business executors by job type.
   - `JobExecutor` contains job-specific execution logic and does not depend on Worker, Redis, `JobQueue`, or `WorkerRegistry`.
   - `ExecutionResult` communicates the executor outcome. Retry is a later concern.
3. **Single Source of Truth:**
   - The `JobRepository` is the sole authoritative store of job state. The queue contains only references (`JobId`).
4. **Fault Containment:**
   - Unhandled exceptions or errors in user-defined job executors must never terminate worker threads or corrupt queue state.
5. **Fail-Safe Operational Defaults:**
   - Systems fail fast on invalid configurations and default to conservative backoff, explicit timeouts, and bounded concurrency.

---

## 4. Planned Technology Stack

- **Java 21 LTS** (Modern language features: records, virtual threads, pattern matching)
- **Apache Maven** (Standard build and dependency management)
- **Redis 7+** (Unified backend for persistence, queues, and sorted sets)
- **JUnit 5** (Unit and integration testing)
- **Jackson** (JSON serialization across persistence boundary — Phase 2)
- **Jedis** (Synchronous, pooled Redis client — Phase 2)
- **Picocli** (Administrative CLI — Phase 7)
- **Javalin** (Lightweight embedded REST API — Phase 9)
