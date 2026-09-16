# System Overview

JobStream is a Redis-backed distributed job queue designed to provide reliable, decoupled, and scalable background job processing.

> **Note:** This document outlines the TARGET architecture. Currently, the project is in Phase 0 and consists only of a bare Maven scaffold (`Main.java` and `MainTest.java`). The features described here are planned.

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
   - Manages worker registration, heartbeat-based liveness detection, graceful shutdown, and concurrent processing loops.
   - Dequeues `JobId` from queues, fetches full `Job` state from `JobRepository`, and coordinates execution.
5. **Execution Engine (`executor`)**
   - Pluggable `JobExecutor` abstraction and `ExecutorRegistry` dispatching execution based on job type.
   - Decoupled from worker machinery: executors know only about `Job` and `Payload`; workers invoke executors through the abstraction.
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
Producer ────────> [ JobQueue ] ────────> [ Worker ] ────────> [ JobExecutor ]
   │               (Redis List)              │                        │
   │               Queue → JobId             │ 4. Execute             │
   │                     │                   └────────────────────────┘
   │ 2. Push JobId       │ 3. Pop JobId
   └─────────────────────┘
```

### Complete Step-by-Step Data Flow

1. **Job Creation:** Producer instantiates a `Job` with unique `JobId`, `type`, and immutable `Payload` (status: `PENDING`).
2. **Persistence & Enqueue:**
   - Producer writes the `Job` to `JobRepository` with status `QUEUED`.
   - Producer pushes `JobId` to the target `JobQueue`.
   - *Atomicity boundary:* State update and queue insertion are coordinated to avoid stranded jobs or invalid references.
3. **Worker Acquisition:**
   - A `Worker` polling the queue performs a blocking pop (`BRPOP`) to acquire the next `JobId`.
   - Worker fetches the full `Job` record from `JobRepository`.
   - Worker transitions job status to `PROCESSING` in `JobRepository`.
4. **Execution:**
   - Worker retrieves the appropriate `JobExecutor` from `ExecutorRegistry` matching `job.getType()`.
   - Worker invokes `JobExecutor.execute(job)` inside a defensive boundary intercepting `Throwable`.
5. **Outcome Resolution:**
   - **Success:** Worker updates job status to `COMPLETED` and saves completion timestamp in `JobRepository`.
   - **Failure:** Worker records error details. The `RetryPolicy` is evaluated:
     - If retries remain: Job status becomes `RETRYING`, retry counter increments, and job is scheduled for future re-enqueueing.
     - If retries exhausted: Job status becomes `DEAD`, and `JobId` is moved to the Dead-Letter Queue (DLQ).

---

## 3. Key Architectural Principles

1. **Dependency Direction Inward:**
   - The core domain (`job`, `payload`) has zero dependencies on infrastructure, configuration, or execution frameworks.
   - High-level orchestrators depend on abstractions; infrastructure implements abstractions.
2. **Worker / Executor Decoupling:**
   - `Worker` depends on `JobExecutor` abstraction.
   - `JobExecutor` has **zero knowledge** of `Worker`. Executors process jobs and return results.
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
