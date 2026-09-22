# Project Structure

This document is the **CANONICAL reference** for the entire JobStream repository and Java package organization.

## Repository Structure

```
JobStream/
├── docs/
│   ├── architecture/        # How the system is structured
│   ├── requirements/         # What the system must do
│   ├── decisions/            # Why architectural choices were made (ADRs)
│   └── versions/             # Phase-by-phase implementation guides
├── src/
│   ├── main/java/io/github/pandeyayushk/jobstream/
│   │   ├── job/              # Core Job domain model, status, and identity (Zero dependencies)
│   │   ├── payload/          # Domain payload abstraction and handling (Zero dependencies)
│   │   ├── serialization/    # Serialization abstractions and implementations (JSON)
│   │   ├── persistence/      # Storage abstraction and Redis repository implementation
│   │   ├── queue/            # Queue abstraction and Redis-backed implementation
│   │   ├── worker/           # Worker model, registration, lifecycle, heartbeat
│   │   ├── executor/         # Job executor interface, factory, and implementations
│   │   ├── retry/            # Retry policy, backoff calculation, and dead-letter queue
│   │   ├── cli/              # Command-line interface
│   │   ├── schedule/         # Delayed/scheduled job support
│   │   ├── priority/         # Priority queue support
│   │   ├── metrics/          # Statistics, counters, and observability
│   │   ├── api/              # HTTP API (REST endpoints)
│   │   ├── config/           # Centralized configuration loading and validation
│   │   └── Main.java         # Application entry point
│   └── test/java/io/github/pandeyayushk/jobstream/
│       ├── job/              # Unit tests for job domain
│       ├── payload/          # Unit tests for payload
│       ├── serialization/    # Unit & integration tests for serialization
│       ├── persistence/      # Repository integration tests
│       ├── queue/            # Queue integration and concurrency tests
│       ├── worker/           # Worker lifecycle and concurrency tests
│       ├── executor/         # Executor and registry tests
│       ├── retry/            # Retry policy and DLQ tests
│       ├── cli/              # CLI parsing and command tests
│       ├── schedule/         # Scheduler and poller tests
│       ├── priority/         # Priority queue tests
│       ├── metrics/          # Metrics and counters tests
│       ├── api/              # REST API endpoint tests
│       ├── config/           # Configuration loader tests
│       └── MainTest.java     # Existing Phase 0 smoke test
├── pom.xml
├── README.md
└── .gitignore
```

## Java Package Hierarchy & Dependency Rules

### Canonical Architectural Principle: Dependency Inward toward Domain

The core domain (`job`, `payload`) represents pure business concepts. It must remain strictly independent of infrastructure, storage, network, and execution machinery.

**Dependency Inversion Rules:**
1. **The Core Domain depends on NOTHING.** `job` and `payload` have zero package dependencies (no `config`, no `persistence`, no `Redis`, no `worker`, no `executor`).
2. **Dependencies flow inward toward the domain:**
   ```
   [api] ────┐
             ▼
   [cli] ──> [application / coordinator]
                   │
         ┌─────────┼──────────────┐
         ▼         ▼              ▼
     [worker]   [queue]      [schedule]
         │         │              │
         ▼         ▼              │
    [executor] [persistence] <────┘
         │         │
         ▼         ▼
    [payload] [serialization]
         │         │
         └────┬────┘
              ▼
            [job]
   ```
   `application / coordinator` in this diagram is a conceptual coordination layer, not a current Java package. Coordination responsibilities remain in the packages that introduce them; this diagram does not require or introduce an `application` package.
3. **Worker → Executor Relationship:**
   - `worker` depends on the `executor` abstraction (`JobExecutor`, `ExecutorRegistry`).
   - `executor` **MUST NOT** depend on `worker`. An executor only knows about `Job` and `Payload`.
4. **Configuration Dependency:**
   - `config` provides operational configuration parameters.
   - Runtime/infrastructure packages (`persistence`, `worker`, `cli`, `api`) may depend on `config`.
   - Core domain (`job`, `payload`) and pure serialization (`serialization`) **MUST NOT** depend on `config`.
5. **No Circular Dependencies:** Circular dependencies between any packages are strictly forbidden.
6. **Package Restructuring:** Any change to package boundaries requires an Architecture Decision Record (ADR).

---

### Package Responsibilities & Allowed Dependencies

*Note: The interfaces/classes listed below are planned for their respective phases unless explicitly marked implemented.*

- **`io.github.pandeyayushk.jobstream.job`**
  - **Responsibility:** Core domain entity (`Job`), unique identity (`JobId`), and complete lifecycle state contract (`JobStatus`).
  - **Phase Introduced:** Phase 1
  - **Status:** Implemented in Phase 1
  - **Implemented Types:** `Job`, `JobId`, `JobStatus`
  - **Allowed Dependencies:** **None** (Pure Java Standard Library only).

- **`io.github.pandeyayushk.jobstream.payload`**
  - **Responsibility:** Domain payload abstraction representing structured execution parameters.
  - **Phase Introduced:** Phase 1
  - **Status:** Implemented in Phase 1
  - **Implemented Types:** `Payload`
  - **Allowed Dependencies:** **None** (Pure Java Standard Library only).

- **`io.github.pandeyayushk.jobstream.serialization`**
  - **Responsibility:** Converting domain objects (`Job`, `Payload`) to/from portable byte or string formats across storage boundaries.
  - **Phase Introduced:** Phase 2
  - **Planned Types:** `JobSerializer`, `JacksonJobSerializer`
  - **Allowed Dependencies:** `job`, `payload`

- **`io.github.pandeyayushk.jobstream.persistence`**
  - **Responsibility:** Storage repository abstraction (`JobRepository`) and Redis implementation storing `JobId -> Job`.
  - **Phase Introduced:** Phase 2
  - **Planned Types:** `JobRepository`, `RedisJobRepository`
  - **Allowed Dependencies:** `job`, `payload`, `serialization`, `config`

- **`io.github.pandeyayushk.jobstream.queue`**
  - **Responsibility:** Queue abstractions (`JobQueue`) and Redis implementations storing `Queue -> JobId` with atomic enqueue/dequeue semantics.
  - **Phase Introduced:** Phase 3
  - **Status:** Implemented in Phase 3
  - **Implemented Types:** `JobQueue`, `RedisJobQueue`, `QueueCoordinator`, `QueueCoordinatorImp`, `JobSubmissionStore`, `RedisJobSubmissionStore`, `QueueException`
  - **Allowed Dependencies:** `job`, `serialization`, Redis client
  - **Boundary:** `JobQueue` has no dependency on `JobRepository`; `JobRepository` has no dependency on `JobQueue`. `QueueCoordinatorImp` owns application-level submission and delegates Redis-specific atomic work to `JobSubmissionStore`.

- **`io.github.pandeyayushk.jobstream.worker`**
  - **Responsibility:** Worker process model, polling loop, lifecycle management, registration, and heartbeat.
  - **Phase Introduced:** Phase 4
  - **Planned Types:** `Worker`, `WorkerId`, `WorkerInfo`, `WorkerStatus`, `WorkerRegistry`, `RedisWorkerRegistry`
  - **Allowed Dependencies:** `job`, `queue`, `persistence`, `executor`, `config`

- **`io.github.pandeyayushk.jobstream.executor`**
  - **Responsibility:** Pluggable execution abstraction (`JobExecutor`), execution result model, and executor registry/factory.
  - **Phase Introduced:** Phase 5
  - **Planned Types:** `JobExecutor`, `ExecutionResult`, `ExecutorRegistry`, `DefaultExecutorRegistry`
  - **Allowed Dependencies:** `job`, `payload` *(Explicitly: NO dependency on `worker`)*

- **`io.github.pandeyayushk.jobstream.retry`**
  - **Responsibility:** Retry policy evaluation, backoff computation, and Dead-Letter Queue (DLQ) management.
  - **Phase Introduced:** Phase 6
  - **Planned Types:** `RetryPolicy`, `FixedDelayRetryPolicy`, `ExponentialBackoffRetryPolicy`, `DeadLetterQueue`, `RedisDeadLetterQueue`
  - **Allowed Dependencies:** `job`, `queue`, `persistence`, `config`

- **`io.github.pandeyayushk.jobstream.cli`**
  - **Responsibility:** Command-line operational tool for operators.
  - **Phase Introduced:** Phase 7
  - **Planned Types:** `JobStreamCli`, command subpackages
  - **Allowed Dependencies:** `job`, `payload`, `queue`, `persistence`, `worker`, `retry`, `config`

- **`io.github.pandeyayushk.jobstream.schedule`**
  - **Responsibility:** Time-delayed execution and scheduled job migration poller.
  - **Phase Introduced:** Phase 8
  - **Planned Types:** `JobScheduler`, `RedisJobScheduler`, `SchedulerPoller`
  - **Allowed Dependencies:** `job`, `queue`, `persistence`, `config`

- **`io.github.pandeyayushk.jobstream.priority`**
  - **Responsibility:** Priority-differentiated queueing and multi-tier queue dispatch.
  - **Phase Introduced:** Phase 8
  - **Planned Types:** `Priority`, `PriorityJobQueue`, `RedisPriorityJobQueue`
  - **Allowed Dependencies:** `job`, `queue`, `config`

- **`io.github.pandeyayushk.jobstream.metrics`**
  - **Responsibility:** Counters, gauges, and throughput statistics collection.
  - **Phase Introduced:** Phase 9
  - **Planned Types:** `JobMetrics`, `RedisJobMetrics`
  - **Allowed Dependencies:** `job`, `config`

- **`io.github.pandeyayushk.jobstream.api`**
  - **Responsibility:** REST HTTP endpoints and web server lifecycle.
  - **Phase Introduced:** Phase 9
  - **Planned Types:** `JobStreamApiServer`, controllers, DTOs
  - **Allowed Dependencies:** `job`, `queue`, `persistence`, `worker`, `retry`, `metrics`, `config`

- **`io.github.pandeyayushk.jobstream.config`**
  - **Responsibility:** Centralized configuration loading (environment variables, system properties, files) and validation.
  - **Phase Introduced:** Phase 2 (basic `RedisConfig`), Phase 10 (unified `JobStreamConfig`)
  - **Planned Types:** `RedisConfig`, `JobStreamConfig`, `ConfigLoader`
  - **Allowed Dependencies:** **None** (Pure Java Standard Library only).

---

## Package Boundary Rules

1. **Information Hiding:** Expose minimal public interfaces. Implementations should remain package-private unless cross-package construction requires visibility.
2. **Mirroring in Tests:** Test packages in `src/test/java` must mirror production packages in `src/main/java` exactly.
3. **No Premature Directory Creation:** Directories must not be created in `src/` until the phase introducing that package is actively implemented.

## Current State (Phase 3)

Phase 1 is complete.
- The `job`, `payload`, `serialization`, `persistence`, and `queue` packages are implemented.
- The `queue` package contains FIFO Redis list operations and the atomic submission adapter described above.
- Worker and later operational packages remain future architecture.
