# Phase 4 — Worker Foundation

## 1. Objective

Implement the worker infrastructure: worker identity, registration, heartbeat-based liveness detection, graceful shutdown, and concurrent processing loops that acquire `JobId`s from `JobQueue` and load `Job` entities from `JobRepository`.

## 2. Why This Phase Exists

Workers are the distributed execution nodes of JobStream. Without workers, jobs remain queued indefinitely. This phase establishes:
- **Acquisition Protocol:** Worker pops `JobId` from `JobQueue`, fetches `Job` from `JobRepository`, and transitions status to `PROCESSING`.
- **Worker Lifecycle:** Clean transitions (`STARTING` → `RUNNING` → `STOPPING` → `STOPPED`).
- **Liveness Detection:** Periodic heartbeats with TTL in Redis to track cluster health.
- **Graceful Shutdown:** Intercepting termination signals to finish in-flight jobs without state corruption.
- **Explicit Test Boundary:** Uses a minimal internal processing hook for testing the loop, reserving the pluggable, type-dispatched execution engine for Phase 5.

**Depends on:**
- Phase 1 (Domain Model: `Job`, `JobId`, `JobStatus`)
- Phase 2 (Persistence: `JobRepository`, `RedisJobRepository`)
- Phase 3 (Queue: `JobQueue`, `RedisJobQueue`)

## 3. Scope

**In scope:**
- `WorkerId` value object (UUID-based identity)
- `WorkerStatus` enum (`STARTING`, `RUNNING`, `STOPPING`, `STOPPED`)
- `WorkerInfo` metadata record (id, host, queues, concurrency, heartbeat)
- `WorkerRegistry` interface and `RedisWorkerRegistry` implementation (storing worker metadata and heartbeats with TTL)
- `Worker` concurrent processing loop managing a thread pool
- `WorkerTestHandler` (explicit test-only functional interface for verifying the acquisition loop)
- Graceful shutdown protocol with timeout
- Concurrency and lifecycle integration tests

**Out of scope:**
- Production `JobExecutor` abstraction and type-based dispatch (Phase 5)
- Automatic retry scheduling and backoff calculation (Phase 6)
- Dead-worker detection and recovery of abandoned `PROCESSING` jobs (later reliability/recovery phase)

## 4. Prerequisites

- Phase 3 complete (`JobQueue` and `JobRepository` operational)
- Redis 7+ running locally
- Understanding of Java concurrency (`ExecutorService`, `ScheduledExecutorService`, `CountDownLatch`, `AtomicBoolean`)

## 5. Read Before Starting

- `docs/requirements/worker-model.md` — Worker specifications and acquisition flow
- `docs/architecture/project-structure.md` — Package location for `worker`
- `docs/architecture/system-overview.md` — Worker role in overall architecture
- `docs/architecture/job-lifecycle.md` — `QUEUED → PROCESSING → COMPLETED` transitions

## 6. Concepts to Understand

- **Worker Acquisition Flow:**
  ```
  Queue (Redis List) ──(pop JobId)──> Worker ──(fetch Job)──> JobRepository
                                        │
                           (status = PROCESSING)
  ```
- **Java ThreadPoolExecutor:** Managing worker threads with configurable concurrency.
- **ScheduledExecutorService:** Used for periodic non-blocking heartbeat ticks.
- **Redis TTL (Key Expiration):** Setting `SET jobstream:worker:{id}:heartbeat "alive" EX 30`. If the worker process crashes, Redis expires the key automatically after 30 seconds.
- **Graceful Shutdown Pattern:**
  1. Flag worker loop as stopping (`AtomicBoolean running = false`).
  2. Stop taking new `JobId`s from `JobQueue`.
  3. Wait for active threads via `executorService.awaitTermination(timeout, unit)`.
  4. Remove worker record from `WorkerRegistry`.

## 7. Design Decisions

### 7.1 Worker Concurrency Model

**Decision:** Each `Worker` instance manages a configurable thread pool (`concurrency` threads, default: 4) using `Executors.newFixedThreadPool`. Each thread runs an independent acquisition loop polling assigned queues.

**Record this decision in:** ADR-006 (to be created during this phase).

### 7.2 Heartbeat Architecture

**Decision:** Two-tier Redis tracking:
1. `jobstream:worker:{workerId}`: Hash storing static metadata (`WorkerInfo`).
2. `jobstream:worker:{workerId}:heartbeat`: Key with TTL (default: 30s), refreshed every 10s by a background daemon thread.

**Lifecycle distinction:**
- On graceful shutdown, the worker explicitly deregisters from `WorkerRegistry` after it stops accepting work and completes its shutdown protocol.
- On a process crash, explicit deregistration cannot run; the heartbeat TTL eventually expires to indicate lost liveness.
- Static or stale worker metadata may remain after heartbeat expiration for inspection.
- Recovering an abandoned `PROCESSING` job is not a Phase 4 responsibility; it belongs to the later reliability/recovery phase.

### 7.3 Execution Boundary in Phase 4 (Test Boundary vs Production Architecture)

**Crucial Architectural Rule:** Avoid fake production execution implementations.
- In Phase 4, the `Worker` requires a way to simulate task completion during tests.
- We define a minimal internal hook:
  ```java
  @FunctionalInterface
  public interface WorkerJobHandler {
      void handle(Job job);
  }
  ```
- In Phase 4 tests, a simple lambda handles or completes the job.
- In Phase 5, this hook is replaced by the production `ExecutorRegistry` and `JobExecutor` subsystem.
- This keeps Phase 4 focused strictly on worker lifecycle, concurrency, and acquisition mechanics.

## 8. Requirements

### Functional Requirements

1. **`WorkerId` (package `io.github.pandeyayushk.jobstream.worker`):**
   - UUID-based value object.
   - `WorkerId.generate()` and `WorkerId.fromString(String id)`.

2. **`WorkerStatus`:**
   - Enum values: `STARTING`, `RUNNING`, `STOPPING`, `STOPPED`.

3. **`WorkerInfo`:**
   - Record containing: `WorkerId id`, `String hostname`, `List<String> queues`, `int concurrency`, `WorkerStatus status`, `Instant startedAt`, `Instant lastHeartbeatAt`.

4. **`WorkerRegistry` Interface & `RedisWorkerRegistry`:**
   - `void register(WorkerInfo info)`
   - `void heartbeat(WorkerId workerId)`
   - `void deregister(WorkerId workerId)`
   - `List<WorkerInfo> listActiveWorkers()`
   - `Optional<WorkerInfo> getWorker(WorkerId workerId)`

5. **`Worker` Core Engine:**
   - Constructor accepts: `WorkerConfig`, `JobQueue`, `JobRepository`, `WorkerRegistry`, `WorkerJobHandler`.
   - `void start()`: Launches worker threads and heartbeat scheduler.
   - `void stop()`: Initiates graceful shutdown.
   - Acquisition loop:
     1. Pop `JobId` from assigned queue(s) via blocking dequeue.
     2. Load `Job` from `JobRepository`.
     3. Update `Job` status to `PROCESSING` in `JobRepository`.
     4. Invoke handler.
     5. Update `Job` status to `COMPLETED` in `JobRepository`.

### Non-Functional Requirements

- **Fault Containment:** Unhandled runtime exceptions thrown by the handler must not terminate worker threads.
- **Clean Resource Release:** Shutdown must cleanly close thread pools and heartbeat schedulers within timeout.

## 9. File Change Manifest

### CREATE

```
src/main/java/io/github/pandeyayushk/jobstream/worker/WorkerId.java
src/main/java/io/github/pandeyayushk/jobstream/worker/WorkerStatus.java
src/main/java/io/github/pandeyayushk/jobstream/worker/WorkerInfo.java
src/main/java/io/github/pandeyayushk/jobstream/worker/WorkerConfig.java
src/main/java/io/github/pandeyayushk/jobstream/worker/WorkerJobHandler.java
src/main/java/io/github/pandeyayushk/jobstream/worker/WorkerRegistry.java
src/main/java/io/github/pandeyayushk/jobstream/worker/RedisWorkerRegistry.java
src/main/java/io/github/pandeyayushk/jobstream/worker/Worker.java
src/test/java/io/github/pandeyayushk/jobstream/worker/WorkerIdTest.java
src/test/java/io/github/pandeyayushk/jobstream/worker/RedisWorkerRegistryTest.java
src/test/java/io/github/pandeyayushk/jobstream/worker/WorkerTest.java
```

### MODIFY

None.

### DELETE

None.

### DO NOT TOUCH

```
src/main/java/.../job/*
src/main/java/.../payload/*
src/main/java/.../serialization/*
src/main/java/.../persistence/*
src/main/java/.../queue/*
pom.xml
```

## 10. Implementation Order

1. **`WorkerId.java`** & **`WorkerStatus.java`** — Identity and states.
2. **`WorkerInfo.java`** & **`WorkerConfig.java`** — Metadata and operational parameters.
3. **`WorkerRegistry.java`** & **`RedisWorkerRegistry.java`** — Redis registry implementation.
4. **`RedisWorkerRegistryTest.java`** — Test worker registration, heartbeat TTL, deregistration.
5. **`WorkerJobHandler.java`** — Test execution boundary hook.
6. **`Worker.java`** — Thread pool loop, blocking acquisition, graceful shutdown.
7. **`WorkerTest.java`** — Multi-threaded acquisition, heartbeat, and shutdown verification.

## 11. Testing Requirements

### Integration Tests (Requires Redis)
- **Registration & Liveness:**
  - `Worker.start()` registers worker in `RedisWorkerRegistry`.
  - Heartbeat key exists with TTL in Redis.
  - `Worker.stop()` deregisters worker from Redis.
- **End-to-End Acquisition Loop:**
  - Submit 10 jobs to queue.
  - Start Worker with 2 threads.
  - Handler counts processed jobs using `CountDownLatch`.
  - Assert all 10 jobs transition `QUEUED` → `PROCESSING` → `COMPLETED` in `JobRepository`.
- **Graceful Shutdown:**
  - Submit long-running task.
  - Call `worker.stop()`.
  - Assert active task completes and its status becomes `COMPLETED`.
  - Assert no subsequent jobs are taken from the queue.

## 12. Failure and Edge Cases

- Redis temporarily disconnects during polling → worker catches exception, logs warning, backs off briefly, retries without crashing thread.
- Worker process crashes → no deregistration occurs; its heartbeat TTL eventually expires, while static worker metadata may remain for inspection. Recovery of any abandoned `PROCESSING` job is deferred to the later reliability/recovery phase.
- Handler throws `RuntimeException` → worker catches `Throwable`, updates job to `FAILED`, and worker thread continues processing next job.
- `JobId` popped from queue but missing in `JobRepository` → log error and continue (orphaned ID).

## 13. Validation

```powershell
# Compile
mvn compile

# Run Phase 4 tests
mvn test -Dtest="WorkerIdTest,RedisWorkerRegistryTest,WorkerTest"

# Verify worker heartbeat in Redis
redis-cli KEYS "jobstream:worker:*"
redis-cli TTL "jobstream:worker:<id>:heartbeat"
```

## 14. Completion Criteria

- [ ] `WorkerId`, `WorkerStatus`, and `WorkerInfo` defined.
- [ ] `RedisWorkerRegistry` tracks active workers and heartbeats with TTL.
- [ ] `Worker` polls `JobQueue` for `JobId`, fetches `Job` from `JobRepository`, marks `PROCESSING`.
- [ ] Graceful shutdown drains in-flight jobs within configured timeout.
- [ ] All unit and integration tests pass cleanly (`mvn test` exits 0).

## 15. Self-Review Checklist

- [ ] Does worker acquire `JobId` and retrieve `Job` from repository? (Must be YES).
- [ ] Is `WorkerJobHandler` an explicit test hook rather than a fake production execution engine?
- [ ] Does worker catch `Throwable` to prevent thread pool death?
- [ ] Are all previous tests passing?

## 16. Documentation Updates

### Requirements documentation
- Verify `docs/requirements/worker-model.md` matches implemented acquisition flow.

### Architecture documentation
- Confirm `docs/architecture/system-overview.md` reflects worker registry keys.

### ADRs
- **Create ADR-006:** Worker Threading Model & Heartbeat TTL Architecture.

### Final README Notes
- Multi-threaded workers poll named queues using blocking operations.
- Redis-backed registry tracks worker liveness with automatic heartbeat expiration.

## 17. Git Milestone

- **Branch:** `phase-4-worker-foundation`
- **Base:** `master` (after Phase 3 merge)
- **Implementation milestone:** Worker lifecycle, heartbeat, and processing loop functional.
- **Testing milestone:** Multi-threaded acquisition and shutdown verified.
- **Review milestone:** ADR-006 created, self-review complete.
- **Merge condition:** Clean Maven build and test pass on master.

## 18. What This Phase Enables

With worker infrastructure in place:
- **Phase 5 (Execution Engine)** can replace the test handler with the pluggable `JobExecutor` abstraction and `ExecutorRegistry`, connecting real business logic to the worker loop.
