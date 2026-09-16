# Phase 5 — Execution Engine

## 1. Objective

Decouple job business logic from worker machinery. Introduce a pluggable `JobExecutor` abstraction (Strategy Pattern) and a thread-safe `ExecutorRegistry` to route job types to their respective handlers. Integrate execution with the `Worker` loop, replacing the Phase 4 test handler with real, type-dispatched execution.

## 2. Why This Phase Exists

In Phase 4, workers acquired jobs and ran a minimal test handler. In production:
- Workers must remain pure, generic infrastructure agnostic of business logic.
- Different job types (`email:send`, `report:generate`, `order:process`) require completely distinct execution logic.
- Adding a new job type must satisfy the **Open-Closed Principle (OCP)** — requiring zero modifications to the worker core.
- **Strict Architectural Relationship:**
  - `Worker` depends on the `JobExecutor` abstraction.
  - `JobExecutor` knows **nothing** about `Worker`, threads, or Redis. It receives a `Job` (with `Payload`) and returns an `ExecutionResult`.

**Depends on:**
- Phase 1 (Domain Model: `Job`, `JobId`, `JobStatus`, `Payload`)
- Phase 2 (Persistence: `JobRepository`)
- Phase 3 (Queue: `JobQueue`)
- Phase 4 (Worker: `Worker` loop)

## 3. Scope

**In scope:**
- `JobExecutor` interface representing executable business logic
- `ExecutionResult` domain value object (success, failure, error cause, exit codes)
- `ExecutorRegistry` interface and thread-safe `DefaultExecutorRegistry`
- Integrating `ExecutorRegistry` into `Worker`
- Status transitions based on execution outcome:
  - `PROCESSING → COMPLETED` (on success)
  - `PROCESSING → FAILED` (on failure or exception)
- Updating `JobRepository` with completion/failure timestamps and diagnostic metadata
- Built-in test executors (`NoOpExecutor`, `FailingExecutor`)

**Out of scope:**
- Automatic retry policies, backoff calculation, and DLQ routing (Phase 6)
- Execution timeouts with thread interruption / process isolation (Phase 6 / Phase 10)
- Classpath reflection scanning (deferred to Phase 10)

## 4. Prerequisites

- Phase 4 complete (`Worker` successfully acquires jobs from `JobQueue` and loads from `JobRepository`)
- Redis running locally for integration verification
- Understanding of Strategy Pattern and Java functional interfaces

## 5. Read Before Starting

- `docs/requirements/executor-system.md` — Execution engine requirements
- `docs/architecture/project-structure.md` — Package rules (`executor` depends only on `job`, `payload`)
- `docs/architecture/system-overview.md` — Execution engine placement in data flow
- `docs/architecture/job-lifecycle.md` — `PROCESSING → COMPLETED / FAILED` transitions
- `docs/versions/v4-worker-foundation/README.md` — Worker loop implementation

## 6. Concepts to Understand

- **Strategy Pattern (Gang of Four):** Encapsulating execution algorithms in independent classes implementing a common `JobExecutor` contract.
- **Open-Closed Principle (SOLID):** Open for extension (registering new executors), closed for modification (worker processing loop).
- **Decoupled Architectural Flow:**
  ```
  [ Worker ] ──> invokes ──> [ ExecutorRegistry ] ──> gets ──> [ JobExecutor ]
                                                                      │
                                                            receives: [ Job / Payload ]
                                                             returns: [ ExecutionResult ]
  ```
- **Checked vs Unchecked Exceptions in Thread Pools:** Worker must catch `Throwable` to intercept all exceptions and errors thrown inside user job code.
- **Result Object Pattern:** Returning `ExecutionResult` rather than using thrown exceptions for expected business errors.

## 7. Design Decisions

### 7.1 Executor Interface Contract

**Decision:** `JobExecutor` accepts `Job` and returns `ExecutionResult`:
```java
@FunctionalInterface
public interface JobExecutor {
    ExecutionResult execute(Job job);
}
```
- Accepts the full domain `Job`, providing access to `job.getPayload()`, `job.getId()`, `job.getType()`, and `job.getMetadata()`.
- Returns a typed `ExecutionResult`. This avoids forcing all failures to be exceptions, and enables fine-grained error reasons.

**Record this decision in:** ADR-007 (to be created during this phase).

### 7.2 Missing Executor Policy

**Decision:** If a worker acquires a job whose `type` has no registered `JobExecutor`, the worker marks the job as `FAILED` with an error message: `"No executor registered for job type: <type>"`. The worker then continues to the next job.
- Never discard silently (prevents data loss).
- Never re-enqueue in a fast loop (prevents infinite CPU-spinning starvation).

### 7.3 Thread Safety in Registries

**Decision:** `DefaultExecutorRegistry` uses `java.util.concurrent.ConcurrentHashMap` to store type-to-executor mappings, guaranteeing thread-safe concurrent read access during worker execution.

## 8. Requirements

### Functional Requirements

1. **`JobExecutor` Interface (package `io.github.pandeyayushk.jobstream.executor`):**
   - Method: `ExecutionResult execute(Job job)`
   - Functional interface supporting lambda implementations in tests.
   - Allowed dependencies: `job`, `payload` (ZERO dependency on `worker`).

2. **`ExecutionResult` Value Object:**
   - Factory methods:
     - `ExecutionResult.success()`
     - `ExecutionResult.failure(String message)`
     - `ExecutionResult.failure(Throwable cause)`
   - Methods: `boolean isSuccess()`, `Optional<String> getErrorMessage()`, `Optional<Throwable> getErrorCause()`.

3. **`ExecutorRegistry` Interface & `DefaultExecutorRegistry`:**
   - `void register(String jobType, JobExecutor executor)`
   - `Optional<JobExecutor> get(String jobType)`
   - `boolean hasExecutor(String jobType)`
   - Thread-safe for concurrent read and write operations.

4. **Worker Integration:**
   - Refactor `Worker` constructor to accept `ExecutorRegistry`.
   - In worker processing loop:
     1. Look up executor for `job.getType()`.
     2. If found: invoke `executor.execute(job)` inside a `try-catch (Throwable t)` block.
     3. If `result.isSuccess()`: transition status to `COMPLETED` in `JobRepository`.
     4. If result failed or threw: transition status to `FAILED`, record failure reason in job, save in `JobRepository`.
     5. If executor not found: transition status to `FAILED` with missing executor diagnostic.

### Non-Functional Requirements

- **Fault Isolation:** Malfunctioning executor code (e.g. infinite loop, thrown `RuntimeException`, `Error`) must never terminate the worker thread.
- **Performance:** O(1) registry dispatch lookup.

## 9. File Change Manifest

### CREATE

```
src/main/java/io/github/pandeyayushk/jobstream/executor/JobExecutor.java
src/main/java/io/github/pandeyayushk/jobstream/executor/ExecutionResult.java
src/main/java/io/github/pandeyayushk/jobstream/executor/ExecutorRegistry.java
src/main/java/io/github/pandeyayushk/jobstream/executor/DefaultExecutorRegistry.java
src/test/java/io/github/pandeyayushk/jobstream/executor/ExecutionResultTest.java
src/test/java/io/github/pandeyayushk/jobstream/executor/DefaultExecutorRegistryTest.java
src/test/java/io/github/pandeyayushk/jobstream/executor/WorkerExecutionIntegrationTest.java
```

### MODIFY

```
src/main/java/io/github/pandeyayushk/jobstream/worker/Worker.java       — Replace WorkerJobHandler test hook with ExecutorRegistry
src/test/java/io/github/pandeyayushk/jobstream/worker/WorkerTest.java   — Pass ExecutorRegistry with mock/stub executor
```

### DELETE

None. (Retain `WorkerJobHandler` as deprecated or replace directly).

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

1. **`ExecutionResult.java`** — Result value object with success/failure factories.
2. **`ExecutionResultTest.java`** — Unit tests for result state.
3. **`JobExecutor.java`** — Core execution strategy interface.
4. **`ExecutorRegistry.java`** & **`DefaultExecutorRegistry.java`** — Thread-safe registry.
5. **`DefaultExecutorRegistryTest.java`** — Unit tests for registry operations and thread safety.
6. **`Worker.java`** — Refactor to wire `ExecutorRegistry` into the acquisition loop.
7. **`WorkerExecutionIntegrationTest.java`** — End-to-end tests: enqueue job → worker dequeues → executor runs → verify repository status.

## 11. Testing Requirements

### Unit Tests
- `DefaultExecutorRegistryTest`:
  - Register and retrieve executor by type.
  - Unregistered type returns `Optional.empty()`.
  - Re-registering overwrites existing executor cleanly.
  - Concurrent reads and writes succeed without exceptions.
- `ExecutionResultTest`:
  - `success()` has `isSuccess() == true` and empty error.
  - `failure("reason")` captures message.
  - `failure(cause)` captures message and throwable.

### Integration Tests (Requires Redis)
- **Successful Execution:** Job with registered executor finishes; status in repository transitions to `COMPLETED`.
- **Business Failure:** Executor returns `ExecutionResult.failure("Invalid email")`; status in repository transitions to `FAILED`.
- **Thrown Exception:** Executor throws `IllegalStateException("DB connection down")`; caught safely, status in repository transitions to `FAILED`, worker thread continues running.
- **Unregistered Job Type:** Job with unknown type transitions to `FAILED` with `"No executor registered..."`, worker thread continues to next job.
- **Multi-Type Dispatch:** Single worker instance correctly dispatches `"email:send"` to EmailExecutor and `"report:build"` to ReportExecutor.

## 12. Failure and Edge Cases

- Executor returns `null` instead of `ExecutionResult` → defensively treat as `FAILED` with `"Executor returned null ExecutionResult"`.
- Executor throws `OutOfMemoryError` or `StackOverflowError` → caught via `Throwable`, logged at ERROR, job marked `FAILED`.
- Executor modifies input `Payload` → prevented because `Payload` is immutable.

## 13. Validation

```powershell
# Compile
mvn compile

# Run Phase 5 tests
mvn test -Dtest="ExecutionResultTest,DefaultExecutorRegistryTest,WorkerExecutionIntegrationTest"
```

## 14. Completion Criteria

- [ ] `JobExecutor` defined with zero dependency on `Worker`.
- [ ] `ExecutionResult` models success and failure outcomes.
- [ ] `DefaultExecutorRegistry` provides thread-safe registration and lookup.
- [ ] `Worker` delegates execution exclusively through `ExecutorRegistry`.
- [ ] Unhandled exceptions inside executors are contained; worker threads never die.
- [ ] Missing executors fail fast with status `FAILED`.
- [ ] All unit and integration tests pass cleanly (`mvn test` exits 0).

## 15. Self-Review Checklist

- [ ] Does `JobExecutor` know anything about `Worker`? (Must be strictly NO).
- [ ] Does `DefaultExecutorRegistry` use `ConcurrentHashMap`?
- [ ] Does `Worker` catch `Throwable` around `executor.execute()`?
- [ ] Are Phase 1 through Phase 4 tests continuing to pass?

## 16. Documentation Updates

### Requirements documentation
- Confirm `docs/requirements/executor-system.md` reflects implemented interfaces.

### Architecture documentation
- Verify `docs/architecture/system-overview.md` data flow matches executor dispatch.

### ADRs
- **Create ADR-007:** JobExecutor Strategy Pattern and Error Containment Model.

### Final README Notes
- Pluggable, type-dispatched execution engine adhering to the Open-Closed Principle.
- Complete failure isolation protecting worker threads from user code bugs.

## 17. Git Milestone

- **Branch:** `phase-5-execution-engine`
- **Base:** `master` (after Phase 4 merge)
- **Implementation milestone:** `JobExecutor`, `ExecutionResult`, `DefaultExecutorRegistry`, and `Worker` integration complete.
- **Testing milestone:** Exception containment and multi-type dispatch verified.
- **Review milestone:** ADR-007 recorded, self-review complete.
- **Merge condition:** Clean Maven build and test pass on master.

## 18. What This Phase Enables

With the execution engine in place:
- **Phase 6 (Reliability & Retry)** can evaluate failed job executions (`FAILED` status and failure cause), apply retry policies with backoff, and route exhausted jobs to the Dead-Letter Queue (DLQ).
- Real business workloads can now be processed by JobStream.
