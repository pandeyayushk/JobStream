# Phase 6 — Reliability & Retry

## 1. Objective

Implement fault tolerance, automatic retry policies with configurable backoff strategies, and a Dead-Letter Queue (DLQ) for permanently failed jobs. Connect retry evaluation to the worker loop while preserving worker availability (non-blocking retry scheduling).

## 2. Why This Phase Exists

In a distributed job queue, failures are inevitable: transient network hiccups, database lock timeouts, or external service downtime. Without reliability mechanisms:
- Any transient error causes immediate, permanent job failure.
- Repeated immediate retries can overwhelm struggling downstream services (thundering herd problem).
- Poison-pill jobs (unrecoverable bugs) block queues or fail indefinitely without operator isolation.
- **Worker Availability Rule:** A worker thread must **never** be put to sleep waiting for a retry delay. Worker threads must remain available to process other ready jobs immediately.

**Depends on:**
- Phase 1 (Domain Model: `Job`, `JobStatus` — includes `RETRYING` and `DEAD`)
- Phase 2 (Persistence: `JobRepository`)
- Phase 3 (Queue: `JobQueue`)
- Phase 4 (Worker: `Worker` loop)
- Phase 5 (Execution Engine: `JobExecutor`, `ExecutionResult`)

## 3. Scope

**In scope:**
- Retry tracking in `Job` (`retryCount`, `maxRetries`, `lastErrorReason`, `lastFailedAt`)
- `RetryPolicy` abstraction (fixed delay, exponential backoff with jitter, no retry)
- Non-blocking retry scheduling concept: worker updates job status to `RETRYING` and re-enqueues without blocking the worker thread
- `DeadLetterQueue` interface and `RedisDeadLetterQueue` implementation (`jobstream:queue:dead-letter`)
- Integration with worker failure loop:
  - If `shouldRetry(...) == true`: increment retry count, set status `RETRYING`, re-enqueue.
  - If `shouldRetry(...) == false` or retries exhausted: set status `DEAD`, push to DLQ.
- DLQ inspection and administrative operations: list dead jobs, purge, and manual re-enqueueing

**Out of scope:**
- Non-blocking arbitrary time delays via Redis Sorted Sets (owned by Phase 8: `JobScheduler`)
- Distributed dead-worker detection and recovery of stranded `PROCESSING` jobs (Phase 10)

## 4. Prerequisites

- Phase 5 complete (`Worker` executes jobs via `JobExecutor` and captures `ExecutionResult.failure`)
- Redis running locally
- Understanding of backoff algorithms (jitter, exponential growth) and non-blocking queue scheduling

## 5. Read Before Starting

- `docs/requirements/retry-and-failure.md` — Retry and DLQ requirements
- `docs/architecture/job-lifecycle.md` — Lifecycle transitions `FAILED → RETRYING → QUEUED` and `FAILED → DEAD`
- `docs/architecture/project-structure.md` — Package location for `retry`
- `docs/architecture/system-overview.md` — Reliability layer flow

## 6. Concepts to Understand

- **Worker Availability & Non-Blocking Retry:**
  - *Anti-Pattern:* Calling `Thread.sleep(delay)` inside a worker thread blocks that thread from processing any other jobs during the backoff period.
  - *Target Pattern:* Worker records the retry attempt, marks status `RETRYING`, schedules the job for future re-enqueueing, and **immediately returns to the queue** to process the next ready job.
- **Exponential Backoff & Full Jitter:**
  - Mitigates thundering herds by exponentially increasing delay with randomized jitter:
    $$\text{delay} = \min(\text{maxDelay}, \text{baseDelay} \times 2^{\text{attempt}}) \times \text{random}(0.5, 1.5)$$
  - Reference: AWS Architecture Blog: "Exponential Backoff And Jitter"
- **Dead-Letter Queue (DLQ):**
  - Dedicated storage list (`jobstream:queue:dead-letter`) holding `JobId`s of jobs that have exhausted all retries.
  - Keeps poisoned jobs from polluting active queues while preserving full error history for operator inspection.
- **Phase 6 vs Phase 8 Scheduling Boundary:**
  - In Phase 6, immediate re-enqueue (with incremented attempt count) or short timer-based test delays provide the baseline retry scheduling mechanism.
  - Phase 8 introduces Redis Sorted Sets (`ZSET`) to provide arbitrary, persistent, non-blocking time delays for delayed retries.

## 7. Design Decisions

### 7.1 Retry State Storage: Core Job Entity

**Decision:** Store `retryCount`, `maxRetries`, `lastErrorReason`, and `lastFailedAt` directly on the `Job` entity.
- Keeps the job record self-contained and inspectable across all persistence boundaries.
- Stored and retrieved through existing `JobRepository` without secondary tracking tables.

**Record this decision in:** ADR-008 (to be created during this phase).

### 7.2 DLQ Storage Schema in Redis

**Decision:** Dedicated Redis list `jobstream:queue:dead-letter` storing `JobId` references, matching regular `JobQueue` storage.
- The job entity status is marked `DEAD` in `JobRepository`.
- Diagnostic failure reasons are stored in the job's metadata and `lastErrorReason` field.

### 7.3 Backoff Execution Strategy

**Decision:**
- For Phase 6 baseline: The worker does **not** block with `Thread.sleep()`. When a retryable failure occurs:
  1. Increment `job.retryCount`.
  2. Set status to `RETRYING`.
  3. For zero-delay policies or immediate retry: re-enqueue `job.getId()` into `JobQueue`.
  4. For policies specifying backoff: record `nextRunAt` timestamp. (In Phase 8, this hands off to `JobScheduler` for non-blocking sorted-set scheduling).
- Worker thread immediately continues to the next job in the queue.

## 8. Requirements

### Functional Requirements

1. **Job Entity Retry Extensions (package `io.github.pandeyayushk.jobstream.job`):**
   - Accessors: `int getRetryCount()`, `int getMaxRetries()`, `Optional<String> getLastErrorReason()`, `Optional<Instant> getLastFailedAt()`.
   - Transition helper: `Job withRetryAttempt(String failureReason)` returning updated `Job` with incremented `retryCount`, updated timestamp, and status `RETRYING`.

2. **`RetryPolicy` Interface (package `io.github.pandeyayushk.jobstream.retry`):**
   - `boolean shouldRetry(Job job, Throwable cause)`
   - `Duration computeBackoff(Job job)`
   - Implementations:
     - `NoRetryPolicy`: Always returns `false`.
     - `FixedDelayRetryPolicy`: Returns fixed backoff duration; retries while `retryCount < maxRetries`.
     - `ExponentialBackoffRetryPolicy`: Calculates exponential delay with randomized jitter; caps at max duration.

3. **`DeadLetterQueue` Interface & `RedisDeadLetterQueue`:**
   - `void moveToDeadLetter(Job job, String reason)`: Sets status to `DEAD` in repository and pushes `JobId` to DLQ list.
   - `List<Job> listDeadJobs(int offset, int limit)`: Queries dead jobs by popping IDs and fetching from repository.
   - `Optional<Job> requeue(JobId jobId, String targetQueue)`: Atomically removes from DLQ, sets status `QUEUED`, resets retry count, and pushes to target queue.
   - `void purge()`: Clears all dead jobs from the DLQ list.
   - `long size()`: Returns count of dead jobs.

4. **Worker Failure Integration:**
   - When `ExecutionResult.isSuccess()` is `false`:
     - Evaluate `retryPolicy.shouldRetry(job, result.getErrorCause().orElse(null))`.
     - If `true` and `job.getRetryCount() < job.getMaxRetries()`:
       - Update job via `withRetryAttempt(...)` in `JobRepository`.
       - Re-enqueue `job.getId()` into queue.
       - Worker thread immediately moves to next job (non-blocking).
     - If `false` or retries exhausted:
       - Move job to `DeadLetterQueue`.
       - Log WARN/ERROR with diagnostic details.

### Non-Functional Requirements

- **Zero Worker Starvation:** Worker threads must never sleep or block waiting for retry delays.
- **Data Preservation:** A job that fails permanently must never disappear from the system; it must be safely stored in the DLQ.

## 9. File Change Manifest

### CREATE

```
src/main/java/io/github/pandeyayushk/jobstream/retry/RetryPolicy.java
src/main/java/io/github/pandeyayushk/jobstream/retry/NoRetryPolicy.java
src/main/java/io/github/pandeyayushk/jobstream/retry/FixedDelayRetryPolicy.java
src/main/java/io/github/pandeyayushk/jobstream/retry/ExponentialBackoffRetryPolicy.java
src/main/java/io/github/pandeyayushk/jobstream/retry/DeadLetterQueue.java
src/main/java/io/github/pandeyayushk/jobstream/retry/RedisDeadLetterQueue.java
src/test/java/io/github/pandeyayushk/jobstream/retry/RetryPolicyTest.java
src/test/java/io/github/pandeyayushk/jobstream/retry/RedisDeadLetterQueueTest.java
src/test/java/io/github/pandeyayushk/jobstream/retry/WorkerRetryIntegrationTest.java
```

### MODIFY

```
src/main/java/io/github/pandeyayushk/jobstream/job/Job.java         — Add retry tracking fields and withRetryAttempt helper
src/main/java/io/github/pandeyayushk/jobstream/worker/Worker.java   — Integrate RetryPolicy and DeadLetterQueue into failure handler
src/test/java/io/github/pandeyayushk/jobstream/job/JobTest.java     — Add tests for retry fields and copy helpers
```

### DELETE

None.

### DO NOT TOUCH

```
src/main/java/.../job/JobStatus.java   — All 7 lifecycle states already defined in Phase 1
src/main/java/.../serialization/*
src/main/java/.../config/*
pom.xml
```

## 10. Implementation Order

1. **`Job.java`** — Add `retryCount`, `maxRetries`, `lastErrorReason`, and `withRetryAttempt` methods.
2. **`JobTest.java`** — Unit tests for retry tracking fields and immutability.
3. **`RetryPolicy.java`**, **`FixedDelayRetryPolicy.java`**, **`ExponentialBackoffRetryPolicy.java`** — Math and policy rules.
4. **`RetryPolicyTest.java`** — Unit tests for backoff progression, capping, and jitter bounds.
5. **`DeadLetterQueue.java`** & **`RedisDeadLetterQueue.java`** — Quarantine storage and requeue logic.
6. **`RedisDeadLetterQueueTest.java`** — Integration tests for DLQ list, requeue, and purge.
7. **`Worker.java`** — Wire retry evaluation and DLQ routing into worker failure path.
8. **`WorkerRetryIntegrationTest.java`** — End-to-end failure, non-blocking retry, and DLQ exhaustion tests.

## 11. Testing Requirements

### Unit Tests
- `ExponentialBackoffRetryPolicyTest`:
  - Delay increases exponentially with attempt count.
  - Delay never exceeds configured `maxBackoff`.
  - Full jitter produces values within expected randomized range.
  - `shouldRetry()` returns `false` when `attempt >= maxRetries`.
- `FixedDelayRetryPolicyTest`:
  - Constant delay returned across all attempts.
- `JobTest`:
  - `withRetryAttempt` increments counter, sets status to `RETRYING`, updates timestamp, and leaves original instance unchanged.

### Integration Tests (Requires Redis)
- **Non-Blocking Retry Flow:**
  - Submit job that fails on first execution, succeeds on second.
  - Worker fails job once → status in repository becomes `RETRYING` → job re-enqueued → worker re-executes and succeeds → status becomes `COMPLETED`.
  - Worker thread does not block during retry processing.
- **DLQ Quarantine Flow:**
  - Submit job that consistently fails with maxRetries = 2.
  - Job executes and fails attempt 1 → re-enqueued.
  - Job executes and fails attempt 2 → retries exhausted.
  - Job status in repository becomes `DEAD`.
  - `DeadLetterQueue.size()` is 1; job appears in `listDeadJobs()`.
  - Active queue is empty.
- **Manual Requeue Flow:**
  - Call `deadLetterQueue.requeue(jobId, "default")`.
  - Job is removed from DLQ, status reset to `QUEUED`, appears in active queue.
  - Worker dequeues and processes job.

## 12. Failure and Edge Cases

- Worker crashes during DLQ transfer → Redis transaction or atomic list push ensures job reference is not lost.
- Negative or zero `maxRetries` → rejected with `IllegalArgumentException`.
- Requeuing a non-existent `JobId` from DLQ → returns `Optional.empty()` safely.

## 13. Validation

```powershell
# Compile
mvn compile

# Run Phase 6 tests
mvn test -Dtest="RetryPolicyTest,RedisDeadLetterQueueTest,WorkerRetryIntegrationTest"

# Inspect DLQ in Redis
redis-cli LLEN jobstream:queue:dead-letter
redis-cli LRANGE jobstream:queue:dead-letter 0 -1
```

## 14. Completion Criteria

- [ ] `Job` stores retry metadata with immutable transition helper.
- [ ] `RetryPolicy` strategies (Fixed, Exponential with jitter) calculate delays correctly.
- [ ] `DeadLetterQueue` quarantines permanently failed jobs with full diagnostics.
- [ ] Worker evaluates retry policy and re-enqueues without blocking worker threads.
- [ ] Exhausted jobs transition to `DEAD` and route to DLQ list.
- [ ] Requeuing from DLQ places jobs back into active queues.
- [ ] `JobStatus` was **not** modified (all states were already present from Phase 1).
- [ ] All unit and integration tests pass cleanly (`mvn test` exits 0).

## 15. Self-Review Checklist

- [ ] Are worker threads kept non-blocking during retries? (Must be YES).
- [ ] Did we avoid calling `Thread.sleep()` in worker threads?
- [ ] Does `DeadLetterQueue` maintain key separation (`jobstream:queue:dead-letter`)?
- [ ] Did we avoid modifying `JobStatus.java`?
- [ ] Are all previous phase tests intact and passing?

## 16. Documentation Updates

### Requirements documentation
- Update `docs/requirements/retry-and-failure.md` with tested backoff progression.

### Architecture documentation
- Confirm `docs/architecture/job-lifecycle.md` matches implemented `RETRYING` and `DEAD` transitions.

### ADRs
- **Create ADR-008:** Non-Blocking Retry Architecture & Dead-Letter Queue Design.

### Final README Notes
- Configurable retry policies with non-blocking re-enqueueing and exponential backoff jitter.
- Dead-Letter Queue quarantines exhausted failures and supports operator requeueing.

## 17. Git Milestone

- **Branch:** `phase-6-reliability-and-retry`
- **Base:** `master` (after Phase 5 merge)
- **Implementation milestone:** `RetryPolicy`, `DeadLetterQueue`, and worker retry integration complete.
- **Testing milestone:** Non-blocking retry and DLQ routing verified by integration tests.
- **Review milestone:** ADR-008 created, self-review complete.
- **Merge condition:** Clean Maven build and test pass on master.

## 18. What This Phase Enables

With reliability and retry in place:
- **Phase 7 (CLI)** can provide operators with commands to inspect DLQ contents, diagnose failure causes, and trigger dead-letter requeueing.
- The queue is resilient against transient failures without sacrificing worker throughput.
