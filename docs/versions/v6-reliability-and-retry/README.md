# Phase 6 — Reliability & Retry

**Status: Complete.** This guide records the behavior implemented in the repository; it is not an implementation checklist.

## 1. Scope

Phase 6 adds retry state to `Job`, policy-based decisions and backoff, Worker failure integration, in-memory scheduled retry requeueing, a Redis Dead-Letter Queue, and manual DLQ requeue. Durable arbitrary delayed scheduling is explicitly out of scope and belongs to Phase 8.

## 2. Job retry state

Each Job stores `retryCount`, `maxRetries`, `lastErrorReason`, and `lastFailedAt`. `maxRetries` means retries allowed after the initial attempt: `maxRetries=2` permits initial execution, retry 1, and retry 2 (three executions maximum). Values of `maxRetries <= 0`, negative retry counts, or counts above the maximum are rejected.

`withRetryAttempt(reason)` accepts a `FAILED` Job with retries remaining and returns `RETRYING` with incremented count and updated failure diagnostics. `resetRetryForRequeue()` accepts a `DEAD` Job and returns `QUEUED` with retry count zero; diagnostics remain on the Job.

## 3. Policies and backoff

`RetryPolicy` defines `shouldRetry(Job, Throwable)` and `computeBackoff(Job)`. `NoRetryPolicy` always declines and returns zero delay. `FixedDelayRetryPolicy` returns its configured non-negative delay. `ExponentialBackoffRetryPolicy` permits attempts while `retryCount < maxRetries` and computes:

```text
preJitterMillis = min(baseBackoffMillis * 2^retryCount, maxBackoffMillis)
delayMillis = round(preJitterMillis * random(0.5, 1.5))
```

Jitter is applied after the cap; therefore the final delay may exceed `maxBackoff` (up to approximately 1.5 times the capped amount). Computation uses milliseconds.

## 4. Worker retry lifecycle

On execution failure Worker persists `FAILED` and failure diagnostics. If policy allows another attempt, it saves `RETRYING` and schedules a task on its separate retry scheduler. That scheduled task later verifies the Job remains `RETRYING`, persists `QUEUED`, and adds the ID to the active queue. The processing thread returns immediately and can process another ready job.

```text
PROCESSING -> FAILED -> RETRYING -> QUEUED -> PROCESSING
```

If policy declines, retries are exhausted, or policy/backoff evaluation fails, the Job is persisted as `DEAD` and routed to the DLQ.

```text
PROCESSING -> FAILED -> DEAD -> DLQ
```

Pending timer tasks are in memory and can be lost on process restart. Phase 6 does not persist arbitrary delayed work or `nextRunAt` timestamps.

The current Worker acquisition loop uses `dequeueNonBlocking()` and waits 50 ms between empty-queue checks. This avoids a Redis blocking dequeue keeping acquisition alive during `Worker.stop()`. This idle acquisition wait is distinct from retry delay, which uses the retry scheduler and does not sleep a processing thread.

## 5. Dead-Letter Queue

`DeadLetterQueue` defines move, paginated listing, manual requeue, purge, and size operations. `RedisDeadLetterQueue` uses `jobstream:queue:dead-letter` for Job IDs; complete DEAD Jobs remain in the authoritative repository record and status index. Listing loads records by ID and skips missing records. Purge deletes the DLQ list, not the Job records.

Manual requeue returns empty if the Job is absent or not `DEAD`. Otherwise a Redis `MULTI`/`EXEC` transaction removes the DLQ ID, changes the Job to `QUEUED`, resets retry count, updates status indexes, and adds the ID to the requested queue. The latest failure diagnostic fields are preserved.

## 6. Redis resource lifecycle

Redis transactions in `RedisJobSubmissionStore`, `RedisWorkerRegistry`, and `RedisDeadLetterQueue` use try-with-resources. The injected Redis client is owned by its creator; these components do not close it.

## 7. Phase 6 test coverage

The repository tests cover:

- Job retry state, incrementing, reset-on-requeue, and invalid retry limits (`JobTest`).
- Fixed-delay and no-retry policy behavior (`FixedDelayRetryPolicyTest`, `NoRetryPolicyTest`).
- Exponential delay progression and tested jitter bounds (`ExponentialBackoffRetryPolicyTest`).
- Serialization and repository persistence/reload of retry fields (`JacksonJobSerializerTest`, `RedisJobRepositoryTest`).
- Redis DLQ move, listing, size, purge, manual requeue, and missing/non-dead jobs (`RedisDeadLetterQueueTest`).
- Worker eventual retry completion, failure diagnostics, exhausted retries, DLQ manual requeue, missing DLQ job, invalid retry limits, and processing another ready job while a retry is scheduled (`WorkerRetryIntegrationTest`).

The Redis integration tests require a real Redis instance. No test count is stated here.

## 8. Phase 6 / Phase 8 responsibility boundary

- **Phase 6 complete:** retry policy, retry state persistence, in-memory scheduled retries, failure handling, DLQ, and manual requeue.
- **Phase 8 planned:** persistent delayed scheduling with Redis Sorted Sets (ZSET), durable next-run timestamps, and arbitrary long retry delays; scheduling/priority features are not yet implemented.

## 9. Related documents

- [Retry and Failure Requirements](../../requirements/retry-and-failure.md)
- [Job Lifecycle](../../architecture/job-lifecycle.md)
- [ADR-008](../../decisions/ADR-008-retry-and-failure.md)
