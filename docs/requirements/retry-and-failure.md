# Retry and Failure Handling Requirements

This document records the Phase 6 retry and dead-letter behavior present in JobStream.

## 1. Job retry state and semantics

`Job` stores `retryCount`, `maxRetries`, `lastErrorReason`, and `lastFailedAt`. `maxRetries` means the number of retries allowed after the initial execution. For example, `maxRetries=2` allows an initial attempt plus retry 1 and retry 2 (three executions maximum). `Job` requires `maxRetries > 0`, `retryCount >= 0`, and `retryCount <= maxRetries`.

`withRetryAttempt(reason)` accepts only a `FAILED` Job, rejects blank/null reasons and exhausted retry allowance, increments the count, records the failure reason/time, and returns a `RETRYING` Job. `resetRetryForRequeue()` accepts only `DEAD`, returns `QUEUED` with retry count reset to zero, and retains failure diagnostics.

## 2. RetryPolicy

The interface exposes `shouldRetry(Job job, Throwable cause)` and `computeBackoff(Job job)`.

- `NoRetryPolicy.shouldRetry` is always false and its backoff is zero.
- `FixedDelayRetryPolicy` returns its configured non-negative delay and permits retry while `retryCount < maxRetries`.
- `ExponentialBackoffRetryPolicy` uses the same retry-count rule. It calculates `capped = min(baseBackoffMillis * 2^retryCount, maxBackoffMillis)`, then multiplies that cap by a random value in `[0.5, 1.5)` and rounds to the nearest millisecond. Jitter is applied after the cap, so the resulting delay can exceed `maxBackoff` (up to approximately 1.5 times the cap); the configured maximum caps the pre-jitter delay only. The implementation computes using millisecond precision.

Both built-in retry-capable policies require non-null Job/cause arguments for `shouldRetry`, and non-null Job for `computeBackoff`. Exponential policy requires positive base and maximum durations with base no greater than maximum.

## 3. Worker failure and retry flow

Worker persists `FAILED` with `failure.reason` metadata, then evaluates the policy. If retry is allowed and the retry limit is not exhausted, Worker persists `RETRYING` and schedules a task on its separate scheduled retry executor. When due, that task reloads the Job, verifies it remains `RETRYING`, persists `QUEUED`, and enqueues its `JobId`.

```text
PROCESSING -> FAILED -> RETRYING -> QUEUED -> PROCESSING
```

The processing task does not wait for the delay; another ready Job can use the processing capacity. The retry schedule is in memory and does not survive Worker/JVM restart. Phase 6 does not provide persistent arbitrary delayed scheduling.

If policy declines retry, attempts are exhausted, or a retry policy/backoff evaluation fails, Worker routes the failed Job to the DLQ:

```text
PROCESSING -> FAILED -> DEAD -> DLQ
```

## 4. DeadLetterQueue

`DeadLetterQueue` defines `moveToDeadLetter(Job, reason)`, `listDeadJobs(offset, limit)`, `requeue(JobId, targetQueue)`, `purge()`, and `size()`. `RedisDeadLetterQueue` stores Job IDs in `jobstream:queue:dead-letter`; full `DEAD` Job records remain in the authoritative `jobstream:job:<id>` record, and the status index is updated.

`listDeadJobs` reads a list page and loads corresponding records; missing records are skipped. `purge` removes the DLQ list key; it does not delete persisted dead Job records. `size` returns the list length. Manual `requeue` returns empty when the Job does not exist or is not `DEAD`. Otherwise it resets retry count, persists `QUEUED`, removes the ID from the DLQ, updates status indexes, and pushes it to the requested queue in a Redis `MULTI`/`EXEC` transaction.

## 5. Worker acquisition and shutdown

The current Worker acquisition loop uses `dequeueNonBlocking()` and waits 50 ms when the queue is empty. This allows shutdown to stop acquisition without waiting for a Redis blocking dequeue. The idle acquisition wait is separate from retry backoff: retry delays use the Worker retry scheduler and do not block processing threads.

## 6. Phase boundaries

- **Phase 6:** retry policies and state, in-memory scheduled retry mechanism, failure handling, DLQ, and manual DLQ requeue.
- **Phase 8:** persistent delayed scheduling using Redis Sorted Sets (ZSET), durable next-run timestamps, and arbitrary long retry delays.

## 7. Verified Phase 6 coverage

Tests cover policy decisions, fixed delays, exponential progression and jitter bounds, Job retry state and validation, serialization/persistence round trips, Redis DLQ storage/listing/purge/requeue, retry exhaustion, manual requeue, missing DLQ jobs, failure diagnostics, and processing another ready job while a retry is scheduled. See tests under `src/test/java/io/github/pandeyayushk/jobstream/{job,retry,persistence}`. The test suite uses real Redis for Redis integration tests.
