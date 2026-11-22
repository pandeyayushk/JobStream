# ADR-008: Retry, Failure Handling, and Dead-Letter Queue

- **Status:** Accepted; implemented in Phase 6
- **Date:** 2026-09-29

## Context

Phase 5 workers execute jobs and persist `COMPLETED` or `FAILED`. Transient failures need bounded retries without tying up processing capacity, and exhausted or non-retryable failures need inspection and controlled recovery.

## Problem

The system needs explicit retry state and policy, failure diagnostics that survive Redis reload, delayed retry execution that does not block a processing thread, and a quarantine path with manual requeue.

## Decision

### Retry state design

`Job` owns `retryCount`, `maxRetries`, `lastErrorReason`, and `lastFailedAt`. `maxRetries` counts retries after the initial execution; a value of two permits at most three executions. `withRetryAttempt(reason)` transitions `FAILED -> RETRYING`, increments the count, and records the failure reason and timestamp. `resetRetryForRequeue()` transitions only `DEAD -> QUEUED` and resets retry count while retaining diagnostics. `maxRetries` must be positive.

### RetryPolicy abstraction

`RetryPolicy` exposes `shouldRetry(Job, Throwable)` and `computeBackoff(Job)`. Implementations are `NoRetryPolicy`, `FixedDelayRetryPolicy`, and `ExponentialBackoffRetryPolicy`. Fixed and exponential policies permit retry while `retryCount < maxRetries`; no-retry always declines.

### Backoff strategy

Fixed delay returns its configured non-negative duration. Exponential delay computes `min(baseBackoffMillis * 2^retryCount, maxBackoffMillis)`, then applies a random multiplier in `[0.5, 1.5)` and rounds to milliseconds. Because jitter follows the cap, the final duration can exceed `maxBackoff`; the maximum bounds only the pre-jitter value.

### Worker non-blocking retry scheduling

Worker records failure, evaluates policy, persists `RETRYING`, and schedules requeue work on its separate scheduled executor. When due, the task reloads the Job, verifies it is still `RETRYING`, saves `QUEUED`, and enqueues its ID. A processing thread does not wait for the retry delay. This is an in-memory schedule, not durable delayed scheduling.

### DLQ design and Redis storage model

`DeadLetterQueue` defines dead-letter movement, paginated listing, manual requeue, purge, and size operations. `RedisDeadLetterQueue` stores Job IDs in Redis list `jobstream:queue:dead-letter`; the full `DEAD` record remains at `jobstream:job:<id>` and its status index is updated. Purge deletes the list key, not the Job records. Missing records encountered by listing are skipped.

Moving a failed Job to the DLQ persists `DEAD`, updates status indexes, and pushes the ID in one Lua script. `RedisDeadLetterQueue.requeue` returns empty for missing or non-DEAD jobs. For a DEAD job, a Lua script checks the current revision, removes the DLQ reference, persists QUEUED with the next revision, updates indexes, resets retry count, and inserts into the target priority queue atomically.

### Redis transaction/resource lifecycle

`RedisWorkerRegistry` uses `MULTI`/`EXEC` with try-with-resources so transaction resources are closed after execution or failure. Job submission and DLQ writes use Lua scripts; Redis clients remain injected resources owned by their creators.

### Phase 6 and Phase 8 boundary

Phase 6 owns retry policy, retry state, in-memory scheduled retry execution, failure handling, DLQ, and manual requeue. Phase 8 implements numeric priority queues, persistent delayed scheduling using Redis Sorted Sets (ZSET), revision safety, and atomic poller release. Worker retry backoff has not been connected to the durable scheduler; pending Worker-local delay timers still do not survive restart.

## Consequences

- Retry attempt history and last failure diagnostics travel with the persisted Job through Jackson serialization.
- Processing capacity remains available while retry delay elapses.
- Exhausted/non-retryable failures remain inspectable in Redis and can be manually requeued.
- A process restart can lose retries waiting in the in-memory scheduler; the durable scheduler exists but Worker retry integration remains deferred.
- The DLQ move/requeue scripts update Job state, status indexes, and queue/list references atomically.

## Known limitations and future work

- Pending retries held in the Worker-local scheduler are not durable. Durable scheduling is available through Phase 8 infrastructure but is not wired to Worker backoff.
- Revision-based Job writes reject stale snapshots, but do not provide Worker leases or an atomic `QUEUED -> PROCESSING` claim protocol.
- Phase 8 Worker retry wiring and priority/delay CLI options remain deferred; HTTP API work remains planned for Phase 9.

## Alternatives considered

- Sleeping a processing thread during backoff was rejected because it reduces available processing capacity.
- Persistent Redis sorted-set scheduling was reserved for Phase 8 rather than added to the Phase 6 in-memory retry implementation.
