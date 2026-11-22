# Job Lifecycle

This document describes the implemented Job states and transition ownership. Phase 8 adds durable scheduled waiting and release but does not add lifecycle states.

## Lifecycle states

| State | Meaning |
|---|---|
| `PENDING` | A new Job is awaiting submission or is waiting in the durable scheduler. |
| `QUEUED` | The Job is persisted and its JobId is available in a named priority queue. |
| `PROCESSING` | A Worker has loaded the Job and execution is underway. |
| `COMPLETED` | The executor succeeded. |
| `FAILED` | Execution failed; Worker records the reason and evaluates retry policy. |
| `RETRYING` | A retry was permitted and is waiting either in the Worker's in-memory retry scheduler or, when explicitly scheduled, in durable scheduler indexes. Worker backoff is not integrated with the durable scheduler. |
| `DEAD` | Retry is unavailable or exhausted; the Job is in the dead-letter queue. |

## Transition ownership

| Transition | Owner | Phase | Implemented behavior |
|---|---|---:|---|
| `PENDING -> QUEUED` | Queue submission | 3 | `QueueCoordinator` and `RedisJobSubmissionStore` persist the queued snapshot, update status indexes, and insert the JobId into the priority queue atomically. |
| `PENDING -> QUEUED` | `RedisSchedulerPoller` | 8 | When a scheduled pending Job is due, one Lua release updates the Job/status indexes, removes schedule indexes, and inserts its JobId into the target priority queue atomically. |
| `QUEUED -> PROCESSING` | Worker acquisition | 4 | Worker acquires the ID, loads the authoritative Job, and saves `PROCESSING`. |
| `PROCESSING -> COMPLETED` | Executor success through Worker | 5 | Worker persists the successful result. |
| `PROCESSING -> FAILED` | Execution failure through Worker | 5 | Missing executor, failure result, or thrown failure is recorded in `failure.reason`. |
| `FAILED -> RETRYING` | Retry policy and Worker | 6 | When retry is permitted and available, `withRetryAttempt` increments retry count and records the latest failure. |
| `RETRYING -> QUEUED` | Worker retry scheduler | 6 | After the in-memory delay, Worker reloads and verifies the Job, saves `QUEUED`, and enqueues its ID. This timer is not durable. |
| `RETRYING -> QUEUED` | `RedisSchedulerPoller` | 8 | A due Job explicitly placed in the durable scheduler is released atomically to its configured priority queue. Worker retry backoff does not currently use this path. |
| `FAILED -> DEAD` | Worker and `DeadLetterQueue` | 6 | A no-retry decision or exhausted retry allowance persists `DEAD` and adds the ID to the DLQ. |
| `DEAD -> QUEUED` | DLQ manual requeue | 6 | `RedisDeadLetterQueue` atomically removes the DLQ reference, resets retry count, persists the queued snapshot, updates indexes, and inserts into the target priority queue. |

Scheduling a Job is not a status transition. `RedisJobScheduler` accepts only `PENDING` or `RETRYING`, persists `scheduledAt` with a new revision, and adds JobId/destination indexes while leaving the status unchanged until the poller releases it.

## Retry semantics

`maxRetries` is the number of retries permitted after the initial execution attempt. Thus `maxRetries=2` permits three executions. `Job.withRetryAttempt(reason)` accepts only `FAILED`, increments retry count, and updates `lastErrorReason` and `lastFailedAt`. `resetRetryForRequeue()` accepts only `DEAD`, returns `QUEUED`, and resets retry count while retaining the most recent failure diagnostics.

The Phase 6 Worker retry scheduler is in-memory and is not durable across process restart. Phase 8 supplies durable scheduling infrastructure, but the Worker retry path has not been connected to it.

## Lifecycle invariants and limits

- The persisted Job is authoritative; active and dead-letter queues and scheduler indexes store JobId references.
- Only `QUEUED` Jobs are eligible for Worker execution.
- `COMPLETED` is terminal. `DEAD` remains quarantined until explicitly requeued.
- Job transition validation is defined by `JobStatus`. Redis Job record writes use revision checks; they do not constitute a Worker claim/lease protocol.
- A malformed or incorrectly routed scheduled record remains indexed for recovery. Missing Jobs and Jobs that are no longer schedulable are cleaned from scheduler indexes.

## Worker acquisition and shutdown

The acquisition loop calls `dequeueNonBlocking()` and, when empty, waits briefly (50 ms) before trying again. This avoids leaving the acquisition thread blocked in Redis during `Worker.stop()` and makes shutdown deterministic. Retry backoff runs on the separate Worker retry scheduler and does not sleep a processing thread.
