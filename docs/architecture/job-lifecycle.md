# Job Lifecycle

This document describes the lifecycle states, implemented transitions, and subsystem ownership in JobStream.

## 1. Lifecycle states

| State | Meaning |
|---|---|
| `PENDING` | A new Job has not yet been submitted to a queue. |
| `QUEUED` | The Job is persisted and its `JobId` is waiting on a queue. |
| `PROCESSING` | A Worker has loaded the Job and execution is underway. |
| `COMPLETED` | The executor succeeded. |
| `FAILED` | Execution failed; Worker records the reason and evaluates retry policy. |
| `RETRYING` | A retry was permitted and its delay is pending in the Worker's in-memory retry scheduler. |
| `DEAD` | Retry is unavailable or exhausted; the Job is in the dead-letter queue. |

## 2. Transition ownership

| Transition | Owner | Phase | Implemented behavior |
|---|---|---|---|
| `PENDING -> QUEUED` | Queue submission | 3 | The queued Job, status index, and queue ID are submitted atomically. |
| `QUEUED -> PROCESSING` | Worker acquisition | 4 | Worker acquires the ID, loads the authoritative Job, and saves `PROCESSING`. |
| `PROCESSING -> COMPLETED` | JobExecutor success through Worker | 5 | Worker persists the successful result. |
| `PROCESSING -> FAILED` | Execution failure through Worker | 5 | Missing executor, failure result, or thrown failure is recorded in `failure.reason`. |
| `FAILED -> RETRYING` | RetryPolicy and Worker | 6 | When retry is permitted and remains available, `withRetryAttempt` increments the retry count and records the latest failure. |
| `RETRYING -> QUEUED` | Worker retry scheduler | 6 | After the scheduled delay, Worker saves `QUEUED` and enqueues the ID. Scheduling is in memory, not durable. |
| `FAILED -> DEAD` | Worker and DeadLetterQueue | 6 | A no-retry decision or exhausted retry allowance persists `DEAD` and adds the ID to the DLQ. |
| `DEAD -> QUEUED` | DeadLetterQueue manual requeue | 6 | An atomic Redis transaction removes the DLQ entry, resets retry count, saves `QUEUED`, and enqueues the ID. |

## 3. Retry semantics

`maxRetries` is the number of retries permitted after the initial execution attempt. Thus `maxRetries=2` permits three executions: initial attempt, retry 1, and retry 2. `Job` rejects `maxRetries <= 0` and retry counts outside `0..maxRetries`.

Failure handling follows one of these paths:

```text
PROCESSING -> FAILED -> RETRYING -> QUEUED -> PROCESSING
                         (scheduled retry)

PROCESSING -> FAILED -> DEAD -> DLQ
```

`Job.withRetryAttempt(reason)` is valid from `FAILED`, increments `retryCount`, and updates `lastErrorReason` and `lastFailedAt`. `resetRetryForRequeue()` is valid from `DEAD`, returns the Job to `QUEUED`, and resets `retryCount` to zero; the most recent failure diagnostics remain available.

Phase 6 retry delays are held by the Worker's scheduled executor and are not durable across process restart. Phase 8 owns persistent delayed scheduling, Redis Sorted Sets (ZSET), durable next-run timestamps, and arbitrary long retry delays.

## 4. Lifecycle invariants and limits

- The persisted Job is authoritative; Redis active and dead-letter queues store `JobId` references.
- Only `QUEUED` Jobs are eligible for Worker execution.
- `COMPLETED` is terminal. `DEAD` remains quarantined until explicitly requeued.
- Job status transitions are validated by `JobStatus`; `JobRepository.save` itself is not an atomic compare-and-set claim operation.

## 5. Worker acquisition and shutdown

The current acquisition loop calls `dequeueNonBlocking()` and, when empty, waits briefly (50 ms) before trying again. This avoids leaving the acquisition thread blocked in Redis during `Worker.stop()` and makes shutdown deterministic. This short wait belongs only to idle queue acquisition; retry backoff runs on the separate Worker retry scheduler and never sleeps a processing thread.
