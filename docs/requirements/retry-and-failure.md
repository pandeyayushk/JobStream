# Retry and Failure Handling Requirements

## 1. Purpose

Defines how JobStream handles transient execution failures, calculates backoff intervals, preserves worker availability during retry delays, and quarantines permanently failed jobs in a Dead-Letter Queue (DLQ).

---

## 2. Core Architectural Distinctions

To avoid naive implementations that degrade system performance, the architecture strictly distinguishes between the following concepts:

1. **Retry Decision:**
   - Determining *whether* a failed job should be retried based on attempt count (`retryCount < maxRetries`), error category (transient network glitch vs fatal business violation), and job type policy.
2. **Retry Scheduling (Non-Blocking):**
   - In production, retrying a job after a backoff delay **MUST NOT** block or sleep worker threads.
   - Worker threads must immediately become available to process other ready jobs from the queue.
3. **Delayed Execution:**
   - Waiting for a backoff delay to elapse requires a time-delayed scheduling mechanism (e.g. Redis Sorted Sets where score = ready timestamp, introduced in Phase 8).
   - In Phase 6, immediate re-enqueueing or short in-memory test delays may be used as an educational intermediate step, but its limitations must be explicitly documented.
4. **Retry Exhaustion:**
   - When attempts reach `maxRetries`, the job transitions from `RETRYING` to `DEAD`.
5. **Dead-Letter Handling (DLQ):**
   - Quarantining dead jobs in a dedicated structure (`jobstream:queue:dead-letter`) for operator inspection, diagnosis, and optional manual requeueing.

---

## 3. Functional Requirements

### 3.1 Job Entity Retry Metadata
- The `Job` domain entity tracks:
  - `int retryCount`: Current attempt number (starts at 0, increments on each failure).
  - `int maxRetries`: Maximum allowed retry attempts (default: 3).
  - `String lastErrorReason`: Diagnostic message from the most recent failure.
  - `Instant lastFailedAt`: Timestamp of the most recent failure.

### 3.2 Retry Policy Abstraction (`RetryPolicy`)
- Interface defining retry rules:
  - `boolean shouldRetry(Job job, Throwable cause)`: Decides if another attempt is permitted.
  - `Duration computeBackoff(Job job)`: Computes delay before the next attempt.
- Built-in strategies:
  - `FixedDelayRetryPolicy`: Constant delay between attempts.
  - `ExponentialBackoffRetryPolicy`: Exponentially increasing delay with full randomized jitter to prevent thundering herds:
    $$\text{delay} = \min(\text{maxBackoff}, \text{baseBackoff} \times 2^{\text{attempt}}) \times \text{random}(0.5, 1.5)$$
  - `NoRetryPolicy`: Immediately fails without retrying.

### 3.3 Dead-Letter Queue Abstraction (`DeadLetterQueue`)
- Technology-neutral interface for dead job quarantine:
  - `void moveToDeadLetter(Job job, String reason)`: Transitions job status to `DEAD` and moves `JobId` into DLQ list.
  - `List<Job> listDeadJobs(int offset, int limit)`: Paginated inspection of dead jobs.
  - `Optional<Job> requeue(JobId jobId, String targetQueue)`: Re-enqueues a dead job into an active queue, resetting its status to `QUEUED`.
  - `void purge()`: Removes all dead jobs after administrative review.
  - `long size()`: Returns total count of dead jobs.
- Storage key: `jobstream:queue:dead-letter`.

---

## 4. Worker Availability & Concurrency Rules

- **Anti-Pattern Warning:** Putting a worker thread to sleep (`Thread.sleep(delay)`) while holding a job starves worker concurrency and halts processing of healthy jobs.
- **Production Contract:**
  - When a job fails and retries remain, the worker updates the job state, increments `retryCount`, marks status `RETRYING`, and passes the job to the scheduling mechanism.
  - The worker immediately proceeds to the next job in the queue.
  - In Phase 6, zero-delay or immediate re-enqueueing represents the baseline mechanism; Phase 8 integrates Redis Sorted Set scheduling to support true non-blocking arbitrary delays.

---

## 5. Non-Functional Requirements

- Zero data loss: Exhausted jobs are never dropped silently; they must reside safely in the DLQ.
- Re-queue operations from DLQ must be idempotent and atomic.

---

## 6. Dependencies

- Domain Model (Phase 1: `Job`, `JobStatus`, `JobId`)
- Persistence Layer (Phase 2: `JobRepository`)
- Queue System (Phase 3: `JobQueue`)
- Execution Engine (Phase 5: `ExecutionResult`)

---

## 7. Phase Ownership

- **Phase 6 (Reliability & Retry):** Establishes `RetryPolicy`, `DeadLetterQueue`, `RedisDeadLetterQueue`, and worker failure integration.
- **Phase 8 (Scheduling & Priority):** Provides the non-blocking Redis Sorted Set delayed-job mechanism for long backoffs.
