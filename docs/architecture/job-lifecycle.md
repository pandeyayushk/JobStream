# Job Lifecycle

This document is the authoritative architectural reference for the complete Job lifecycle in JobStream. It defines all states, legal and illegal transitions, invariants, and which subsystem and project phase owns the implementation of each transition.

---

## 1. Complete Domain Lifecycle States

The core domain model establishes a closed set of 7 lifecycle states in the `JobStatus` enum (introduced in Phase 1 as the system-wide domain vocabulary):

| State | Definition | Nature |
|---|---|---|
| **`PENDING`** | Job entity has been instantiated, but has not yet been placed into a queue. | Transient (Initial) |
| **`QUEUED`** | Job state is persisted and its `JobId` resides in an active queue awaiting acquisition. | Passive / Waiting |
| **`PROCESSING`** | `JobId` has been dequeued by an active worker; execution logic is currently running. | Active |
| **`COMPLETED`** | The job's execution logic finished successfully with all side effects confirmed. | **Terminal** |
| **`FAILED`** | The execution logic threw an unhandled error or returned a failure result. | Transient / Evaluative |
| **`RETRYING`** | The retry policy permitted another attempt; the job is awaiting backoff delay before re-enqueueing. | Waiting |
| **`DEAD`** | All retry attempts have been exhausted; the job is quarantined in the Dead-Letter Queue (DLQ). | **Terminal (Quarantine)** |

---

## 2. State Transition Matrix & Subsystem Ownership

A crucial architectural distinction exists between **defining the lifecycle contract** (established in Phase 1) and **implementing the subsystem that triggers a transition** (introduced incrementally across phases).

```
                 ┌────────────────────────────────────────────────────────┐
                 │                                                        │
                 ▼                                                        │
[ PENDING ] ─────────> [ QUEUED ] ─────────> [ PROCESSING ] ─────────> [ COMPLETED ]*
                           ▲                     │
                           │                     │
                           │                     ▼
                     [ RETRYING ] <───────── [ FAILED ]
                                                 │
                                                 │
                                                 ▼
                                             [ DEAD ]*
                                                 │
                                                 │ (Manual operator requeue)
                                                 └───────────> [ QUEUED ]

* Terminal state
```

### Transition Specifications

| Transition | Triggering Subsystem | Implementing Phase | Description & Invariants |
|---|---|---|---|
| **`PENDING → QUEUED`** | **Producer / Queue System** | **Phase 3** | Occurs when a client submits a job. Job is saved to `JobRepository` and its `JobId` is atomically pushed to `JobQueue`. |
| **`QUEUED → PROCESSING`** | **Worker Acquisition** | **Phase 3 / Phase 4** | Occurs when an active `Worker` pops a `JobId` from `JobQueue` and claims the job in `JobRepository`. |
| **`PROCESSING → COMPLETED`** | **Execution Engine** | **Phase 5** | Occurs when `JobExecutor.execute(job)` returns `ExecutionResult.success()`. Completion timestamp is recorded. |
| **`PROCESSING → FAILED`** | **Execution Engine** | **Phase 5** | Occurs when `JobExecutor.execute(job)` fails or throws a `Throwable`. Error details and timestamp are recorded. |
| **`FAILED → RETRYING`** | **Reliability Subsystem** | **Phase 6** | Occurs when `RetryPolicy.shouldRetry(...)` evaluates to `true`. Retry attempt counter is incremented. |
| **`RETRYING → QUEUED`** | **Reliability / Scheduler** | **Phase 6 (immediate/timer) / Phase 8 (ZSET)** | Occurs after backoff delay elapses. `JobId` is placed back onto the active queue for worker acquisition. |
| **`FAILED → DEAD`** | **Reliability Subsystem** | **Phase 6** | Occurs when retry attempts reach `maxRetries`. Job is moved to the Dead-Letter Queue (`jobstream:queue:dead-letter`). |
| **`DEAD → QUEUED`** | **Operator / CLI / API** | **Phase 6 (DLQ API) / Phase 7 (CLI) / Phase 9 (API)** | Exceptional administrative intervention. An operator inspects the dead job and manually triggers a requeue. |

---

## 3. Illegal Transitions

To protect system integrity, the following transitions are strictly illegal and must be rejected with an `IllegalStateException`:

- **`PENDING → PROCESSING`**: A job cannot be processed without first being enqueued and claimed via the queue.
- **`PENDING → COMPLETED` / `FAILED`**: Phase 1 establishes domain types, but does NOT allow arbitrary jumping from creation directly to completed/failed without execution.
- **`COMPLETED → *`**: `COMPLETED` is strictly terminal. A completed job can never be re-executed, re-queued, or marked failed.
- **`PROCESSING → QUEUED`**: A processing job cannot jump directly back to the queue without going through failure/retry or explicit abandonment recovery.
- **`DEAD → PROCESSING`**: A dead job cannot be claimed directly by a worker; it must be explicitly re-queued to `QUEUED` by an operator first.
- **`RETRYING → COMPLETED`**: A job awaiting retry cannot magically succeed without being re-executed.

---

## 4. System Invariants

1. **Single State at Any Point:** A job can occupy exactly one state at any given millisecond.
2. **Strict Unidirectionality:** Lifecycle moves forward along defined pathways. There are no reverse transitions except the explicit `RETRYING → QUEUED` and administrative `DEAD → QUEUED`.
3. **Only `QUEUED` Jobs are Claimable:** A worker thread may only claim and transition a job from `QUEUED` to `PROCESSING`.
4. **State Transition Atomicity:** In a distributed multi-worker environment, status updates in `JobRepository` must be atomic to avoid race conditions (e.g. two workers claiming the same job).
5. **Phase 1 Responsibility Boundary:** Phase 1 defines the `JobStatus` enum and validation methods (e.g. `isValidTransition(from, to)`). Phase 1 **does not** execute jobs, enqueue jobs, or claim jobs.
