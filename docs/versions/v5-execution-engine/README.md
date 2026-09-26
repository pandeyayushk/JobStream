# Phase 5 — Execution Engine

## 1. Objective

Phase 5 adds type-based job execution while keeping queue transport, job state, worker coordination, and business logic in separate responsibilities. This phase is implemented.

## 2. Architecture

```text
JobQueue (JobId) → Worker → ExecutorRegistry → JobExecutor
                                      JobExecutor → ExecutionResult
Worker → JobRepository (authoritative Job state)
```

- `JobQueue` transports `JobId` values only.
- `JobRepository` owns authoritative Job state.
- Worker loads and verifies jobs, owns lifecycle transitions, coordinates execution, and persists outcomes.
- `ExecutorRegistry` maps job type to `JobExecutor`.
- `JobExecutor` contains job-specific logic and has no dependency on Worker, Redis, `JobQueue`, or `WorkerRegistry`.
- `ExecutionResult` communicates execution outcome.

## 3. Implemented Components

- `JobExecutor` accepts a `Job` and returns `ExecutionResult`.
- `ExecutionResult` represents executor outcome and supports `success()`, `failure(String message)`, and `failure(Throwable cause)`. Failure information may include a message, a cause, or both.
- `ExecutorRegistry` supports `register(jobType, executor)`, `get(jobType)`, and `hasExecutor(jobType)`.
- `DefaultExecutorRegistry` uses `ConcurrentHashMap`. Registration replaces an existing mapping; null or blank job types and null executors are rejected.
- Registry instances store executor instances directly. Executor implementations must be safe for concurrent invocation when Worker concurrency is greater than one.
- `Job.withMetadata(...)` creates a new Job containing the metadata update. Worker records failure diagnostics under `failure.reason`.

## 4. Worker Execution Flow

1. Worker dequeues a `JobId` from `JobQueue`.
2. Worker loads the Job from `JobRepository`.
3. Worker verifies that the Job is `QUEUED`; other or missing jobs are not executed.
4. Worker transitions the Job to `PROCESSING` and persists it.
5. Worker looks up a `JobExecutor` using the Job type.
6. If no executor exists, Worker marks the Job `FAILED` and persists a diagnostic in `failure.reason`.
7. If the executor returns `ExecutionResult.success()`, Worker marks the Job `COMPLETED`.
8. If the executor returns a failure, Worker marks the Job `FAILED` and persists its diagnostic in `failure.reason`.
9. If the executor throws a `Throwable`, Worker catches it, marks the Job `FAILED`, and persists a diagnostic in `failure.reason`.
10. Executor failure does not terminate Worker processing; Worker can continue with another job.

`PROCESSING → COMPLETED` and `PROCESSING → FAILED` are Phase 5 execution outcomes. Failure diagnostics are metadata; Phase 5 does not add retry counters or separate failure fields.

## 5. Scope Boundary

Phase 5 does not implement retry logic, timeout logic, dead-letter behavior, or crash recovery/recovery of `PROCESSING` jobs. Retry is a later phase concern.

## 6. Verification Coverage

Phase 5 integration coverage includes:

- Successful execution results in `COMPLETED`.
- Returned failure and thrown exception result in `FAILED`.
- Missing executor results in `FAILED`.
- Worker continues after a failed job.
- Different job types dispatch to different executors.
- Failure reason is persisted and can be read from `JobRepository`/Redis.

Test fixtures include `NoOpExecutor` and `FailingExecutor`.

## 7. Related Documentation

- [Executor System Requirements](../../requirements/executor-system.md)
- [System Overview](../../architecture/system-overview.md)
- [Job Lifecycle](../../architecture/job-lifecycle.md)
- [Project Structure](../../architecture/project-structure.md)
- [ADR-007: Executor System](../../decisions/ADR-007-executor-system.md)
