# ADR-007: Executor System

**Status:** Accepted

## Context

Phase 5 adds job-type-specific execution to the Worker foundation. `JobQueue` transports `JobId` values and `JobRepository` remains authoritative for Job state. Worker must coordinate acquisition and lifecycle transitions while business execution remains independently implementable. Workers may process jobs concurrently, and execution failures must be recorded without terminating the Worker.

## Decision

1. Use `JobExecutor` as the business execution strategy. It receives a `Job` and returns an `ExecutionResult`; it does not depend on Worker, Redis, `JobQueue`, or `WorkerRegistry`.
2. Resolve executors through `ExecutorRegistry`, mapping job type to executor.
3. Use `DefaultExecutorRegistry`, backed by `ConcurrentHashMap`. Registration replaces an existing executor for the same job type. Null or blank job types and null executors are rejected.
4. Represent execution outcome with `ExecutionResult`: `success()`, `failure(String message)`, or `failure(Throwable cause)`. A failure may expose a message, a cause, or both.
5. Worker owns lifecycle transitions around execution: it persists `PROCESSING` before invoking an executor, then persists `COMPLETED` or `FAILED` according to the outcome.
6. Worker contains executor-thrown `Throwable`s and converts them into a failed job with a diagnostic; it continues processing other jobs.
7. A missing executor results in a `FAILED` job with a diagnostic, rather than terminating Worker.
8. Worker stores failure diagnostics in immutable Job metadata under `failure.reason`. `Job.withMetadata(...)` returns a new Job.
9. The registry stores executor instances directly. Implementations must be safe for concurrent invocation when Worker concurrency is greater than one.
10. Retry, timeout, dead-letter, and recovery of `PROCESSING` jobs remain outside Phase 5. Retry is addressed in a later phase.

## Alternatives Considered

- **Direct `WorkerJobHandler`:** This was the Phase 4 execution boundary and test seam. Phase 5 replaces it as the active production boundary so Worker can select business behavior by job type.
- **Switch/if-based dispatch in Worker:** This would couple Worker changes to each added job type. Registry lookup fits the existing separation between queue transport, lifecycle coordination, and business execution.
- **One executor per Worker:** This would bind a Worker instance to one job type and make mixed-type queue processing less direct.
- **Registry-based dispatch:** Chosen because it maps job types to executor strategies while keeping Worker generic and permitting multiple types to share a Worker.

## Consequences

- New job types can be supported by registering a `JobExecutor` without adding type branches to Worker.
- Worker persists execution outcomes and diagnostics through `JobRepository`; the queue continues to carry only `JobId`.
- Executor instances may be called concurrently, so their implementations must provide their own safe concurrency behavior.
- Failed jobs are available for later reliability handling, but Phase 5 does not retry, time out, dead-letter, or recover them.
