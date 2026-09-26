# Worker Model Requirements

## 1. Purpose

Describes the worker lifecycle, registry, heartbeat, queue acquisition, and Phase 5 executor-based execution boundary as implemented.

## 2. Acquisition and Processing Flow

```text
JobQueue -> JobId -> Worker -> JobRepository -> QUEUED Job
Worker -> ExecutorRegistry -> JobExecutor -> ExecutionResult -> JobRepository
```

`JobQueue` stores and returns `JobId` values only. It does not load jobs or depend on `JobRepository`. The worker performs these steps:

1. Dequeue a `JobId` from the configured queue.
2. Load the corresponding `Job` from `JobRepository`.
3. Continue without execution if no job exists or its status is not `QUEUED`.
4. Transition the job to `PROCESSING` and persist it.
5. Look up a `JobExecutor` by job type through `ExecutorRegistry`.
6. Persist `COMPLETED` on `ExecutionResult.success()`. Persist `FAILED` if the executor is missing, returns a failure, or throws; store the diagnostic in Job metadata under `failure.reason`.

Worker owns lifecycle transitions around execution. Executor failures do not terminate Worker processing. `JobExecutor` implementations may be invoked concurrently and must be safe for concurrent use.

## 3. Worker Identity and Metadata

- `WorkerId` is UUID-based.
- `WorkerInfo` contains only `WorkerId workerId`, `WorkerStatus status`, and `Instant startedAt`.
- Worker states are `STARTING`, `RUNNING`, `STOPPING`, and `STOPPED`.

## 4. Configuration

`WorkerConfig` contains `queueName`, `concurrency`, `heartbeatInterval`, `heartbeatTtl`, and `shutdownTimeout`.

Defaults are queue `default`, concurrency `4`, heartbeat interval 10 seconds, heartbeat TTL 30 seconds, and shutdown timeout 30 seconds. Durations and concurrency must be positive; heartbeat TTL must be at least one second; heartbeat interval must be less than the TTL. Queue name must be non-null and non-blank.

## 5. Concurrency Model

Each worker has one dedicated acquisition executor, a fixed processing executor sized to configured concurrency, a semaphore limiting submitted/in-flight processing to that concurrency, and a separate scheduled heartbeat executor. Acquisition is a single loop; processing runs concurrently. The semaphore is acquired before dequeue, so the acquisition loop does not take more jobs while all processing permits are occupied.

## 6. Redis Registry and Heartbeat

`RedisWorkerRegistry` stores metadata in the hash `jobstream:worker:<workerId>` and liveness in `jobstream:worker:<workerId>:heartbeat`. The heartbeat key is refreshed with Redis expiration using the configured TTL. `getWorker()` reads metadata only. `listActiveWorkers()` uses `SCAN` to find metadata keys and considers a worker active when its heartbeat key exists.

Heartbeat uses `WATCH`, `MULTI`, and `EXEC`: it watches the metadata key before refreshing the heartbeat. If the metadata changes or disappears before `EXEC`, the transaction is aborted. This prevents a heartbeat racing with deregistration from recreating or refreshing a stale heartbeat after the worker was removed. Redis transactions do not provide rollback.

`SCAN` is used for active-worker discovery rather than `KEYS`, avoiding a single blocking full-keyspace lookup.

## 7. Lifecycle and Graceful Shutdown

Normal lifecycle:

```text
STOPPED -> STARTING -> RUNNING -> STOPPING -> STOPPED
```

Start registers `STARTING` metadata, creates executors, starts heartbeat scheduling, registers `RUNNING` metadata, then submits acquisition. Shutdown:

1. Changes status from `RUNNING` to `STOPPING`.
2. Stops job acquisition and waits for its executor to terminate.
3. Allows already-submitted jobs to finish, bounded by the configured shutdown timeout.
4. Shuts down heartbeat.
5. Deregisters the worker.
6. Changes status to `STOPPED`.

If a `JobId` was dequeued but shutdown starts before submission to the processing executor, acquisition enqueues the ID back onto the configured queue. This protects that job from being silently lost at this race boundary. Processing that exceeds the timeout may be interrupted; shutdown does not promise completion beyond the configured wait.

## 8. Failure and Phase Ownership

- An executor failure or thrown `Throwable` marks that job `FAILED`; the worker remains available for later jobs.
- An orphaned `JobId` with no matching job does not terminate the worker.
- Phase 5 handles `PROCESSING -> COMPLETED` and `PROCESSING -> FAILED`; it does not retry failed jobs.
- Phase 5 failure diagnostics use immutable Job metadata key `failure.reason`.
- Phase 6 owns retry policy and scheduling, retry counters, `FAILED -> RETRYING`, `RETRYING -> QUEUED`, and dead/retry-exhaustion behavior.

## 9. Verified Coverage

Worker tests cover startup and registration state, deregistration, queued-job processing, the executor observing `PROCESSING`, executor failure and continued worker operation, orphan IDs, heartbeat scheduling, configured concurrency, stopping further acquisition, waiting for in-flight work, rejecting repeated start, and safe stop from `STOPPED`. Registry tests cover metadata, heartbeat TTL/liveness, listing, and deregistration.
