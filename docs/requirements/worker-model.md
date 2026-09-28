# Worker Model Requirements

Describes the implemented Worker lifecycle, acquisition, execution, and Phase 6 retry integration.

## 1. Acquisition and processing flow

```text
JobQueue -> JobId -> Worker -> JobRepository -> QUEUED Job
Worker -> ExecutorRegistry -> JobExecutor -> ExecutionResult -> JobRepository
```

The queue stores Job IDs only. Worker acquires an ID, loads the authoritative Job, ignores missing or non-`QUEUED` records, persists `PROCESSING`, executes through the registered `JobExecutor`, and persists `COMPLETED` or `FAILED`. Failure reason is recorded in metadata as `failure.reason`.

Acquisition uses `dequeueNonBlocking()`. If the queue is empty, the acquisition loop waits 50 ms and checks again. This short idle wait lets `Worker.stop()` promptly stop acquisition without leaving a thread in a Redis blocking dequeue. It does not implement retry backoff.

## 2. Retry and failure handling (Phase 6)

Worker evaluates `RetryPolicy` after persisting a failed execution. A permitted retry updates the Job to `RETRYING`, then schedules a requeue task on the separate Worker retry scheduler. The processing thread returns to its executor pool immediately and can process another ready Job. At the due time, the scheduler reloads the Job, checks that it is still `RETRYING`, changes it to `QUEUED`, and enqueues its ID. This schedule is in memory only and is not durable across process restarts.

When retry is declined or exhausted, Worker sends the failed Job to `DeadLetterQueue`, where it is persisted as `DEAD`. Manual DLQ requeue returns it to `QUEUED` and resets retry count. Durable delayed scheduling via Redis ZSET and next-run timestamps belongs to Phase 8.

`maxRetries` counts retries after the first execution. A maximum of two retries allows three executions total.

## 3. Identity, configuration, and concurrency

`WorkerId` is UUID-based. `WorkerInfo` contains `WorkerId`, `WorkerStatus`, and `startedAt`. Worker statuses are `STARTING`, `RUNNING`, `STOPPING`, and `STOPPED`.

`WorkerConfig` contains queue name, concurrency, heartbeat interval and TTL, and shutdown timeout. Defaults are `default`, 4, 10 seconds, 30 seconds, and 30 seconds respectively. Each Worker has a single acquisition executor, a fixed processing executor, a semaphore limiting in-flight jobs to configured concurrency, a heartbeat scheduler, and a separate retry scheduler.

## 4. Registry and Redis resource lifecycle

`RedisWorkerRegistry` stores metadata in `jobstream:worker:<workerId>` and heartbeat liveness in the corresponding `:heartbeat` key. Heartbeats use `WATCH` with `MULTI`/`EXEC` to avoid refreshing a removed worker. Registry transactions are scoped with try-with-resources so transaction connections are released.

Redis transactions in `RedisWorkerRegistry`, `RedisJobSubmissionStore`, and `RedisDeadLetterQueue` use try-with-resources. Injected `RedisClient` instances are owned and closed by their creator, not by these collaborators.

## 5. Lifecycle and shutdown

```text
STOPPED -> STARTING -> RUNNING -> STOPPING -> STOPPED
```

Shutdown marks `STOPPING`, interrupts/stops acquisition and waits within the configured timeout, allows submitted processing to finish within the timeout, stops retry and heartbeat executors, deregisters the Worker, then marks `STOPPED`. A dequeued ID that cannot be submitted because shutdown raced with acquisition is returned to the queue. The acquisition loop is non-blocking with respect to Redis, supporting deterministic shutdown.

## 6. Verified Phase 6 coverage

Worker retry integration tests exercise eventual completion after retry, processing another ready job while retry is delayed, exhausted retries and DLQ routing, manual DLQ requeue, failure diagnostics, invalid retry limits, and missing DLQ jobs. Worker lifecycle and Redis registry tests cover start/stop, processing concurrency, heartbeat, and deregistration.
