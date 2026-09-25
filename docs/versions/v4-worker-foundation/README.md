# Phase 4 — Worker Foundation

## 1. Objective

Phase 4 implements worker identity and lifecycle, Redis registration and heartbeat, bounded concurrent processing, graceful shutdown, and the `WorkerJobHandler` execution boundary.

## 2. Implemented Scope

- `WorkerId`, `WorkerStatus`, `WorkerInfo`, `WorkerConfig`, and `WorkerException`.
- `WorkerRegistry` and `RedisWorkerRegistry`.
- `WorkerJobHandler`, the Phase 4 execution boundary/test seam.
- `Worker`, which acquires queue IDs, loads jobs, claims queued jobs, delegates handling, and persists outcomes.
- Worker lifecycle, heartbeat scheduling, bounded processing concurrency, and graceful shutdown.

The worker data flow is:

```text
Worker -> JobQueue -> JobId -> JobRepository -> Job -> WorkerJobHandler
```

Phase 3 `JobQueue` operations return `JobId` only. `RedisJobQueue` does not load jobs and does not depend on `JobRepository`.

## 3. Phase Boundaries

| Phase | Responsibility |
|---|---|
| 4 — Worker Foundation | Worker identity and lifecycle, registry, heartbeat, shutdown, queue acquisition, `QUEUED -> PROCESSING`, `WorkerJobHandler`, and `PROCESSING -> COMPLETED` / `PROCESSING -> FAILED`. |
| 5 — Execution Engine | Production `JobExecutor` abstraction, executor implementations, executor dispatch/factory, and actual job-type execution. |
| 6 — Reliability & Retry | Retry policy and scheduling, retry counters, `FAILED -> RETRYING`, `RETRYING -> QUEUED`, and dead/retry-exhaustion behavior. |

Retry is intentionally outside Phase 4 so the worker foundation has no retry policy, scheduling, or retry-counter responsibility. No automatic retry occurs for a Phase 4 handler failure.

## 4. Worker Metadata and Configuration

`WorkerInfo` contains exactly `WorkerId workerId`, `WorkerStatus status`, and `Instant startedAt`.

`WorkerConfig` fields are `queueName`, `concurrency`, `heartbeatInterval`, `heartbeatTtl`, and `shutdownTimeout`. Defaults:

| Setting | Default |
|---|---:|
| Queue | `default` |
| Concurrency | 4 |
| Heartbeat interval | 10 seconds |
| Heartbeat TTL | 30 seconds |
| Shutdown timeout | 30 seconds |

Heartbeat TTL must be at least one second and heartbeat interval must be less than the TTL. Queue name must be non-blank and concurrency and durations must be positive.

## 5. Lifecycle and Concurrency

```text
STOPPED -> STARTING -> RUNNING -> STOPPING -> STOPPED
```

The worker registers `STARTING`, creates its executors, schedules heartbeats, registers `RUNNING`, and starts acquisition. Its concurrency model has one acquisition executor, a fixed processing executor sized by `concurrency`, a semaphore that bounds submitted/in-flight processing, and a separate scheduled heartbeat executor. There is one acquisition loop, not one polling loop per processing thread.

The processing sequence for each ID is:

1. Dequeue `JobId` from the configured queue.
2. Load the job from `JobRepository`.
3. Ignore an absent job or a job whose status is not `QUEUED`.
4. Persist the `PROCESSING` transition.
5. Invoke `WorkerJobHandler`.
6. Persist `COMPLETED` if the handler returns, or `FAILED` if it throws an `Exception`.

Handler failures and orphaned IDs do not terminate the worker. Retry is deferred to Phase 6.

## 6. Redis Registry

The registry uses these keys:

```text
jobstream:worker:<workerId>
jobstream:worker:<workerId>:heartbeat
```

The first is a Redis hash containing `id`, `status`, and `startedAt`; the second is an `alive` key refreshed with the configured Redis expiration/TTL. `getWorker()` retrieves metadata without checking heartbeat liveness. `listActiveWorkers()` scans metadata keys and includes a worker only when the matching heartbeat key exists.

Discovery uses `SCAN`, not `KEYS`. Heartbeat refresh uses `WATCH`, `MULTI`, and `EXEC` on the worker metadata key and heartbeat writes. If the metadata key changes or is removed before execution, `EXEC` aborts. This prevents a heartbeat racing with deregistration from recreating or refreshing a stale heartbeat after removal. This transaction does not provide rollback.

## 7. Graceful Shutdown

Shutdown changes `RUNNING -> STOPPING`, stops acquisition, waits for acquisition to terminate, then lets already-submitted/in-flight work finish up to the configured shutdown timeout. It then shuts down heartbeat, deregisters the worker, and sets status to `STOPPED`. If acquisition has dequeued an ID but shutdown begins before it submits that ID to the processing executor, it re-enqueues the ID on the configured queue. This handles the dequeue/shutdown race without silently discarding that ID. Work exceeding the wait timeout may be interrupted; no stronger completion guarantee is made.

## 8. Phase 4 Tests

The existing worker tests exercise lifecycle and registry interaction, status transitions, handler failure and continued operation, orphan IDs, heartbeat scheduling, processing concurrency, prevention of new acquisitions on stop, waiting for in-flight work, and start/stop edge cases. Redis registry tests exercise registration metadata, heartbeat TTL and liveness discovery, and deregistration.

Run the full suite with:

```powershell
mvn clean test
```
