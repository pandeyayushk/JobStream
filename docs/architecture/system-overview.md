# System Overview

JobStream is a Redis-backed job queue. Phases 1-7 are implemented. Phases 8-10 remain planned.

## 1. Implemented subsystems

1. **Job domain (`job`, `payload`)**: immutable Job state, identity, lifecycle status, payload, and retry diagnostics.
2. **Serialization and persistence**: Jackson JSON serialization and authoritative Redis Job records with status indexes. Phase 6 retry fields are part of the persisted Job representation.
3. **Queue (`queue`)**: FIFO Redis lists contain `JobId` values only. Submission atomically persists the queued Job, updates its status index, and enqueues the ID.
4. **Worker (`worker`)**: lifecycle, registry, heartbeat, bounded processing, queue acquisition, failure handling, retry scheduling, and DLQ integration.
5. **Execution (`executor`)**: type-based `JobExecutor` dispatch and explicit success/failure results.
6. **Reliability (`retry`)**: no-retry, fixed-delay, and jittered exponential policies; in-memory scheduled retries; Redis DLQ and manual requeue.
7. **CLI (`cli`)**: Picocli operator commands over the existing submission, repository, queue, worker-registry, and DLQ abstractions. A shared lazy context owns the Redis client lifecycle.

Scheduling and priority (Phase 8), metrics and HTTP API (Phase 9), and deployment/hardening (Phase 10) are not implemented.

## 2. Data and execution flow

The Job repository is authoritative (`JobId -> Job`); active and dead-letter Redis lists carry IDs only.

1. A new Job starts `PENDING`.
2. Queue submission persists `QUEUED` and adds its ID to the selected FIFO list using `MULTI`/`EXEC`.
3. Worker acquisition polls with `dequeueNonBlocking()`. When empty, the acquisition loop waits 50 ms and retries. This lets `Worker.stop()` end acquisition without waiting for a Redis blocking dequeue.
4. Worker loads the Job, verifies `QUEUED`, persists `PROCESSING`, then calls its registered executor.
5. Success persists `COMPLETED`. Failure persists `FAILED` and `failure.reason`, then Phase 6 evaluates retry policy.
6. For a permitted retry, Worker persists `RETRYING` and schedules a task on its separate in-memory retry scheduler. The processing thread does not wait for the delay. When due, the task verifies the Job remains `RETRYING`, persists `QUEUED`, and enqueues its ID.
7. If retry is unavailable or exhausted, the Job is persisted as `DEAD` and its ID is placed on `jobstream:queue:dead-letter`.
8. Manual DLQ requeue uses Redis `MULTI`/`EXEC` to remove the dead-list reference, reset retry count, persist `QUEUED`, update status indexes, and enqueue the ID to the requested queue.

## 3. Retry and phase boundary

`maxRetries` counts retries after the initial execution. `maxRetries=2` permits at most three executions. `Job` persists retry count, configured limit, last error, and failure timestamp; Jackson serialization retains these fields through Redis reload.

Phase 6 delayed retries use Worker-local scheduled executor state. Pending delays are not durable across process restart, and Phase 6 does not support arbitrary long delays. Phase 8 owns persistent delayed scheduling with Redis Sorted Sets (ZSET) and durable next-run timestamps.

## 4. Worker and Redis lifecycle

Worker owns separate acquisition, processing, heartbeat, and retry executors. Idle acquisition uses a 50 ms wait; retry delay is handled by the retry scheduler, not by sleeping a processing thread. Shutdown stops acquisition, drains/interrupts processing within configured bounds, stops schedulers, and deregisters the Worker.

Redis `MULTI` transactions in `RedisJobSubmissionStore`, `RedisWorkerRegistry`, and `RedisDeadLetterQueue` use try-with-resources, releasing transaction resources on completion or failure. Redis clients are injected and owned by their creator.

## 5. Package map and future work

Implemented packages include `job`, `payload`, `serialization`, `persistence`, `queue`, `worker`, `executor`, `retry`, and `cli`. The CLI is an application/interface layer over the established domain and infrastructure abstractions; it does not replace them. Future package responsibilities are scheduling and priority (Phase 8), metrics and API (Phase 9), and deployment/configuration hardening (Phase 10). See [project structure](project-structure.md), [job lifecycle](job-lifecycle.md), and the [Phase 7 CLI guide](../versions/v7-cli/README.md).
