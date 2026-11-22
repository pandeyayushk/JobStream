# System Overview

JobStream is a Java 21 Redis-backed job queue. Phase 8 priority and durable scheduling infrastructure is implemented. Worker retry integration, CLI scheduling flags, and automatic poller bootstrap remain deferred. Metrics/API (Phase 9) and deployment/hardening (Phase 10) are planned.

## Implemented subsystems

1. **Domain (`job`, `payload`)** — immutable Jobs, lifecycle states, priority, scheduled timestamp, retry data, and payload.
2. **Serialization and persistence** — Jackson serialization, authoritative Job records, status indexes, and revision-based optimistic concurrency.
3. **Queue and submission (`queue`)** — named Redis priority Sorted Sets, FIFO tie-breaking for equal priorities, legacy active List migration, atomic submission, and JobId-only queue operations.
4. **Scheduling (`schedule`)** — durable JobId schedule indexes, destination routing, and bounded atomic release to priority queues.
5. **Worker and execution (`worker`, `executor`)** — queue acquisition, registered execution, lifecycle, heartbeat, and bounded processing.
6. **Retry and failure (`retry`)** — retry policy, existing in-memory Worker retry delays, Redis DLQ, and manual requeue.
7. **CLI (`cli`)** — operator commands for job submission and inspection, queues, workers, and DLQ. It has no delay or priority submission flags.

## Architecture flows

The persisted Job record remains authoritative. Queue and scheduler structures contain references and routing data; they do not contain competing serialized Job copies.

### Immediate submission and execution

```text
Job (PENDING)
  -> QueueCoordinator.submit
  -> revision-aware RedisJobSubmissionStore Lua script
       -> authoritative Job record (QUEUED, incremented revision)
       -> status indexes
       -> named priority queue ZSET + membership Hash
  -> Worker acquires JobId through JobQueue
  -> JobRepository loads authoritative Job
  -> Worker persists PROCESSING and executes
```

The priority queue score is negative numeric priority. A fixed-width Redis Stream ID in each member provides FIFO ordering among equal priorities. The membership Hash prevents duplicate JobId entries. The submission operation returns the persisted queued Job snapshot, including its incremented revision.

### Durable scheduling and release

```text
JobScheduler
  -> delayed Sorted Set (JobId, epoch-millisecond due score)
  -> target Hash (JobId, queue name)
  -> SchedulerPoller.pollOnce(batchSize)
  -> per-Job atomic release script
       -> authoritative Job status/revision + status indexes
       -> remove delayed ZSET and target Hash entries
       -> insert JobId in target priority queue + membership Hash
  -> Worker acquires JobId and loads the authoritative Job
```

Scheduler indexes store JobIds and queue names only. The full scheduled `Instant` and other Job fields remain in the authoritative Job record. The poller uses an injected Clock and persistent rotating cursor. Its Lua release operation coordinates Job compare-and-set, lifecycle/status-index changes, scheduler cleanup, and queue membership atomically for each Job. Polling does not execute Jobs and is not automatically started by production bootstrap code.

## Revision concurrency

New Jobs have revision zero. Legacy serialized records without a revision are read as revision zero. Job writers compare revisions atomically and reject stale snapshots. Repository save and queue submission return their persisted snapshots; scheduler and dead-letter operations also preserve newer fields by checking the current record before writing. This protects priority, scheduledAt, retry fields, payload, metadata, and status from stale overwrites.

## Retry boundary

The current Worker retry path persists `RETRYING` and stores its timer in a Worker-local scheduled executor. That timer is not durable across process restart. Although `RedisJobScheduler` and `RedisSchedulerPoller` support durable delayed Jobs, Worker retry backoff is not connected to them. Manual DLQ operations are implemented independently.

## Phase 8 deferred integrations

1. Connecting Worker retry backoff to `RedisJobScheduler`.
2. Adding delayed and priority submission flags to the CLI.
3. Production runtime bootstrap and automatic poller lifecycle management.

See [project structure](project-structure.md), [job lifecycle](job-lifecycle.md), the [Phase 8 guide](../versions/v8-scheduling-and-priority/README.md), [ADR-010](../decisions/ADR-010-numeric-priority-queue.md), and [ADR-011](../decisions/ADR-011-durable-job-scheduling.md).
