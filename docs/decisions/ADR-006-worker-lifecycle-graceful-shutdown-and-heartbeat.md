# ADR-006: Worker Lifecycle, Graceful Shutdown, and Heartbeat

**Status:** Accepted

## Context

Phase 4 introduces workers that consume `JobId` values from the Phase 3 queue, load authoritative jobs from `JobRepository`, and coordinate processing without implementing the production job-type executor or retry system. Workers need an observable lifecycle, bounded concurrency, liveness records, and orderly shutdown.

## Decision

### Lifecycle and identity

Each worker is constructed with a UUID-based `WorkerId`. Its lifecycle is `STOPPED -> STARTING -> RUNNING -> STOPPING -> STOPPED`. `WorkerInfo` contains only `WorkerId`, `WorkerStatus`, and `startedAt`. Startup registers `STARTING`, initializes the worker resources and heartbeat schedule, registers `RUNNING`, and begins acquisition.

### Queue and execution boundary

`JobQueue` remains responsible for queue operations over `JobId` only. The worker dequeues an ID, loads its job from `JobRepository`, ignores missing jobs and jobs not in `QUEUED`, persists `PROCESSING`, and invokes `WorkerJobHandler`. Handler return leads to persisted `COMPLETED`; a handler `Exception` leads to persisted `FAILED`. `WorkerJobHandler` is the Phase 4 execution boundary and test seam. Production `JobExecutor` abstractions, implementations, type dispatch/factory, and job-type execution belong to Phase 5. Retry policy, scheduling, counters, retry transitions, and exhaustion/dead behavior belong to Phase 6.

### Registry and heartbeat

`RedisWorkerRegistry` stores metadata in `jobstream:worker:<workerId>` and the liveness key in `jobstream:worker:<workerId>:heartbeat`. Metadata is a Redis hash containing `id`, `status`, and `startedAt`. Heartbeat refresh sets the liveness key and applies configured expiration. Defaults from `WorkerConfig` are queue `default`, concurrency 4, heartbeat interval 10 seconds, heartbeat TTL 30 seconds, and shutdown timeout 30 seconds. The config requires TTL of at least one second and interval less than TTL.

Heartbeat refresh watches the metadata key, reads its existence/metadata, then queues heartbeat set and expiration commands in `MULTI` and executes them with `EXEC`. If metadata changes or is deleted between `WATCH` and `EXEC`, Redis aborts the transaction. This prevents a heartbeat racing with deregistration from recreating or refreshing a stale heartbeat after worker removal. `MULTI`/`EXEC` here is not rollback.

`getWorker()` reads registered metadata and does not verify liveness. `listActiveWorkers()` uses `SCAN` to discover metadata keys and checks the corresponding heartbeat key to determine liveness. `SCAN` avoids `KEYS`' single full-keyspace blocking operation during discovery.

### Concurrency and shutdown

Each worker uses a dedicated single-thread acquisition executor, a fixed processing executor sized by configured concurrency, a semaphore limiting submitted/in-flight jobs to that concurrency, and a separate scheduled heartbeat executor.

Shutdown changes status to `STOPPING`, stops acquisition and waits for its executor to terminate, allows already-submitted/in-flight jobs to finish within the configured timeout, shuts down heartbeat, deregisters the worker, and changes status to `STOPPED`. If shutdown begins after dequeue but before submission to processing, the worker re-enqueues the `JobId`. Processing that exceeds the timeout may be interrupted; the design does not promise completion after timeout.

## Alternatives Considered

The architecture explicitly separates queue references from persisted job records and reserves production executor dispatch for Phase 5. The worker concurrency arrangement and guarded shutdown behavior are described as implemented here; no additional historical alternatives are recorded.

## Consequences

- Queue and persistence responsibilities remain independent; job loading and state transitions belong to the worker.
- Handler failures and orphaned IDs are contained so the worker continues, while Phase 4 does not retry.
- A dequeued but not yet submitted ID is returned to the queue during the shutdown race.
- The heartbeat key provides TTL-based liveness independently of stored worker metadata.
- `SCAN`-based discovery may return results incrementally; active status is based on heartbeat-key existence when checked.
- Phase 5 can add production execution dispatch at the handler boundary, and Phase 6 can add retry behavior without attributing either feature to Phase 4.
