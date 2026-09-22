# Phase 3 — Queue System

## Objective and Result

Phase 3 is complete. It provides a Redis-backed FIFO queue of `JobId` references and an atomic Redis submission path that persists a job as `QUEUED` while placing its ID on a named queue.

## Implemented Components

- `JobQueue` defines enqueue, blocking and non-blocking dequeue, size, and peek operations.
- `RedisJobQueue` implements those operations for keys named `jobstream:queue:<queueName>`.
- `QueueCoordinator` and `QueueCoordinatorImp` implement application-level `submit(Job, String)`.
- `JobSubmissionStore` abstracts atomic submission; `RedisJobSubmissionStore` supplies the Redis implementation.
- `QueueException` represents Redis/Jedis queue-boundary failures.

## FIFO Queue Semantics

The queue stores only `JobId` values, never serialized `Job` objects. `LPUSH` inserts new IDs at the list head; `RPOP` and `BRPOP` remove the oldest ID from the tail. Therefore enqueuing `Job1`, `Job2`, and `Job3` dequeues them in that same order. The physical list is the reverse of logical FIFO order; `peek` reverses its tail-range result so callers see FIFO order without removal.

Blocking dequeue accepts a non-null, positive `Duration`; timeout precision is whole seconds, with fractional seconds rounded up. A timeout and a non-blocking dequeue of an empty queue return `Optional.empty()`.

## Submission and Atomicity

`QueueCoordinatorImp` validates the supplied job and queue name, transitions the supplied job to `QUEUED`, then gives both `originalJob` and `queuedJob` to `JobSubmissionStore`. It contains no Redis operations and has no `RedisJobQueue` dependency.

`RedisJobSubmissionStore` executes this fixed transaction using Redis `MULTI`/`EXEC`:

1. Persist the queued JSON: `SET jobstream:job:<id> <queued-job-json>`.
2. Remove the ID from the original status index: `SREM jobstream:status:<original-status> <id>`.
3. Add the ID to `jobstream:status:QUEUED`.
4. Add the ID to `jobstream:queue:<queueName>` with `LPUSH`.

Both job forms are necessary: the original status determines the index to remove, and the queued form is the new authoritative persisted value. This supports valid submission transitions such as `PENDING → QUEUED` and `DEAD → QUEUED`.

`MULTI`/`EXEC` prevents other Redis clients from interleaving commands with this sequence. It is not a rollback transaction: Redis does not automatically undo an already executed command if a later command encounters an execution-time error. ADR-005 records the decision and trade-off.

## Boundaries and Lifecycle Ownership

`JobRepository` is authoritative for complete job records. `JobQueue` handles only ID list operations. Neither depends on the other, and the atomic submission logic is deliberately not part of `RedisJobQueue`.

Phase 3 owns the transition to `QUEUED`. Dequeue only returns a `JobId`; it does not transition the job to `PROCESSING`. In Phase 4, a Worker will dequeue the ID, load the job from `JobRepository`, and own `QUEUED → PROCESSING`.

## Validation and Coverage

The suite contains 72 tests in total. Phase 3 coverage includes FIFO enqueue/dequeue and peek, timeout and empty behavior, size, queue isolation, validation of queue names, IDs, timeouts, and peek counts, failure translation, concurrent consumption by ten consumers of fifty unique IDs, atomic submission, persisted status/index checks, both `PENDING → QUEUED` and `DEAD → QUEUED`, and coordinator behavior.

```powershell
mvn test
```

Redis integration tests require Redis at `localhost:6379`.
