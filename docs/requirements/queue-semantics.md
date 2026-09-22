# Queue Semantics Requirements

## 1. Purpose

Defines the implemented Phase 3 queue contract: FIFO dispatch of `JobId` values and atomic submission of a queued job to Redis.

## 2. Queue Contract

`JobQueue` operates only on `JobId` values:

```java
void enqueue(JobId jobId, String queueName);
Optional<JobId> dequeue(String queueName, Duration timeout);
Optional<JobId> dequeueNonBlocking(String queueName);
long size(String queueName);
List<JobId> peek(String queueName, int count);
```

`RedisJobQueue` stores IDs at `jobstream:queue:<queueName>`; it never stores a complete `Job`.

### FIFO and inspection

- Enqueue uses `LPUSH`.
- Non-blocking dequeue uses `RPOP`; blocking dequeue uses `BRPOP`.
- Thus jobs enqueued as `Job1`, `Job2`, and `Job3` are dequeued as `Job1`, `Job2`, and `Job3`.
- The physical Redis list is newest-to-oldest. `peek` reads and reverses the relevant tail range, returning logical FIFO order without removal.
- Named queues are isolated. `size` uses the Redis list length.

### Blocking, validation, and failures

- `dequeue` requires a non-null, positive `Duration`. Redis blocking time is expressed in whole seconds; the implementation rounds a fractional second up rather than claiming sub-second precision.
- A timed-out blocking dequeue and an empty non-blocking dequeue return `Optional.empty()`.
- Null or blank queue names, null IDs, and negative peek counts are rejected with `IllegalArgumentException`; `peek(..., 0)` returns an empty list.
- Redis/Jedis failures, and an invalid ID returned from Redis, are reported as `QueueException`.
- `RPOP` and `BRPOP` atomically remove one list element. Competing consumers therefore cannot receive the same removed ID during normal Redis operation.

## 3. Submission Boundary

`QueueCoordinator` owns the application-level `submit(Job, String)` operation. `QueueCoordinatorImp` validates its inputs, creates `queuedJob` by transitioning the supplied job to `QUEUED`, and passes both the original and queued jobs to `JobSubmissionStore`. It neither invokes Redis directly nor depends on `RedisJobQueue`.

`RedisJobSubmissionStore` is the Redis-specific implementation. In one `MULTI`/`EXEC` transaction it performs:

1. `SET jobstream:job:<id> <queued-job-json>`
2. `SREM jobstream:status:<original-status> <id>`
3. `SADD jobstream:status:QUEUED <id>`
4. `LPUSH jobstream:queue:<queueName> <id>`

The original job identifies the status index to remove; the queued job is the state persisted. This supports, for example, both `PENDING → QUEUED` and valid `DEAD → QUEUED` transitions. The job record remains authoritative in `JobRepository`; the queue remains a reference list.

`MULTI`/`EXEC` executes the queued commands sequentially without interleaving from other clients. It is not a traditional rollback transaction: Redis does not undo commands that have already executed if a later command fails at execution time.

## 4. Ownership and Boundaries

- Phase 3 owns submission transitions to `QUEUED`, including `PENDING → QUEUED`.
- Dequeue returns only a `JobId`; it does not perform `QUEUED → PROCESSING`.
- Phase 4's Worker will load the authoritative job from `JobRepository` after dequeue and own `QUEUED → PROCESSING`.
- `JobRepository` owns job persistence. `JobQueue` owns list operations. Neither depends on the other, and submission logic does not belong in `RedisJobQueue`.

## 5. Verified Coverage

The Phase 3 tests cover FIFO enqueue/dequeue and peek, timeout and empty behavior, size, named-queue isolation, input validation, Redis failure translation, ten concurrent consumers of fifty unique IDs, atomic submission, persisted status/index verification, `PENDING → QUEUED`, `DEAD → QUEUED`, submitted-ID queueing, and coordinator validation/transition behavior.
