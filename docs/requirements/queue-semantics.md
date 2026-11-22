# Queue Semantics Requirements

## Purpose

Define the implemented active queue contract: named priority ordering over JobId references, duplicate protection, legacy FIFO List migration, and atomic Job submission.

## Queue API

`JobQueue` operates on `JobId` values:

```java
void enqueue(JobId jobId, String queueName);
Optional<JobId> dequeue(String queueName, Duration timeout);
Optional<JobId> dequeueNonBlocking(String queueName);
long size(String queueName);
List<JobId> peek(String queueName, int count);
```

## Priority order and FIFO ties

- `Job.priority()` is an integer defaulting to zero; larger integers have higher priority. Negative values are valid.
- `RedisJobQueue` stores each active named queue in a Redis Sorted Set at `jobstream:priority-queue:<queueName>`, with score equal to negative priority.
- A fixed-width Redis Stream ID in the member orders jobs of equal score FIFO by atomic insertion order. The Stream key `jobstream:priority-sequence:<queueName>` allocates that ordering ID; `jobstream:priority-members:<queueName>` maps JobId to its unique member.
- Queue structures contain JobIds, not serialized Jobs. `RedisJobQueue.enqueue` reads priority from the authoritative Job record and uses zero when the record is absent or has legacy priority data.
- Lua scripts atomically maintain queue entries and membership. Dequeue removes the first (highest-priority, earliest equal-priority) member and its membership entry; it returns only the JobId.
- Each named queue has an independent order. `size` counts active ZSET members and `peek` returns entries in dequeue order without removal.

`dequeueNonBlocking` returns empty when no JobId is ready. `dequeue` repeatedly checks the queue for a positive timeout and returns empty when it expires; the implementation uses short waits between non-blocking Redis operations rather than Redis `BRPOP`. Null or blank queue names, null IDs, invalid timeouts, and negative peek counts are rejected; `peek(..., 0)` returns an empty list. Redis failures and malformed returned IDs are reported as `QueueException`.

## Legacy List compatibility

The former active FIFO List key `jobstream:queue:<queueName>` is a migration source, not the active queue representation. On first use of a queue name, an atomic migration reads entries in their original FIFO order, loads priority from authoritative Job records, populates the priority ZSET and membership Hash, and deletes the old List after migration. Missing or legacy Jobs default to priority zero. Duplicate IDs are represented once. The DLQ List `jobstream:queue:dead-letter` is separate and remains a List.

## Submission boundary

`QueueCoordinatorImp` validates inputs, creates a `QUEUED` Job copy, and delegates to `JobSubmissionStore`; it does not issue Redis commands. `RedisJobSubmissionStore` uses a Lua compare-and-set script to atomically validate the Job revision and Redis key types, persist the queued Job snapshot, update status indexes, allocate its FIFO member, and insert it into the priority queue. The API returns the exact persisted queued snapshot, including its incremented revision.

Job records remain authoritative. The queue and status indexes are references; a stale submission snapshot is rejected instead of overwriting a newer Job. Legacy queue migration runs before the submission script and is itself atomic.

## Ownership and boundaries

- Queue submission owns `PENDING -> QUEUED`; durable scheduler release also owns `PENDING -> QUEUED` and `RETRYING -> QUEUED` for due scheduled Jobs.
- Dequeue does not perform `QUEUED -> PROCESSING`. Worker loads the authoritative Job and owns that transition.
- `JobRepository` owns Job persistence. `JobQueue` owns ready-queue operations. `QueueCoordinator` is the application submission boundary.
- `RedisSchedulerPoller` reuses the same priority ordering and membership representation when releasing scheduled Jobs.
