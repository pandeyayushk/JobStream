# Phase 8 — Scheduling & Priority Infrastructure

**Status: Infrastructure complete.** This guide records the implementation in the repository. It does not describe the deferred Worker, CLI, or runtime-bootstrap integrations as complete.

## 1. Implemented scope

Phase 8 adds numeric priority and an optional scheduled timestamp to the immutable `Job`, Redis-backed priority ordering, durable scheduling storage, revision-safe Job updates, and a poller that releases due Jobs into active queues. The authoritative Job remains in `jobstream:job:<jobId>`; the queue and scheduler indexes contain references and routing data, not serialized Job copies.

Implemented classes are in the existing `job`, `queue`, `persistence`, `serialization`, and `schedule` packages. There is no `priority` package.

## 2. Numeric priority queue

`Job.priority()` is an integer. New Jobs default to priority `0`; any positive or negative integer is accepted, and larger values have higher priority. `RedisJobQueue` implements `JobQueue`; `RedisPriorityQueueStore` in the `queue` package owns its Redis keys and scripts.

Each named active queue uses:

| Redis key | Type | Purpose |
|---|---|---|
| `jobstream:priority-queue:<queueName>` | Sorted Set | Active JobId members ordered by priority and FIFO tie-break |
| `jobstream:priority-sequence:<queueName>` | Stream | Allocates monotonically increasing Redis Stream IDs |
| `jobstream:priority-members:<queueName>` | Hash | Maps a JobId to its unique current Sorted Set member |

The Sorted Set score is the negative numeric priority. Its member is a fixed-width 20-digit Stream millisecond value, a hyphen, a fixed-width 20-digit Stream sequence value, a colon, and the JobId. Thus Redis's ascending score order puts higher priorities first; lexicographic member order preserves FIFO for equal-priority enqueue operations. Sequence values are not combined with priority in a floating-point score. `XADD *` allocates sequence IDs; the temporary Stream entry is deleted while Redis retains the Stream high-water mark.

The membership Hash prevents duplicate queue membership. Dequeue removes the first Sorted Set member and its JobId index entry atomically. Normal submission, Worker retry requeue, shutdown requeue, and DLQ requeue use the same priority queue representation and preserve the Job's persisted priority.

### Legacy queue migration

The former `jobstream:queue:<queueName>` active queue List is retained as a migration source. The first operation on a named active queue atomically reads the List in its original FIFO dequeue order, obtains each Job's priority from the authoritative Job record (defaulting to zero for missing/legacy priority data), inserts the ID into the priority queue, records membership, and deletes the old List after migration. Duplicate IDs are represented once. The DLQ List `jobstream:queue:dead-letter` is a separate structure and remains a List.

## 3. Durable delayed scheduling

`JobScheduler` exposes `schedule(JobId, Instant, targetQueue)`, `scheduleDelayed(JobId, Duration, targetQueue)`, and `scheduledCount()`. `RedisJobScheduler` stores:

| Redis key | Type | Value |
|---|---|---|
| `jobstream:schedule:delayed` | Sorted Set | JobId member; execution epoch-millisecond score |
| `jobstream:schedule:targets` | Hash | JobId field; destination queue name |

Scheduler indexes contain JobIds and routing data only. The Job record remains authoritative and stores the complete `scheduledAt` Instant, including sub-millisecond precision. The Redis score uses epoch milliseconds and is accepted only when exactly representable as a Redis Sorted Set score: within the epoch-millisecond `long` range and the exact integer range `[-2^53, 2^53]`.

Only `PENDING` and `RETRYING` Jobs can be scheduled. Scheduling updates `scheduledAt` and the Job revision atomically with the scheduling indexes, but leaves the lifecycle status unchanged while the Job waits. Rescheduling a JobId updates its existing score and destination. Delays must be non-negative; zero delay is allowed. Cron and recurring scheduling are not implemented.

## 4. Revision-based concurrency

`Job` carries a persisted `long revision`. New Jobs start at revision zero; serialized legacy Jobs with no revision are read as revision zero. Redis Job writers compare the supplied revision with the current stored revision in Lua, reject stale snapshots, increment the revision, and update related indexes in the same script where applicable. Revision comparisons use decimal strings rather than converting the value to a Lua floating-point number.

`RedisJobRepository.save` returns the persisted snapshot with the incremented revision. `RedisJobSubmissionStore.submit` and `QueueCoordinator.submit` return the exact queued snapshot committed by the atomic submission script. Dead-letter requeue also returns its persisted queued snapshot. Scheduler updates compare the complete serialized current Job snapshot and retry against a newer record after a conflict. This prevents an old repository, submission, retry, or DLQ snapshot from silently overwriting a newer `scheduledAt`, priority, retry field, or other Job data.

## 5. Scheduler poller

`SchedulerPoller.pollOnce(batchSize)` performs one bounded pass; it does not run its own daemon or execute Jobs. `RedisSchedulerPoller` accepts an injected `Clock` (system UTC by default), selects due entries whose score is at most the current epoch-millisecond time, and returns a `SchedulerPollResult` with examined, released, cleaned, and deferred counts. `batchSize` must be positive and caps the candidate JobIds returned to Java and the per-Job release attempts in one invocation.

Candidate progression uses the persistent Redis key `jobstream:schedule:poll-cursor`. A Lua selection script reads the due count, normalizes the stored rank modulo the current count, selects a bounded page with `ZRANGEBYSCORE ... LIMIT`, and advances the cursor atomically. Multiple pollers share the cursor; it survives process restart. Entries added, removed, repaired, or rescheduled can shift ranks, so the next selection normalizes and wraps. With a stable due set, repeated polls visit all candidates, including deferred entries. The per-Job atomic release remains the duplicate-release guard if concurrent changes cause overlap.

For each candidate, one release Lua script revalidates the due score, destination, and exact authoritative Job JSON; preflights the relevant Redis key types; changes `PENDING` or `RETRYING` to `QUEUED`; increments revision; updates status indexes; removes the JobId from both scheduler indexes; and inserts it into the destination priority queue and membership Hash. The release operation is atomic per Job, not for the whole batch: successful earlier releases stay committed if a later candidate fails.

Missing Jobs and Jobs that are no longer schedulable are removed from the scheduling Sorted Set and destination Hash. Malformed Job records, invalid JobIds, and missing or blank destinations remain scheduled for repair and are counted as deferred. Redis/type failures are surfaced as failures, not reported as successful deferrals. The batch limits candidate processing; Redis may internally traverse skipped sorted-set ranks for a later offset, so Redis command cost can increase with the due-set size.

## 6. Lifecycle and integration boundary

Scheduling leaves a `PENDING` Job as `PENDING` and a `RETRYING` Job as `RETRYING`. Poller release performs only these transitions:

```text
PENDING  -> QUEUED
RETRYING -> QUEUED
```

The existing Worker retry scheduler is still an in-memory Phase 6 mechanism. It has not been connected to `RedisJobScheduler`. The poller has no production runtime loop or lifecycle management, and the CLI has no priority or delay submission flags.

## 7. Deferred integrations

The following work is explicitly deferred:

1. Connecting Worker retry backoff to `RedisJobScheduler`.
2. Adding delayed and priority submission flags to the CLI.
3. Production runtime bootstrap and automatic poller lifecycle management.

## 8. Related documentation

- [Scheduling and Priority Requirements](../../requirements/scheduling-and-priority.md)
- [Queue Semantics Requirements](../../requirements/queue-semantics.md)
- [Project Structure](../../architecture/project-structure.md)
- [System Overview](../../architecture/system-overview.md)
- [Job Lifecycle](../../architecture/job-lifecycle.md)
- [ADR-010: Numeric Priority Queue Ordering](../../decisions/ADR-010-numeric-priority-queue.md)
- [ADR-011: Durable Redis Job Scheduling](../../decisions/ADR-011-durable-job-scheduling.md)
