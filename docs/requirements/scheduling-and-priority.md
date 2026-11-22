# Scheduling and Priority Requirements

## Purpose

Define JobStream's numeric priority and durable delayed-scheduling contracts. The contracts below describe the infrastructure currently implemented in Phase 8; Worker retry integration, CLI options, and automatic poller lifecycle remain deferred.

## Functional requirements

### Job priority

- A Job has an integer priority with default value `0`.
- Any positive or negative `int` value is valid; larger values have higher priority.
- Updating priority creates a new immutable Job and preserves all other fields.

### Priority queues

- Each named active queue contains JobId references, not serialized Job objects.
- Dequeue returns the highest-priority available Job first.
- Equal-priority Jobs retain FIFO order based on atomic queue insertion order.
- Queue membership is unique by JobId; dequeue clears the membership index atomically.
- Existing FIFO List entries are migrated without stranding queued Jobs. The DLQ List remains separate.

### Durable scheduling

- A scheduled Job is indexed by JobId and destination queue; its authoritative Job record retains payload, priority, retry state, metadata, and the full scheduled Instant.
- Explicit and relative scheduling are supported. Relative delay must be non-negative; zero delay is allowed.
- Only `PENDING` and `RETRYING` Jobs may be scheduled. Scheduling itself does not change status.
- A reschedule updates the existing due time and destination for that JobId.
- Due Jobs remain out of active worker queues until poller release.

### Scheduler release

- One poll call examines a bounded candidate batch and releases due Jobs into their configured named priority queues.
- Eligible statuses transition `PENDING -> QUEUED` and `RETRYING -> QUEUED`.
- Release preserves priority, retry information, payload, metadata, and other Job fields, and increments the persisted revision.
- For each Job, status and revision updates, status-index changes, scheduler-index removal, priority queue insertion, and membership-index update are atomic.
- Competing pollers cannot release a Job twice. Queue membership remains unique when a Job is already indexed in its destination queue.
- The poller releases work only; it does not execute Jobs.

## Non-functional requirements

- **Revision safety:** concurrent Job writers use optimistic revision checks; stale snapshots cannot overwrite newer Job fields. Legacy records without a revision are treated as revision zero.
- **Candidate fairness:** deferred entries remain recoverable and do not permanently monopolize the beginning of candidate selection. A persistent cursor progresses across polls, process restarts, and concurrent poller instances. After a finite set of index mutations, repeated polls over a stable due set visit every entry.
- **Bounded processing:** `batchSize` must be positive and caps the candidate JobIds handed to Java for deserialization and release attempts per invocation. Per-Job commits are atomic; a batch is not a single all-or-nothing transaction. Redis may internally traverse skipped sorted-set ranks, so command cost is not constant as the due set grows.
- **Recovery:** missing Jobs and Jobs no longer in a schedulable state can be cleaned from scheduler indexes. Malformed records, invalid JobIds, and missing or blank destinations remain indexed for repair. Redis and incompatible-type failures surface as errors.
- **Time:** an injectable Clock makes due-time selection deterministic. Sorted Set scores use epoch milliseconds and must be exactly representable in Redis's floating-point score range, `[-2^53, 2^53]`; the complete Instant remains in the Job record.
- **No recurrence:** cron and recurring schedules are outside this contract.

## Implemented Redis key structures

| Key pattern | Type | Purpose |
|---|---|---|
| `jobstream:priority-queue:<queueName>` | Sorted Set | Active JobId members; score is negative priority |
| `jobstream:priority-sequence:<queueName>` | Stream | Per-queue FIFO sequence allocator |
| `jobstream:priority-members:<queueName>` | Hash | JobId-to-member uniqueness index |
| `jobstream:queue:<queueName>` | List | Legacy active queue source, removed after migration |
| `jobstream:schedule:delayed` | Sorted Set | Due epoch-millisecond score to JobId |
| `jobstream:schedule:targets` | Hash | JobId to target queue name |
| `jobstream:schedule:poll-cursor` | String | Persistent rotating candidate rank |
| `jobstream:job:<jobId>` | String | Authoritative serialized Job record |
| `jobstream:status:<STATUS>` | Set | Secondary JobId status index |

## Deferred integrations

1. Connecting Worker retry backoff to durable `RedisJobScheduler` storage.
2. Adding priority and delay submission flags to the CLI.
3. Production bootstrap and automatic scheduler poller lifecycle management.
