# Phase 3 — Queue System

## 1. Objective

Implement a Redis-backed, reference-based job queue that stores and distributes `JobId`s using FIFO ordering. Address queue/persistence atomicity, support blocking and non-blocking dequeue, and coordinate status transitions (`PENDING` → `QUEUED`, `QUEUED` → `PROCESSING`).

## 2. Why This Phase Exists

A queue decouples job producers from job workers:
- **Reference-Based Architecture:** The queue holds lightweight `JobId` strings (`Queue -> JobId`), while authoritative job data lives exclusively in `JobRepository`.
- **Buffering & Load Leveling:** Jobs accumulate safely in Redis lists when workers are busy or offline.
- **Fair FIFO Dispatch:** Jobs are acquired in the order they were submitted.
- **Worker Prerequisite:** Workers (Phase 4) require a queue mechanism to poll before they can process work.

**Depends on:**
- Phase 1 (Domain Model: `Job`, `JobId`, `JobStatus`)
- Phase 2 (Persistence: `JobRepository`, `RedisJobRepository`, `RedisConfig`)

## 3. Scope

**In scope:**
- `JobQueue` interface operating strictly on `JobId`
- `RedisJobQueue` implementation using Redis Lists (`LPUSH` / `RPOP` / `BRPOP`)
- Named queue support (`jobstream:queue:{name}`)
- Investigating and implementing queue/persistence atomicity during enqueue
- Blocking dequeue with timeout and non-blocking dequeue
- Status transitions: `PENDING → QUEUED` (on enqueue), `QUEUED → PROCESSING` (on dequeue)
- Concurrency tests verifying at-most-one delivery across concurrent consumers

**Out of scope:**
- Worker process loops, registration, or heartbeats (Phase 4)
- Job execution logic (Phase 5)
- Priority queues and scheduled delays (Phase 8)
- Dead-letter queues (Phase 6)

## 4. Prerequisites

- Phase 2 complete (`JobRepository` persists and retrieves jobs)
- Redis 7+ running locally
- Understanding of Redis list commands (`LPUSH`, `RPOP`, `BRPOP`, `LLEN`, `LRANGE`)

## 5. Read Before Starting

- `docs/requirements/queue-semantics.md` — Queue semantics and atomicity requirements
- `docs/architecture/project-structure.md` — Package location for `queue`
- `docs/architecture/system-overview.md` — Architectural data flow
- `docs/architecture/job-lifecycle.md` — Lifecycle transitions `PENDING → QUEUED → PROCESSING`
- `docs/decisions/ADR-002-redis-as-persistence-and-queue-backend.md` — Redis context

## 6. Concepts to Understand

- **Reference-Based Queueing:** The queue stores only `JobId` strings, not full JSON payloads. This eliminates duplicate data in Redis and prevents desynchronization between queue items and repository records.
- **Redis Lists for FIFO:**
  - Push to head: `LPUSH jobstream:queue:default <jobId>`
  - Pop from tail: `RPOP jobstream:queue:default` (or `BRPOP` for blocking wait)
- **Blocking Operations (`BRPOP`):** Blocks the connection until an element arrives or timeout elapses. Allows workers to sleep without consuming CPU cycles.
- **Atomicity in Redis:**
  - `MULTI` / `EXEC`: Redis transaction pipeline executing queued commands sequentially.
  - Lua scripts (`EVAL`): Run atomically on the Redis server and are one possible mechanism for conditional operations when compatible with the persistence representation.

## 7. Design Decisions & Atomicity Analysis

### 7.1 Canonical Queue Content: Reference-Based

**Decision:** The queue stores **only** the string representation of `JobId`.
- Queue key: `jobstream:queue:{queueName}`
- Value in list: `<jobId-uuid>`
- The worker pops the `JobId`, then queries `JobRepository.findById(jobId)` to obtain the `Job` entity.

### 7.2 Enqueue / Persistence Atomicity Analysis

**The Problem:** Enqueueing requires two operations:
1. Updating `Job` in `JobRepository` with status `QUEUED`.
2. Pushing `JobId` into `jobstream:queue:{queueName}`.

**Failure Scenarios:**
- **Case A (Stranded Job):** State becomes `QUEUED` in repository, but network fails before queue insertion. Result: The job is marked `QUEUED`, but no worker will ever receive it.
- **Case B (Phantom Item):** `JobId` enters queue, but repository save fails. Result: A worker pops a `JobId` that does not exist (or remains in `PENDING`) in the repository.

**Architectural Requirement:** The selected mechanism must atomically enforce the `JobRepository`/`JobQueue` invariant: a job recorded as `QUEUED` has its `JobId` in the target queue, and an enqueued `JobId` refers to a repository job recorded as `QUEUED`.

**Mechanism to Investigate & Decide in Phase 3:** Evaluate `MULTI`/`EXEC` and Lua against the actual Phase 2 persistence representation, including its serialized job value and any status-index writes.
- **Redis `MULTI`/`EXEC`:** May group the repository writes required by that representation with `LPUSH` in one transaction.
- **Redis Lua (`EVAL`):** May be considered only if that representation permits the required atomic validation and whole-value writes. A Lua script must not be assumed to directly modify JSON job state; RedisJSON or any other Redis module is not part of this architecture unless explicitly adopted later.

Neither mechanism is predetermined. Select and test the implementation that preserves the invariant for the V2 representation, then record the result in ADR-005.

**Record this decision in:** ADR-005 (to be created during this phase).

## 8. Requirements

### Functional Requirements

1. **`JobQueue` Interface (package `io.github.pandeyayushk.jobstream.queue`):**
   - `void enqueue(JobId jobId, String queueName)`
   - `Optional<JobId> dequeue(String queueName, Duration timeout)`
   - `Optional<JobId> dequeueNonBlocking(String queueName)`
   - `long size(String queueName)`
   - `List<JobId> peek(String queueName, int count)`

2. **`RedisJobQueue` Implementation:**
   - Queue key schema: `jobstream:queue:{queueName}`.
   - Default queue name: `"default"`.
   - Uses `JedisPool` for thread-safe operations.
   - Enqueue coordinates with `JobRepository` to ensure status transition to `QUEUED`.
   - Dequeue returns `Optional<JobId>` (empty if timeout expires or queue is empty).

3. **Queue Coordinator / Service Layer:**
   - Provides convenience method: `void submit(Job job, String queueName)`:
     - Saves `job.withStatus(JobStatus.QUEUED)` in `JobRepository`.
     - Pushes `job.getId()` into `JobQueue`.
     - Guarantees the atomicity invariant.

### Non-Functional Requirements

- **O(1) Performance:** Enqueue and dequeue execute in constant time.
- **Thread Safety:** Multiple concurrent threads can enqueue and dequeue safely.

## 9. File Change Manifest

### CREATE

```
src/main/java/io/github/pandeyayushk/jobstream/queue/JobQueue.java
src/main/java/io/github/pandeyayushk/jobstream/queue/RedisJobQueue.java
src/main/java/io/github/pandeyayushk/jobstream/queue/QueueException.java
src/test/java/io/github/pandeyayushk/jobstream/queue/RedisJobQueueTest.java
```

### MODIFY

```
src/main/java/io/github/pandeyayushk/jobstream/job/Job.java — Add status transition convenience methods if not already present in Phase 1
```

### DELETE

None.

### DO NOT TOUCH

```
src/main/java/.../job/JobStatus.java   — All 7 lifecycle states already defined in Phase 1
src/main/java/.../job/JobId.java
src/main/java/.../payload/*
src/main/java/.../serialization/*
src/main/java/.../persistence/*
src/main/java/.../Main.java
pom.xml                               — Jedis already added in Phase 2
```

## 10. Implementation Order

1. **`JobQueue.java`** & **`QueueException.java`** — Define queue contract operating on `JobId`.
2. **`RedisJobQueue.java`** — Implement list operations with `JedisPool`.
3. **`RedisJobQueueTest.java`** — Unit and integration tests for FIFO ordering, timeouts, and size.
4. Integrate atomicity coordination between `RedisJobQueue` and `JobRepository`.
5. Concurrency tests with multiple threads competing for jobs.

## 11. Testing Requirements

### Integration Tests (Requires Redis)
- **FIFO Ordering:** Enqueue `id1`, `id2`, `id3` → Dequeue returns `id1`, `id2`, `id3` in order.
- **Blocking Timeout:** Dequeue on empty queue blocks for specified `Duration` and returns `Optional.empty()`.
- **Non-blocking Dequeue:** Dequeue on empty queue returns `Optional.empty()` immediately.
- **Queue Size & Peek:** `size()` matches count; `peek(2)` returns first two IDs without removing them.
- **Multiple Named Queues:** Queues `alpha` and `beta` are completely independent.

### Concurrency Tests
- 10 threads dequeuing concurrently from a queue with 50 jobs:
  - Exactly 50 jobs are dequeued.
  - Zero duplicate IDs received across threads.
  - Queue size is 0 at the end.

## 12. Failure and Edge Cases

- Redis offline during enqueue/dequeue → throws `QueueException`.
- Invalid queue name (null or empty) → throws `IllegalArgumentException`.
- Timeout of zero or negative duration → handled gracefully per method contract.

## 13. Validation

```powershell
# Compile
mvn compile

# Run Phase 3 tests
mvn test -Dtest="RedisJobQueueTest"

# Inspect queue keys in Redis
redis-cli LLEN jobstream:queue:default
redis-cli LRANGE jobstream:queue:default 0 -1
```

## 14. Completion Criteria

- [ ] `JobQueue` defined operating strictly on `JobId`.
- [ ] `RedisJobQueue` implements FIFO list operations with blocking timeout.
- [ ] Dual-write atomicity analyzed, tested, and resolved via Redis transaction.
- [ ] Concurrency test proves zero duplicate job acquisitions.
- [ ] `JobStatus` was **not** modified (all states were already present from Phase 1).
- [ ] All unit and integration tests pass cleanly (`mvn test` exits 0).

## 15. Self-Review Checklist

- [ ] Does the queue store ONLY `JobId` strings? (Must be YES).
- [ ] Is `RedisJobQueue` free of serialized JSON payload duplication?
- [ ] Are blocking dequeues releasing connections cleanly on timeout?
- [ ] Did we avoid modifying `JobStatus.java`?

## 16. Documentation Updates

### Requirements documentation
- Update `docs/requirements/queue-semantics.md` with tested atomicity results.

### Architecture documentation
- Confirm `docs/architecture/system-overview.md` matches implemented queue behavior.

### ADRs
- **Create ADR-005:** Queue/Persistence Atomicity & Reference-Based Queue Storage.

### Final README Notes
- Queues operate on `JobId` references backed by Redis lists with atomic push/pop.
- Blocking dequeue eliminates polling overhead.

## 17. Git Milestone

- **Branch:** `phase-3-queue-system`
- **Base:** `master` (after Phase 2 merge)
- **Implementation milestone:** `JobQueue` and `RedisJobQueue` complete.
- **Testing milestone:** Concurrency and FIFO tests pass with 0 errors.
- **Review milestone:** ADR-005 created, self-review complete.
- **Merge condition:** Clean build and test execution on master.

## 18. What This Phase Enables

With the queue system in place:
- **Phase 4 (Worker Foundation)** can build worker loops that pop `JobId`s from `JobQueue` and fetch full `Job` entities from `JobRepository`.
