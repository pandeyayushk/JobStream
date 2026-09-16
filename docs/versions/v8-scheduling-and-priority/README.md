# Phase 8 — Scheduling & Priority Queues

## 1. Objective

Enable time-delayed job execution (jobs scheduled for future execution) and priority-based queueing (ensuring critical jobs are dequeued before lower-priority ones) backed by Redis Sorted Sets (`ZSET`) and multi-tier lists.

## 2. Why This Phase Exists

Standard FIFO queues treat every job equally and process them immediately upon arrival. In production environments:
- Certain jobs must be deferred (e.g., "send follow-up email in 24 hours", "poll third-party status in 5 minutes").
- Delayed retries from Phase 6 require non-blocking scheduling rather than thread sleeping, allowing workers to stay productive.
- Critical operations (e.g., password reset, payment verification) must leapfrog long background batches (e.g., monthly report generation).

**Depends on:**
- Phase 1 (Domain Model: `Job`, `JobStatus`)
- Phase 2 (Persistence: `JobRepository`)
- Phase 3 (Queue: `JobQueue`)
- Phase 4 (Worker: `Worker`)
- Phase 6 (Reliability: `RetryPolicy`)

## 3. Scope

**In scope:**
- Priority support in `Job` (integer priority level, default: 0)
- `PriorityJobQueue` backed by multi-level Redis lists or Redis Sorted Sets
- Scheduled / delayed job support: `JobScheduler` interface and Redis `ZSET` implementation storing `JobId` references where score = scheduled execution epoch milliseconds
- Background `SchedulerPoller` to move mature scheduled `JobId`s into active queues
- Integrating delayed retries from Phase 6 into `JobScheduler` (non-blocking delay)
- CLI commands to submit delayed and prioritized jobs

**Out of scope:**
- Recurring cron syntax expressions (deferred to post-MVP / Phase 10)
- Distributed leader election for scheduler poller (addressed in Phase 10)

## 4. Prerequisites

- Phase 7 complete (CLI functional)
- Redis instance running
- Understanding of Redis Sorted Sets (`ZSET`) and time-based scoring

## 5. Read Before Starting

- `docs/requirements/scheduling-and-priority.md` — Scheduling & Priority requirements
- `docs/architecture/project-structure.md` — Packages `priority` and `schedule`
- `docs/architecture/system-overview.md` — Architectural role of scheduling

## 6. Concepts to Understand

- **Redis Sorted Sets (ZSET):** Collection of unique members ordered by a floating-point score.
  - Commands: `ZADD`, `ZRANGEBYSCORE`, `ZREMRANGEBYSCORE`, `ZPOPMIN`.
  - Reference: [Redis Sorted Sets](https://redis.io/docs/data-types/sorted-sets/)
- **Time as Score Pattern:** Storing `JobId` in a ZSET with timestamp (`Instant.toEpochMilli()`) as score.
  - Querying `ZRANGEBYSCORE jobstream:schedule:delayed 0 currentTime` finds all jobs ready to run.
- **Priority Modeling in Redis:**
  - Option A: Single ZSET where score = priority (requires polling or Lua scripts).
  - Option B: Multi-queue priority (e.g. `queue:high`, `queue:default`, `queue:low`) dequeued via `BRPOP queue:high queue:default queue:low 0`.

## 7. Design Decisions

### 7.1 Priority Queue Architecture

**Decision:** Multi-level priority queues (`high`, `default`, `low`) leveraging Redis `BRPOP multi-key` semantics (`BRPOP queue:high queue:default queue:low 0`).
- Provides instant, blocking, zero-overhead priority consumption.
- Preserves O(1) performance without continuous polling loops.

**Record this decision in:** ADR-010 (to be created during this phase).

### 7.2 Scheduled Jobs Migration Mechanism: Reference-Based

**Decision:**
- Delayed jobs store their `JobId` in a Redis Sorted Set: `jobstream:schedule:delayed`.
  - Member: `jobId` string.
  - Score: Scheduled execution epoch milliseconds (`executeAt.toEpochMilli()`).
- A dedicated background daemon (`SchedulerPoller`) queries mature jobs using an atomic Lua script:
  - Fetches `JobId`s with score $\le \text{now}$.
  - Removes them from the ZSET (`ZREM`).
  - Pushes them onto their destination active queue (`LPUSH`).
  - Updates job status in `JobRepository` to `QUEUED`.

**Record this decision in:** ADR-010.

## 8. Requirements

### Functional Requirements

1. **Job Model Updates:**
   - Field `int priority` (default: 0).
   - Field `Optional<Instant> scheduledAt` (nullable/optional).
   - Methods: `withPriority(int priority)`, `withScheduledAt(Instant instant)`.

2. **`PriorityJobQueue` Interface & Implementation:**
   - Enqueue jobs specifying priority tier (`HIGH`, `DEFAULT`, `LOW`).
   - Dequeue automatically prioritizes higher-priority tiers before lower ones.

3. **`JobScheduler` Interface & `RedisJobScheduler`:**
   - `void schedule(JobId jobId, Instant executeAt, String targetQueue)`
   - `void scheduleDelayed(JobId jobId, Duration delay, String targetQueue)`
   - `long scheduledCount()`
   - Storage key: `jobstream:schedule:delayed`.

4. **`SchedulerPoller`:**
   - Periodic background task (using `ScheduledExecutorService`, e.g. every 500ms).
   - Atomically transfers mature `JobId`s to destination queues.

5. **Phase 6 Retry Integration:**
   - When a job fails with a backoff delay, `Worker` invokes `JobScheduler.scheduleDelayed(job.getId(), backoff, targetQueue)`.
   - The worker thread immediately returns to processing the next job (non-blocking).

### Non-Functional Requirements

- Scheduled job migration must be atomic (no lost jobs on poller crash).
- Poller overhead on Redis must be negligible when no jobs are pending.

## 9. File Change Manifest

### CREATE

```
src/main/java/io/github/pandeyayushk/jobstream/priority/Priority.java
src/main/java/io/github/pandeyayushk/jobstream/priority/PriorityJobQueue.java
src/main/java/io/github/pandeyayushk/jobstream/priority/RedisPriorityJobQueue.java
src/main/java/io/github/pandeyayushk/jobstream/schedule/JobScheduler.java
src/main/java/io/github/pandeyayushk/jobstream/schedule/RedisJobScheduler.java
src/main/java/io/github/pandeyayushk/jobstream/schedule/SchedulerPoller.java
src/test/java/io/github/pandeyayushk/jobstream/priority/RedisPriorityJobQueueTest.java
src/test/java/io/github/pandeyayushk/jobstream/schedule/RedisJobSchedulerTest.java
src/test/java/io/github/pandeyayushk/jobstream/schedule/SchedulerPollerTest.java
```

### MODIFY

```
src/main/java/io/github/pandeyayushk/jobstream/job/Job.java               — Add priority and scheduledAt fields
src/main/java/io/github/pandeyayushk/jobstream/worker/Worker.java         — Delegate delayed retries to JobScheduler
src/main/java/io/github/pandeyayushk/jobstream/cli/command/JobCommand.java— Add --priority and --delay flags
```

### DELETE

None.

### DO NOT TOUCH

```
src/main/java/.../serialization/*
src/main/java/.../executor/*
pom.xml
```

## 10. Implementation Order

1. **`Job.java`** — Add `priority` and `scheduledAt` fields.
2. **`Priority.java`** & **`PriorityJobQueue.java`** — Multi-queue priority abstraction.
3. **`RedisPriorityJobQueueTest.java`** — Test priority-ordered dequeueing.
4. **`JobScheduler.java`** & **`RedisJobScheduler.java`** — ZSET storage for delayed `JobId`s.
5. **`SchedulerPoller.java`** — Migration loop transferring mature jobs to active queues.
6. **`SchedulerPollerTest.java`** — Verify accuracy and atomicity of time-based migration.
7. **`Worker.java`** — Connect retry backoff to non-blocking `JobScheduler`.
8. **`JobCommand.java`** — Expose CLI options for delayed submission.

## 11. Testing Requirements

### Unit & Integration Tests
- **Priority Dequeueing:** Enqueue low-priority jobs, then high-priority job → Dequeue yields high-priority first.
- **Scheduled Delay:** Schedule job with 2-second delay → Queue size is 0 initially → After 2.2s, poller migrates job → Worker dequeues and completes job.
- **Non-Blocking Worker Retries:** Worker fails job with backoff → Job scheduled in ZSET → Worker thread immediately free for next task.

## 12. Failure and Edge Cases

- Poller crashes during transfer → Atomic Lua script guarantees zero drop or duplicate.
- Clocks slightly out of sync → Best-effort epoch millisecond ordering.

## 13. Validation

```powershell
# Compile
mvn compile

# Run priority and scheduling tests
mvn test -Dtest="RedisPriorityJobQueueTest,RedisJobSchedulerTest,SchedulerPollerTest"
```

## 14. Completion Criteria

- [ ] `Job` carries `priority` and `scheduledAt`.
- [ ] `PriorityJobQueue` prioritizes higher tiers before lower tiers via multi-key `BRPOP`.
- [ ] `RedisJobScheduler` stores future `JobId`s in Redis Sorted Sets.
- [ ] `SchedulerPoller` transfers mature jobs atomically into active queues.
- [ ] Worker retries utilize non-blocking scheduler delays.
- [ ] All tests pass (`mvn test` exits 0).

## 15. Self-Review Checklist

- [ ] Are retry delays completely non-blocking for worker threads? (Must be YES).
- [ ] Does `SchedulerPoller` clean up its threads on shutdown?
- [ ] Are all previous tests passing?

## 16. Documentation Updates

### Requirements documentation
- Update `docs/requirements/scheduling-and-priority.md` with implemented mechanisms.

### Architecture documentation
- Update `docs/architecture/project-structure.md` marking `priority` and `schedule` active.

### ADRs
- **Create ADR-010:** Redis Multi-Queue Priority & ZSET Time-Based Scheduler.

### Final README Notes
- Multi-tier priority queues powered by Redis multi-key operations.
- Non-blocking delayed and scheduled job execution powered by Redis Sorted Sets.

## 17. Git Milestone

- **Branch:** `phase-8-scheduling-and-priority`
- **Base:** `master` (after Phase 7 merge)
- **Implementation milestone:** `PriorityJobQueue`, `JobScheduler`, and `SchedulerPoller` operational.
- **Testing milestone:** Priority ordering and delay timing verified by integration tests.
- **Review milestone:** ADR-010 written, self-review complete.
- **Merge condition:** Clean build and test suite passing on master.

## 18. What This Phase Enables

With scheduling and priority in place:
- **Phase 9 (Observability & HTTP API)** can monitor scheduled job counts, queue latencies, and expose REST endpoints for delayed submissions.
- The queue engine supports production workflows requiring SLA differentiation and time-based triggering.
