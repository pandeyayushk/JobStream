# Queue Semantics Requirements

## 1. Purpose

Defines the behavior, guarantees, and failure modes of the queue dispatch mechanism responsible for transferring work from producers to distributed workers.

---

## 2. Canonical Queue Content: Reference-Based (`JobId`)

To avoid state synchronization bugs and duplicate payloads:
- **The Queue contains only `JobId` references.**
- The full `Job` entity resides exclusively in the `JobRepository`.
- **Flow:**
  1. Producer persists `Job` in `JobRepository`.
  2. Producer enqueues `JobId` into `JobQueue`.
  3. Worker dequeues `JobId` from `JobQueue`.
  4. Worker retrieves `Job` from `JobRepository` and transitions status to `PROCESSING`.

---

## 3. Functional Requirements

### 3.1 FIFO Ordering & Queue Operations (`JobQueue`)
- Default behavior is FIFO (First-In, First-Out) using Redis Lists:
  - Enqueue: `LPUSH jobstream:queue:{name} {jobId}`
  - Dequeue: `RPOP` (non-blocking) or `BRPOP` (blocking)
- Operations required:
  - `void enqueue(JobId jobId, String queueName)`
  - `Optional<JobId> dequeue(String queueName, Duration timeout)`
  - `Optional<JobId> dequeueNonBlocking(String queueName)`
  - `long size(String queueName)`
  - `List<JobId> peek(String queueName, int count)`
- Support for multiple independent named queues (e.g. `default`, `notifications`, `reports`).

### 3.2 Worker Acquisition Semantics
- At-most-one worker receives each `JobId` upon dequeue (enforced by Redis atomic `RPOP`/`BRPOP`).
- Blocking dequeue must suspend the worker thread efficiently without busy-wait polling.

---

## 4. Queue / Persistence Atomicity Analysis

### 4.1 The Dual-Write Problem
Enqueueing a job requires two logical operations:
1. Update `Job` state in `JobRepository` (set status to `QUEUED`, record timestamp).
2. Insert `JobId` into `JobQueue`.

### 4.2 Failure Scenarios & Invariant Violations

- **Case A: State updated to `QUEUED`, but queue insertion fails.**
  - *Trigger:* Network disconnect or Redis failure after step 1, before step 2.
  - *Result:* **Stranded Job.** The job is marked `QUEUED` in the database, but will never be delivered to any worker because its `JobId` is not in the queue.
- **Case B: `JobId` inserted into queue, but state update fails.**
  - *Trigger:* Inserting into queue first, then repository write fails.
  - *Result:* **Phantom / Out-of-Sync Job.** A worker dequeues the `JobId`, but finds the job in `PENDING` state (or non-existent in repository).

### 4.3 Target Invariant & Atomic Boundary
- **Invariant:** A `JobId` must be visible in a `JobQueue` **if and only if** its corresponding record in `JobRepository` is persisted with status `QUEUED`.
- **Required Atomic Boundary:** The persistence write and queue insertion must execute as a single atomic unit.

### 4.4 Redis Coordination Mechanisms to Investigate (Phase 3 Ownership)
Because Redis serves as both the repository and the queue backend, the atomic boundary can be enforced using Redis native transactional primitives:
1. **Redis Transactions (`MULTI` / `EXEC`):**
   - Groups `SET jobstream:job:{id} ...` and `LPUSH jobstream:queue:{name} {id}` into a single transaction block.
   - *Limitation:* Redis transactions do not support rollbacks based on intermediate query values, but guarantee all-or-nothing execution without interleaving.
2. **Redis Lua Scripting (`EVAL` / `EVALSHA`):**
   - A single Lua script executes atomically on the Redis server, performing state update and list push in one uninterrupted step.
   - Ideal for conditional logic and atomic state validation.

*Phase 3 will benchmark and validate these mechanisms and record the final implementation choice in **ADR-005**.*

---

## 5. Non-Functional Requirements

- **Throughput:** O(1) time complexity for enqueue (`LPUSH`) and dequeue (`RPOP`/`BRPOP`).
- **Safety:** No duplicate delivery during normal operations. At-least-once delivery guaranteed across restarts through acknowledgment/recovery mechanisms (refined in Phase 6).

---

## 6. Dependencies

- Persistence Layer (Phase 2: `JobRepository`)
- Domain Model (Phase 1: `Job`, `JobId`, `JobStatus`)

---

## 7. Phase Ownership

- **Phase 3 (Queue System):** Designs `JobQueue`, implements `RedisJobQueue`, evaluates atomic enqueue mechanisms, and records ADR-005.
