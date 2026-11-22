# Persistence Requirements

## 1. Purpose

Defines the implemented contract and responsibilities for durable Job storage, authoritative records, status indexes, and revision-safe concurrent updates.

---

## 2. Functional Requirements

### 2.1 Repository Abstraction (`JobRepository`)

- All persistence operations are defined behind a technology-neutral interface:
  - `Job save(Job job)`: Persists a new revision and returns the persisted Job snapshot.
  - `Optional<Job> findById(JobId id)`: Retrieves the current state of a job by its unique identity.
  - `boolean delete(JobId id)`: Permanently deletes a job record and reports whether it existed.
  - `List<Job> findByStatus(JobStatus status)`: Queries jobs by lifecycle state.
  - `boolean exists(JobId id)`: Checks existence without full deserialization overhead.
- The interface does not leak Redis-specific commands, keys, or types to callers.
- A stale revision is rejected with `ConcurrentJobUpdateException`; callers must use the returned persisted snapshot as the basis for later updates.

### 2.2 Storage Model: `JobId -> Job Record`

- The repository stores the complete authoritative state of the job.
- Each job is indexed uniquely by its `JobId`.
- **Key Pattern:** `jobstream:job:<uuid>`.
- Value stored: Serialized JSON representation produced by `JobSerializer`.

### 2.3 Redis Key Schema and Status Index

```
jobstream:job:<uuid>
       |
       +--> JSON Job record

jobstream:status:<STATUS>
       |
       +--> Set<JobId>
```

The Job record is authoritative. Status sets are secondary indexes maintained atomically with repository writes.

- `save()` compares the supplied revision with the current record in a Lua script, stores the JSON with its incremented revision, and updates the old/current status indexes atomically. Stale snapshots are rejected; key-type or record validation failures do not partially update the record and indexes.
- `findById()` gets the JSON by `JobId`, returns `Optional.empty()` when absent, and deserializes a present record.
- `exists()` checks the authoritative job key.
- `findByStatus()` uses `SMEMBERS` on the status set, retrieves each corresponding job record, and skips missing records. It does not use `KEYS *`.
- `delete()` retrieves the existing job, removes the authoritative record and its status-index entry, and returns `false` when no record exists.

### 2.4 Separation from Queueing

- The persistence layer is responsible only for state storage.
- No queue, worker, retry, or dispatch behavior is implemented here.

### 2.5 Error Handling and Client Lifecycle

- Redis/Jedis failures are translated to `PersistenceException`.
- `RedisJobRepository` receives an already-created Jedis 8.0.1 `RedisClient` through constructor injection and uses it for Redis operations. The repository neither creates nor owns a Redis connection.
- `JedisPool` and connection pooling are not currently used.
- `RedisConfig` supports host, port, and an optional password. Development and integration tests use a Redis instance running locally at `localhost:6379` through Docker. JobStream does not create or manage the Redis server.

---

## 3. Revision and write concurrency

- New Jobs begin at revision zero. Legacy serialized Jobs without a revision are treated as revision zero.
- Repository, queue submission, scheduler, and DLQ Job record writes use revision checks and increment the revision.
- Queue submission and scheduling also update their related indexes in the same Lua operation as the Job record write.
- `JobRepository.save()` returns the exact persisted snapshot with its new revision. Queue submission returns its exact committed queued snapshot; no follow-up repository read is required.
- Scheduler writes compare the full current serialized Job in addition to revision-aware write protection, allowing conflict retry without losing newer priority, schedule, retry, or metadata fields.

## 4. Non-Functional Requirements

- **Durability:** Persisted jobs can survive application and Redis process restarts when the Redis instance is configured with appropriate persistence settings. Redis durability configuration has not been validated by this phase.
- **Remaining hardening:** Redis persistence settings and durability across Redis server failures are operational concerns and are not validated by the repository implementation.

---

## 5. Implemented Design (Phase 2 and Phase 8 concurrency update)

- **Storage Structure in Redis:** Redis Strings hold complete serialized job documents; Redis Sets provide secondary status indexes.
- **Client Library:** Jedis 8.0.1 `RedisClient`.
- **Testing:** `RedisJobRepositoryTest` runs against a real local Redis instance, not a mock. It covers saving and retrieving all job fields, absent lookup, `exists`, pending-status queries, moving IDs between status indexes on an updated save, and deleting both the job record and its index entry.

---

## 6. Dependencies

- Domain Model (Phase 1: `Job`, `JobId`, `JobStatus`)
- Serialization (Phase 2: `JobSerializer`)

---

## 7. Phase Ownership

- **Phase 2 (Serialization & Persistence):** Implements `JobRepository` and `RedisJobRepository`.
