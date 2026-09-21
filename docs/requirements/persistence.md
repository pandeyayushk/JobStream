# Persistence Requirements

## 1. Purpose

Defines the implemented contract and responsibilities for durable storage of job entities, ensuring that job state survives application restarts independently of future queueing mechanisms.

---

## 2. Functional Requirements

### 2.1 Repository Abstraction (`JobRepository`)

- All persistence operations are defined behind a technology-neutral interface:
  - `void save(Job job)`: Persists a new job or updates an existing job.
  - `Optional<Job> findById(JobId id)`: Retrieves the current state of a job by its unique identity.
  - `boolean delete(JobId id)`: Permanently deletes a job record and reports whether it existed.
  - `List<Job> findByStatus(JobStatus status)`: Queries jobs by lifecycle state.
  - `boolean exists(JobId id)`: Checks existence without full deserialization overhead.
- The interface does not leak Redis-specific commands, keys, or types to callers.
- **Contract reconciliation note:** Earlier requirements documented `delete()` as returning `void`; the current implemented interface returns `boolean`. This mismatch should be reconciled in a later contract review.

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

The Job record is authoritative. Status sets are secondary indexes maintained by the repository.

- `save()` serializes the job, stores its JSON under the job key, and adds its ID to the current status set. When an existing job has changed status, it removes the ID from the prior status set before adding it to the new one.
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

## 3. Non-Functional Requirements

- **Durability:** Persisted jobs can survive application and Redis process restarts when the Redis instance is configured with appropriate persistence settings. Redis durability configuration has not been validated by this phase.
- **Production hardening (deferred):** `save()` consists of multiple Redis operations rather than an atomic transaction. Status-index updates are therefore not crash-atomic, and concurrent read-modify-write status updates have not yet been hardened.

---

## 4. Implemented Design (Phase 2)

- **Storage Structure in Redis:** Redis Strings hold complete serialized job documents; Redis Sets provide secondary status indexes.
- **Client Library:** Jedis 8.0.1 `RedisClient`.
- **Testing:** `RedisJobRepositoryTest` runs against a real local Redis instance, not a mock. It covers saving and retrieving all job fields, absent lookup, `exists`, pending-status queries, moving IDs between status indexes on an updated save, and deleting both the job record and its index entry.

---

## 5. Dependencies

- Domain Model (Phase 1: `Job`, `JobId`, `JobStatus`)
- Serialization (Phase 2: `JobSerializer`)

---

## 6. Phase Ownership

- **Phase 2 (Serialization & Persistence):** Implements `JobRepository` and `RedisJobRepository`.
