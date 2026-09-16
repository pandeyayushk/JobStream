# Persistence Requirements

## 1. Purpose

Defines the contract and responsibilities for durable storage of job entities, ensuring that job state survives process restarts and crashes independently of queueing mechanisms.

---

## 2. Functional Requirements

### 2.1 Repository Abstraction (`JobRepository`)
- All persistence operations must be defined behind a technology-neutral interface:
  - `void save(Job job)`: Persists a new job or updates an existing job.
  - `Optional<Job> findById(JobId id)`: Retrieves the current state of a job by its unique identity.
  - `List<Job> findByStatus(JobStatus status)`: Queries jobs by lifecycle state.
  - `void delete(JobId id)`: Permanently deletes a job record.
  - `boolean exists(JobId id)`: Checks existence without full deserialization overhead.
- The interface must never leak Redis-specific commands, keys, or types to the caller.

### 2.2 Storage Model: `JobId -> Job Record`
- The repository stores the complete authoritative state of the job.
- Each job is indexed uniquely by its `JobId`.
- **Key Pattern:** `jobstream:job:{uuid}`.
- Value stored: Serialized JSON representation produced by `JobSerializer`.

### 2.3 Separation from Queueing
- The persistence layer is strictly responsible for **state storage**, not job dispatching or worker coordination.
- Storing a job in `JobRepository` does not automatically make it visible to workers until its `JobId` is submitted to a `JobQueue`.

### 2.4 Error Handling & Connection Lifecycle
- Transient Redis connection errors must be caught and translated to typed repository exceptions (`PersistenceException`).
- Connection pooling must be managed safely using try-with-resources to prevent connection leaks under load.

---

## 3. Non-Functional Requirements

- **Durability:** Persisted jobs must survive application and Redis process restarts (assuming standard Redis persistence configuration: AOF/RDB).
- **Concurrency:** Repository methods must be safe for concurrent execution by multiple threads and processes.

---

## 4. Design Decisions (Phase 2 Ownership)

- **Storage Structure in Redis:** Redis String (JSON) vs Redis Hash (field-per-attribute). Redis String is recommended for simplicity and atomic read/write of full job objects.
- **Client Library:** Jedis (synchronous, thread-safe with connection pool) vs Lettuce (async/reactive). Jedis is recommended for initial implementation.

---

## 5. Dependencies

- Domain Model (Phase 1: `Job`, `JobId`, `JobStatus`)
- Serialization (Phase 2: `JobSerializer`)

---

## 6. Phase Ownership

- **Phase 2 (Serialization & Persistence):** Implements `JobRepository` and `RedisJobRepository`.
