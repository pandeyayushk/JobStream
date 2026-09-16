# Phase 2 — Serialization & Persistence

## 1. Objective

Establish the serialization boundary (converting domain entities `Job` and `Payload` to/from portable JSON) and the persistence layer (storing and retrieving jobs in Redis indexed by `JobId`). After this phase, jobs can be created, serialized, stored in Redis, retrieved, and deserialized with complete round-trip fidelity.

## 2. Why This Phase Exists

Jobs must cross process and persistence boundaries — created by one process and executed by another. This requires:
1. **Serialization Boundary:** Converting domain objects into a portable format (JSON) without polluting the domain model with framework annotations.
2. **Persistence Layer:** Providing durable storage indexed by `JobId` (`JobId -> Job record`) so jobs survive application restarts.

Establishing persistence before queueing provides a stable storage foundation before introducing queue dispatch mechanics.

**Depends on:** Phase 1 (Domain Model: `Job`, `JobId`, `JobStatus`, `Payload`)

## 3. Scope

**In scope:**
- `JobSerializer` interface in `io.github.pandeyayushk.jobstream.serialization`
- `JacksonJobSerializer` implementation handling records, `Instant`, and `Payload`
- Serialization round-trip unit tests
- `JobRepository` interface in `io.github.pandeyayushk.jobstream.persistence`
- `RedisJobRepository` implementation mapping `JobId -> Job record` (Redis Strings)
- `RedisConfig` configuration object for connection parameters
- Integration tests against a running Redis instance

**Out of scope:**
- Queue abstractions or list operations (Phase 3)
- Worker loops (Phase 4)
- Retry and DLQ handling (Phase 6)

## 4. Prerequisites

- Phase 1 complete (`Job`, `JobId`, `JobStatus`, `Payload` fully implemented and tested)
- Redis 7+ instance running locally or via Docker:
  ```powershell
  docker run -d -p 6379:6379 --name jobstream-redis redis:7-alpine
  ```

## 5. Read Before Starting

- `docs/requirements/serialization.md` — Serialization boundary requirements
- `docs/requirements/persistence.md` — Persistence and repository specifications
- `docs/architecture/project-structure.md` — Package hierarchy for `serialization`, `persistence`, `config`
- `docs/architecture/system-overview.md` — Architectural role of persistence
- `docs/decisions/ADR-002-redis-as-persistence-and-queue-backend.md` — Redis decision criteria

## 6. Concepts to Understand

- **Serialization Boundary:** Isolating domain entities from third-party serialization libraries (e.g. Jackson) using custom serializers or mixins.
- **Repository Pattern:** A collection-like abstraction over persistent storage hiding Redis commands behind domain operations (`save`, `findById`).
- **Redis String as Document Store:** Storing JSON strings under keys `jobstream:job:{uuid}`. Provides O(1) reads and writes without field-level impedance mismatches.
- **Jedis Connection Pooling (`JedisPool`):** Thread-safe connection management. Proper usage via try-with-resources:
  ```java
  try (Jedis jedis = jedisPool.getResource()) {
      jedis.set(key, value);
  }
  ```

## 7. Design Decisions

### 7.1 JSON Library Selection

**Decision:** Use `com.fasterxml.jackson.core:jackson-databind:2.17.0` with `jackson-datatype-jsr310` for ISO-8601 `Instant` support. Jackson is the industry standard for Java 21, providing robust support for records and unmodifiable collections.

### 7.2 Redis Key Schema for Jobs

**Decision:** Key pattern `jobstream:job:{uuid}`.
- Stores the entire serialized `Job` as a JSON string.
- The repository is the single source of truth for full job state.
- Queues in Phase 3 will store only `JobId` strings referencing these keys.

### 7.3 Status Query Strategy (`findByStatus`)

**What to decide:** How does `RedisJobRepository.findByStatus` locate jobs without full table scans (`KEYS *`)?

**Evaluation:**
- Scanning `KEYS jobstream:job:*` is an anti-pattern in Redis that blocks the server on large datasets.
- A secondary index (Redis Set: `jobstream:status:{STATUS}`) holding `JobId`s allows efficient, non-blocking lookups.

**Recommendation:** Maintain secondary sets `jobstream:status:{STATUS}` updated during `save()` and `delete()`.

**Record this decision in:** ADR-002 (update with validation results).

## 8. Requirements

### Functional Requirements

1. **`JobSerializer` Interface:**
   - `String serialize(Job job)`
   - `Job deserialize(String json)`
   - Round-trip fidelity: `deserialize(serialize(job)).equals(job)` must evaluate to `true`.
   - Explicit exception on error (`SerializationException`).

2. **`JacksonJobSerializer`:**
   - Configures Jackson `ObjectMapper` with JavaTimeModule.
   - Handles ISO-8601 formatting for `Instant`.
   - Serializes/deserializes `Payload` map values faithfully.

3. **`RedisConfig` (package `io.github.pandeyayushk.jobstream.config`):**
   - Host (default: `"localhost"`), Port (default: `6379`), Password (default: null), Database (default: 0).
   - Immutable configuration with builder.

4. **`JobRepository` Interface:**
   - `void save(Job job)`
   - `Optional<Job> findById(JobId id)`
   - `List<Job> findByStatus(JobStatus status)`
   - `void delete(JobId id)`
   - `boolean exists(JobId id)`

5. **`RedisJobRepository`:**
   - Implements `JobRepository` using `JedisPool`.
   - Redis key: `jobstream:job:{id.value()}`.
   - Status index: `jobstream:status:{status.name()}`.
   - Translates Jedis exceptions to `PersistenceException`.

### Non-Functional Requirements

- Zero connection leaks (guaranteed by try-with-resources on `JedisPool`).
- Thread-safe repository operations across concurrent threads.

## 9. File Change Manifest

### CREATE

```
src/main/java/io/github/pandeyayushk/jobstream/serialization/JobSerializer.java
src/main/java/io/github/pandeyayushk/jobstream/serialization/SerializationException.java
src/main/java/io/github/pandeyayushk/jobstream/serialization/JacksonJobSerializer.java
src/main/java/io/github/pandeyayushk/jobstream/persistence/JobRepository.java
src/main/java/io/github/pandeyayushk/jobstream/persistence/PersistenceException.java
src/main/java/io/github/pandeyayushk/jobstream/persistence/RedisJobRepository.java
src/main/java/io/github/pandeyayushk/jobstream/config/RedisConfig.java
src/test/java/io/github/pandeyayushk/jobstream/serialization/JacksonJobSerializerTest.java
src/test/java/io/github/pandeyayushk/jobstream/persistence/RedisJobRepositoryTest.java
src/test/java/io/github/pandeyayushk/jobstream/config/RedisConfigTest.java
```

### MODIFY

```
pom.xml  — Add jackson-databind, jackson-datatype-jsr310, jedis
```

### DELETE

None.

### DO NOT TOUCH

```
src/main/java/.../job/*       — Domain model from Phase 1
src/main/java/.../payload/*   — Domain payload from Phase 1
src/test/java/.../job/*       — Phase 1 tests
src/test/java/.../payload/*   — Phase 1 tests
src/main/java/.../Main.java   — Phase 0 entry point
```

## 10. Implementation Order

1. **`pom.xml`** — Add Jackson and Jedis dependencies.
2. **`JobSerializer.java`** & **`SerializationException.java`** — Serialization contract.
3. **`JacksonJobSerializer.java`** — Jackson mapper implementation.
4. **`JacksonJobSerializerTest.java`** — Unit tests for round-trip fidelity.
5. **`RedisConfig.java`** & **`RedisConfigTest.java`** — Configuration parameters.
6. **`JobRepository.java`** & **`PersistenceException.java`** — Storage contract.
7. **`RedisJobRepository.java`** — Redis storage implementation with status indexing.
8. **`RedisJobRepositoryTest.java`** — Integration tests against Redis.

## 11. Testing Requirements

### Unit Tests
- `JacksonJobSerializerTest`:
  - Serialize `Job` with string, integer, boolean payload entries.
  - Deserialize and assert exact equality with original `Job`.
  - Timestamps retain UTC precision.
  - Metadata map preserved.
  - Malformed JSON throws `SerializationException`.

### Integration Tests (Requires Redis)
- `RedisJobRepositoryTest`:
  - `save()` stores job; `findById()` retrieves identical job.
  - `exists()` returns `true` for saved job, `false` for unknown `JobId`.
  - `findByStatus(PENDING)` returns all saved pending jobs.
  - `save()` with updated status updates the status index cleanly.
  - `delete()` removes both job record and status index entry.

## 12. Failure and Edge Cases

- Redis offline on repository call → throws typed `PersistenceException` with cause.
- Corrupted JSON in Redis → `findById()` throws `SerializationException`.
- Saving null job → throws `NullPointerException`.
- Deleting non-existent `JobId` → completes silently without error.

## 13. Validation

```powershell
# Compile
mvn compile

# Run unit tests (no Redis required)
mvn test -Dtest="JacksonJobSerializerTest,RedisConfigTest"

# Run integration tests (Redis running)
mvn test -Dtest="RedisJobRepositoryTest"

# Manual Redis inspection
redis-cli KEYS "jobstream:job:*"
redis-cli GET "jobstream:job:<uuid>"
```

## 14. Completion Criteria

- [ ] `JobSerializer` and `JacksonJobSerializer` achieve 100% round-trip fidelity.
- [ ] `JobRepository` abstraction defined with no Redis leakage.
- [ ] `RedisJobRepository` supports CRUD and status querying via secondary indices.
- [ ] `RedisConfig` supports configurable host, port, and credentials.
- [ ] All unit and integration tests pass cleanly (`mvn test` exits 0).

## 15. Self-Review Checklist

- [ ] Does domain model (`job`, `payload`) remain free of Jackson annotations?
- [ ] Are all Jedis connections closed via try-with-resources?
- [ ] Does `findByStatus` avoid using the blocking `KEYS *` command?
- [ ] Are Phase 1 tests still passing?

## 16. Documentation Updates

### Requirements documentation
- Update `docs/requirements/serialization.md` and `docs/requirements/persistence.md` with final method signatures.

### Architecture documentation
- Create or update Redis key schema reference.

### ADRs
- **Update ADR-002:** Record findings on Jedis connection pooling and Redis String serialization.

### Final README Notes
- Persistence powered by Redis Strings with secondary status indexing.
- Jackson provides zero-domain-pollution JSON serialization.

## 17. Git Milestone

- **Branch:** `phase-2-serialization-persistence`
- **Base:** `master` (after Phase 1 merge)
- **Implementation milestone:** Serializer, repository, and Redis integration complete.
- **Testing milestone:** Unit and integration tests pass with 0 failures.
- **Review milestone:** ADR-002 updated, self-review complete.
- **Merge condition:** Clean Maven build and test pass on master.

## 18. What This Phase Enables

With persistence established:
- **Phase 3 (Queue System)** can implement reference-based queues (`Queue -> JobId`), relying on `JobRepository` as the single authoritative source of job state.
