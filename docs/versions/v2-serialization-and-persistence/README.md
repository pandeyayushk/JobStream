# Phase 2 - Serialization & Persistence

## 1. Objective

Establish the JSON serialization boundary and Redis persistence layer. Jobs can be serialized, stored, retrieved, and deserialized with their `JobId`, type, status, payload, timestamps, and metadata preserved.

## 2. Why This Phase Exists

Jobs cross process and persistence boundaries. This phase provides a portable JSON representation without polluting the domain model with framework annotations, and durable job storage indexed by `JobId`.

**Depends on:** Phase 1 (Domain Model: `Job`, `JobId`, `JobStatus`, `Payload`)

## 3. Implemented Scope

- `JobSerializer` and `SerializationException` in `io.github.pandeyayushk.jobstream.serialization`
- `JacksonJobSerializer`, using Jackson Databind 3.2.2 and a Jackson 3.x `ObjectMapper`
- A private `JobData` representation in `JacksonJobSerializer`; domain classes remain free of Jackson annotations and dependencies
- `JobRepository`, `PersistenceException`, and `RedisJobRepository`
- `RedisConfig` for host, port, and optional password
- Jedis 8.0.1 `RedisClient`, injected into `RedisJobRepository` by its caller
- Integration tests using a real Redis instance running locally through Docker

**Out of scope:** Queue abstractions, workers, retries, transactions, pipelines, Lua scripts, and connection pooling. These remain future phase or production-hardening concerns.

## 4. Development Redis

Development and integration tests expect Redis at `localhost:6379`, commonly started with Docker:

```powershell
docker run -d -p 6379:6379 --name jobstream-redis redis:7-alpine
```

This project does not create or manage the Redis server.

## 5. Implemented Design

### 5.1 Serialization

`JacksonJobSerializer` converts between `Job` and a private `JobData` record. Serialized jobs include `JobId`, type, status, payload, `createdAt`, `updatedAt`, and metadata. `Instant` values retain their precision. Jackson serialization/deserialization failures, including malformed JSON and invalid Job ID input, are translated to `SerializationException`.

### 5.2 Repository Contract

```java
void save(Job job);
Optional<Job> findById(JobId id);
boolean delete(JobId id);
List<Job> findByStatus(JobStatus status);
boolean exists(JobId id);
```

**Contract reconciliation note:** Earlier Phase 2 requirements described `delete()` as returning `void`. The implemented contract returns `boolean`; this documentation/contract mismatch should be reconciled later.

### 5.3 Redis Key Schema

```
jobstream:job:<uuid>
       |
       +--> JSON Job record

jobstream:status:<STATUS>
       |
       +--> Set<JobId>
```

The Job record is authoritative. Status sets are secondary indexes.

`save()` stores the serialized job and adds its ID to the current status set. If an existing job changed status, it first removes the ID from the previous status set. `findByStatus()` uses `SMEMBERS`, loads the corresponding job records, and skips missing records rather than using `KEYS *`. `delete()` removes both the job record and its status-index entry, returning `false` for an unknown ID.

### 5.4 Redis Client and Errors

`RedisJobRepository` receives an already-created Jedis `RedisClient` through constructor injection. It does not create its own client or connection, and the implementation does not use `JedisPool`. Jedis failures are translated to `PersistenceException`.

## 6. Tests

`RedisJobRepositoryTest` runs against real Redis and verifies:

- saving a job and retrieving the same fields with `findById`
- an empty result for an unknown Job ID
- `exists` for saved and unknown jobs
- saved pending jobs returned by `findByStatus`
- moving an ID between status indexes after saving an updated status
- deletion of both the job record and its status-index entry

Serializer tests also cover round-trip fields, timestamp precision, metadata, malformed JSON, and invalid Job IDs.

## 7. Deferred Production Hardening

`save()` currently performs multiple Redis operations. It is not an atomic transaction, so a status-index update is not crash-atomic. Concurrent read-modify-write status updates have not been hardened. These are deferred production-hardening concerns, not completed Phase 2 features.

## 8. Validation

```powershell
mvn clean test
git diff --check
```

## 9. Completion Criteria

- [x] Jackson serialization preserves implemented Job fields without domain Jackson dependencies.
- [x] `JobRepository` and `RedisJobRepository` support storage, lookup, status queries, existence checks, and deletion.
- [x] Redis uses authoritative job records with secondary status indexes.
- [x] Integration tests run against a real local Redis instance.

## 10. What This Phase Enables

Phase 3 may build queue behavior on top of persistent job records. Queue semantics are not part of this phase.
