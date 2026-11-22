# ADR-002: Use Redis as the Persistence and Queue Backend

- **Status:** Accepted; Phase 2 persistence and Phase 3 queue backend
- **Date:** 2026-09-17
- **Deciders:** Engineering Architecture

---

## 1. Context

JobStream uses Redis for durable job storage and, as of Phase 3, FIFO queue dispatch. Workers, retries, and other later queue concerns remain outside this decision's implemented scope.

## 2. Decision

Use Redis for Phase 2 job persistence with Jedis 8.0.1 `RedisClient`. `RedisJobRepository` receives an already-created client through constructor injection; it does not create or own the client, and `JedisPool` is not used.

Jobs are serialized as JSON by the Jackson Databind 3.2.2 serializer and stored using this schema:

```
jobstream:job:<uuid>
       |
       +--> JSON Job record

jobstream:status:<STATUS>
       |
       +--> Set<JobId>
```

The Job record is authoritative. The status sets are secondary indexes. `findByStatus` uses `SMEMBERS` and loads the referenced records, skipping missing records rather than using `KEYS *`.

`save` maintains the status index by removing a job ID from its previous status set when its stored status changes, then adding it to the current set. `delete` removes the authoritative record and its status index entry.

Redis/Jedis errors at the repository boundary are translated to `PersistenceException`. Development and integration tests use a real Redis server at `localhost:6379` run through Docker; the application does not provision or manage that server.

## 3. Consequences

- Full job state is available through a simple key lookup, while status queries avoid a full key scan.
- The pure domain model remains independent of Redis and Jackson.
- Redis persistence depends on the operational Redis configuration for durability across Redis restarts; Phase 2 does not validate AOF/RDB durability settings.
- Historical note: the original persistence implementation used multiple Redis operations for `save`. Phase 8 replaced that write protocol with revision-checked Lua compare-and-set updates that atomically write the authoritative Job record and status indexes; see [ADR-011](ADR-011-durable-job-scheduling.md).

## 4. Queue Backend (Phase 3)

This section records the original Phase 3 FIFO implementation. The active queue representation was upgraded in Phase 8; see [ADR-010](ADR-010-numeric-priority-queue.md). The legacy List key below is now used only as a migration source, while the DLQ retains its separate List.

Historical Phase 3 implementation: `RedisJobQueue` stored only `JobId` strings in Redis Lists at `jobstream:queue:<queueName>`, using `LPUSH` and `RPOP`/`BRPOP` for FIFO. In Phase 8, that List became a legacy migration source; active dispatch uses the priority Sorted Set described in [ADR-010](ADR-010-numeric-priority-queue.md). Atomic submission is now revision-aware Lua, as described in [ADR-005](ADR-005-atomic-redis-job-submission-with-multi-exec.md) and ADR-010.

## 5. Alternatives Considered

- **PostgreSQL:** Strong relational durability, but adds a different persistence system to the learning scope.
- **Redis Hashes:** Field-oriented storage, but Redis Strings containing serialized job documents better match the current repository boundary.
- **Lettuce:** An asynchronous client alternative; Jedis 8.0.1 `RedisClient` is the implemented client for Phase 2.
