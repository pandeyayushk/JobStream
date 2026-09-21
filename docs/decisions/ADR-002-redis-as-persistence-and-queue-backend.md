# ADR-002: Use Redis as the Persistence and Queue Backend

- **Status:** Accepted for Phase 2 persistence; queue backend decision deferred
- **Date:** 2026-09-17
- **Deciders:** Engineering Architecture

---

## 1. Context

JobStream needs durable storage for complete job state indexed by `JobId`. Phase 2 implements that persistence responsibility with Redis. Queue dispatch, workers, retries, and other queue concerns are outside the implemented Phase 2 scope and must not be inferred from this decision.

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
- `save` is currently multiple Redis operations, not an atomic transaction. A crash may leave the status index inconsistent with the authoritative job record, and concurrent read-modify-write status changes are not yet hardened. These are deferred production-hardening concerns.

## 4. Deferred Queue Decision

Redis is not yet implemented as a queue backend. Redis Lists, Streams, queues, workers, retries, transactions, pipelines, and Lua scripts are not Phase 2 features. Their selection and correctness requirements remain for later phases.

## 5. Alternatives Considered

- **PostgreSQL:** Strong relational durability, but adds a different persistence system to the learning scope.
- **Redis Hashes:** Field-oriented storage, but Redis Strings containing serialized job documents better match the current repository boundary.
- **Lettuce:** An asynchronous client alternative; Jedis 8.0.1 `RedisClient` is the implemented client for Phase 2.
