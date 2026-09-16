# ADR-002: Use Redis as the Persistence and Queue Backend

- **Status:** Proposed
- **Date:** 2026-09-17
- **Deciders:** Engineering Architecture

---

## 1. Architectural Intent vs. Implementation Decision

A clear distinction must be maintained between **architectural intent** and an **accepted implementation decision**:

- **Architectural Intent:** JobStream is designed conceptually around Redis to explore whether a single, in-memory, high-performance data store can fulfill both persistent record storage and queue dispatch duties without introducing secondary brokers (e.g. RabbitMQ or Kafka).
- **Implementation Decision (Deferred):** The formal acceptance of Redis as the production backend remains **PROPOSED**. It will only be accepted in Phase 2 (Persistence) and Phase 3 (Queue) after validating performance, durability guarantees (AOF/RDB), client library behavior (Jedis connection pooling vs Lettuce async), and transactional atomicity.

---

## 2. Context & Problem Statement

JobStream requires two fundamental storage capabilities:
1. **Durable Job State Storage (Repository):** A key-value or document store where full job entities are indexed by `JobId` (`JobId -> Job record`).
2. **Queueing & Distribution Mechanism (Queue):** A FIFO ordering queue supporting atomic, blocking dequeue (`Queue -> JobId`) across multiple concurrent workers.

The engineering challenge is determining whether Redis can reliably fulfill both roles simultaneously while meeting reliability and distributed safety constraints.

---

## 3. Proposed Responsibilities for Redis

Under this proposed architecture, Redis will be responsible for:
1. **Job Record Persistence (`jobstream:job:{id}`):** Storing serialized JSON job representations.
2. **FIFO Active Queues (`jobstream:queue:{name}`):** Redis Lists (`LPUSH` / `BRPOP`) holding `JobId` references.
3. **Worker Registry & Liveness (`jobstream:worker:{id}`):** Tracking active worker instances with key expiration (TTL) for heartbeats.
4. **Dead-Letter Queue (`jobstream:queue:dead-letter`):** Holding `JobId`s of jobs that exhausted all retry attempts.
5. **Scheduled & Priority Queues (`jobstream:schedule`):** Redis Sorted Sets (`ZSET`) where score is execution timestamp or priority level.

---

## 4. Benefits

- **Unified Infrastructure:** Operators manage a single external infrastructure component instead of separate database and message broker technologies.
- **Native Queue Primitives:** Redis lists natively support atomic, blocking pops (`BRPOP`), eliminating busy-wait polling loops and external synchronization.
- **Sorted Sets for Scheduling & Priority:** `ZSET` commands (`ZADD`, `ZRANGEBYSCORE`) naturally solve time-delayed scheduling without custom interval trees.
- **High Throughput & In-Memory Speed:** Sub-millisecond latency for queue operations and job state queries.
- **Atomic Operations:** Single commands (`LPUSH`, `RPOP`, `HSET`) and transactional blocks (`MULTI`/`EXEC` or Lua scripts) provide atomic boundaries.

---

## 5. Trade-offs & Operational Risks

1. **In-Memory Volatility & Durability:**
   - *Risk:* By default, Redis is an in-memory store. An abrupt server crash or power failure could result in lost jobs.
   - *Mitigation to Validate:* Persistence requires explicit Redis configuration using Append-Only File (`AOF`) with `fsync everysec` (or `always`) alongside RDB snapshots.
2. **Memory Constraints:**
   - *Risk:* Unlike disk-backed databases (e.g. PostgreSQL), Redis dataset size is strictly bounded by physical RAM.
   - *Mitigation:* Storing only `JobId` in queues and pruning/archiving completed and dead jobs periodically.
3. **Single Point of Failure (SPOF):**
   - *Risk:* A single standalone Redis instance represents a single point of failure for both queueing and persistence.
   - *Mitigation:* In production, Redis Sentinel or Redis Cluster is required (deferred to Phase 10 / post-MVP).
4. **Queue/Persistence Consistency:**
   - *Risk:* Dual writes (saving a job in the repository and pushing its ID to a queue) can fail partially if network disconnects mid-flight.

---

## 6. Validation Criteria Required Before Acceptance

This ADR will transition from `Proposed` to `Accepted` in Phase 2 / Phase 3 upon satisfying the following validation criteria:
- [ ] **Client Library Validation:** Benchmark Jedis connection pooling against thread concurrency in Phase 2.
- [ ] **Serialization & Round-Trip Fidelity:** Verify that complex job payloads serialize to JSON and restore from Redis strings without data loss.
- [ ] **Dual-Write Atomicity:** Evaluate and test whether Redis `MULTI`/`EXEC` transactions or Lua scripts reliably prevent orphaned jobs during enqueue.
- [ ] **AOF Durability Check:** Confirm that simulated process restarts preserve stored jobs with Redis AOF enabled.
- [ ] **Blocking Dequeue Concurrency:** Confirm that multiple concurrent workers executing `BRPOP` never receive duplicate `JobId`s.

---

## 7. Alternatives Considered

- **PostgreSQL + RabbitMQ:**
  - *Pros:* Rock-solid relational ACID durability, mature AMQP queue semantics.
  - *Cons:* Operational complexity of managing two distinct services; high operational overhead for a learning-oriented project.
- **PostgreSQL SKIP LOCKED (Single RDBMS for both):**
  - *Pros:* True relational ACID transactions; enqueue and job creation occur in a single database transaction.
  - *Cons:* Database polling overhead; heavy write contention on queue tables under high throughput.
- **Apache Kafka:**
  - *Pros:* High throughput event streaming.
  - *Cons:* Kafka is an event log, not a worker job queue (lacks fine-grained job acknowledgement, individual retries, and job-level claiming). Overkill for this project.
