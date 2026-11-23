# ADR-014: Register Named Queues in Redis

- **Status:** Accepted for Phase 9 implementation
- **Date:** 2026-11-23
- **Deciders:** JobStream maintainers

## Context

The HTTP API requires `GET /api/v1/queues`, but `JobQueue` accepts a queue name for each operation and does not enumerate queue names. Discovering names by `KEYS` is unsuitable for production request handling. Queue names can be created concurrently by normal submission, Worker requeue, scheduler release, and DLQ requeue.

## Decision

Add a Redis Set `jobstream:queues` and a `QueueRegistry` abstraction. Register a queue name with `SADD` in the same Lua script that inserts the first/current member into that queue. This includes atomic submission, priority enqueue and requeue paths, legacy List migration, scheduler poller release, and DLQ requeue. `SADD` makes concurrent registration idempotent. Queue names remain registered when empty; Phase 9 does not automatically unregister them.

Existing active priority queue keys and legacy active-queue List keys require an idempotent, cursor-based `SCAN` backfill during startup/deployment; explicitly exclude the DLQ List `jobstream:queue:dead-letter`. The scan registers matching names; it is not an atomic snapshot. Concurrent new queue writes are covered by atomic registration. Request paths read the Set and compute depths via `JobQueue.size`; they do not scan the keyspace.

## Alternatives considered

- **`KEYS` discovery:** simple but blocks Redis while scanning and is unsafe on request paths.
- **Repeated `SCAN` per API request:** avoids `KEYS`, but makes latency and completeness depend on a full keyspace traversal for every request.
- **Explicit registry Set:** selected; constant-size name reads and atomic registration on normal insertion paths, with a one-time compatibility backfill.

## Consequences

- Queue listing becomes a Redis-backed capability without changing the meaning of `JobQueue` size operations.
- Every insertion script and legacy migration path must register the name in the same atomic operation; missing one path creates an undiscoverable queue.
- The one-time SCAN backfill is eventually complete, can observe concurrent key changes, and is made restart-safe/idempotent. Historical names are retained even after queues become empty.
- Queue depths are read after listing and can change concurrently; the endpoint is an observational snapshot, not a transactionally consistent cluster-wide view.
