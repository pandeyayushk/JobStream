# Phase 9 — Observability & HTTP API

**Status: Process-local job counters, the Jackson/API foundations, embedded health server, and Redis queue registry/backfill are implemented.** Redis-backed gauges and the remaining management API endpoints are still pending. This document separates current capabilities from remaining planned work and deferred integrations.

## Existing capabilities and constraints

Phase 8 provides revision-aware Job persistence and submission, numeric priority queues, durable scheduling, and a bounded scheduler poller. Worker retry backoff remains in-memory and is not connected to `RedisJobScheduler`; the CLI has no priority or delayed-submission flags; and scheduler-poller production bootstrap/lifecycle management is absent. These integrations are outside Phase 9.

Existing interfaces: `JobRepository` supports lookup/save/status queries; `QueueCoordinator.submit` returns the committed queued snapshot; `JobQueue` operates on a supplied queue name but cannot enumerate names; `DeadLetterQueue` supports paginated listing and requeue; `WorkerRegistry` provides Redis-backed registration and `listActiveWorkers`; `JobScheduler` and `SchedulerPoller` provide storage and bounded release without runtime wiring.

`RedisWorkerRegistry` stores worker ID, status, and `startedAt` in a Redis Hash. A separate heartbeat key has a TTL refreshed by heartbeat calls. Listing filters records by heartbeat-key existence. No last-heartbeat timestamp or processing/idle state is stored or exposed.

## HTTP framework decision

Use Java 21's `com.sun.net.httpserver.HttpServer` (`jdk.httpserver`) for the embedded operator API. It adds no Maven dependency and works with the existing Jackson 3.3.0 `ObjectMapper`. Use a bounded executor, explicit route dispatch, shared JSON/error helpers, request-size limits, and an `AutoCloseable` lifecycle. Integration tests use the JDK `HttpClient` against an ephemeral loopback port.

Javalin 7.2.4 is actively released and requires Java 17+, so it is Java 21 compatible. Jackson 2 (`com.fasterxml.jackson`) and Jackson 3 (`tools.jackson`) are distinct and not binary compatible, but Javalin 7.2.0+ provides an optional native `JavalinJackson3` mapper. With Javalin, configure that mapper explicitly and depend on the Jackson 3 artifacts; do not use `javalin-bundle` if its Jackson 2 defaults are unwanted. Javalin's core Jackson dependencies are optional, while Jetty runtime dependencies are required. Javalin therefore can use the existing mapper generation without a custom adapter, at the cost of Jetty and the framework. Its routing, validation, and exception mapping are useful; for this small fixed route set, the JDK server still has the lower footprint and no extra dependency. Revisit if API needs grow.

Version support: Phase 9 Step 1 selected Jackson 3.3.0, published on the active 3.3 release branch. Jackson 3.1 remains the LTS branch. Javalin 7.2.4's Jackson 3 mapper is optional; any future Javalin selection must explicitly configure it and verify Maven resolution against the project's Jackson pin.

## Metrics architecture and semantics

Use process-local thread-safe `LongAdder` counters behind `JobMetrics`. `ProcessLocalJobMetrics` is injected into the submission coordinator, Worker lifecycle, and DLQ mover through process composition; values reset at restart and are per process, not cluster-wide or durable. Its immutable snapshot exposes all five counters in deterministic order. Instrument only after the durable operation succeeds:

| Counter | Increment point | Meaning |
|---|---|---|
| `jobs_submitted_total` | `QueueCoordinator.submit` returns committed snapshot | Successful immediate submission; conflicts/failures do not count. |
| `jobs_completed_total` | Worker successfully saves `COMPLETED` | Successfully completed Jobs. |
| `jobs_failed_total` | Worker successfully saves `FAILED` after execution attempt | Failed attempts, including retryable ones; not terminally failed unique Jobs. |
| `jobs_retried_total` | Worker successfully saves `RETRYING` | Accepted retry decisions; queue requeue does not increment again. |
| `jobs_moved_to_dlq_total` | DLQ move succeeds | Successful moves; listing and requeueing out do not count. |

A retry intentionally contributes one failed attempt and one retry decision, not another submission. Metrics are best effort: process crash between job persistence and local increment can omit an event. These are not audit or billing records. Recording must be non-throwing and cannot fail job processing. Redis-backed counters would aggregate instances but add hot-path Redis coupling and still diverge from Job writes unless lifecycle scripts were expanded; hybrid counters add unnecessary duplicate semantics.

Counter infrastructure and lifecycle instrumentation are implemented. Gauges are planned to be read on request: per-queue/total depth via `JobQueue.size` for registered names, active registration count via `WorkerRegistry.listActiveWorkers`. Processing/idle gauges are unavailable. Execution and queue-wait latency are out of scope: no per-attempt timestamps or `queuedAt` exist, and `updatedAt` is overwritten at transitions. Do not infer latency from `createdAt` or `updatedAt`.

`GET /metrics` returns JSON counters and gauges. Gauge reads need Redis; on failure return 503 rather than a misleading partial snapshot. Counter recording does not need Redis.

## Queue discovery

`QueueRegistry` is implemented in the `queue` package by `RedisQueueRegistry`, backed by Redis Set `jobstream:queues`. Its public operations register a name idempotently and list names in lexical order. Queue names are retained after the queue empties; there is no automatic unregister operation.

All active queue insertions add the name with `SADD` inside the same Lua script that inserts the member: atomic job submission, scheduler poller release, direct enqueue/requeue, legacy List migration, and DLQ requeue into an active queue. The dead-letter List is not registered as an active queue.

`backfillExistingQueues()` scans `jobstream:priority-queue:*` keys of type ZSET and `jobstream:queue:*` keys of type List, explicitly excluding `jobstream:queue:dead-letter`. It uses cursor-based `SCAN`, never `KEYS`, and only registers keys matching the expected prefix and Redis type. Every invocation starts at cursor zero and completes both scans before returning. If interrupted, successful SADD operations remain and a later invocation safely repeats the scan; there is no premature completion marker or global lock. Multiple instances can backfill concurrently because SADD is idempotent. SCAN is not a snapshot: concurrent new insertions are registered by their insertion scripts, including queues created after their matching scan page has passed. Only existing Redis queue keys can be backfilled; a legacy queue whose data has already disappeared has no key from which to recover its name.

Backfill is an explicit registry operation. No production bootstrap currently invokes it automatically; callers that need names from pre-registry data must run it before relying on listing for those queues.

## Worker inspection

`GET /api/v1/workers` returns worker ID, stored `WorkerStatus`, and `startedAt` from `listActiveWorkers`. “Active” means its heartbeat key existed at lookup; expiry can race immediately after the read. The API cannot claim a worker is executing or healthy and cannot return last heartbeat time or processing/idle state without extending the registry and heartbeat writes.

## Health

- `GET /health/live`: 200 `{"status":"UP"}` if the HTTP process can respond; no Redis call.
- `GET /health/ready`: Redis `PING`; 200 `{"status":"UP","dependencies":{"redis":"UP"}}`; 503 with Redis DOWN on failure. Catch Redis errors so the server remains alive.
- No `/health` alias; liveness and readiness are intentionally distinct.

The JDK `HttpServer` lifecycle is implemented with an explicitly started `AutoCloseable` server and a bounded executor. It currently exposes only these two health routes; the other API contracts below remain planned.

## API contract

Success bodies are JSON. Errors use `{"error":{"code":"...","message":"..."}}`; never expose stack traces, internal exception text, Redis addresses, or credentials.

| Endpoint | Request | Response/status | Rules and dependencies |
|---|---|---|---|
| `GET /health/live` | none | 200 | Process only; Redis independent. |
| `GET /health/ready` | none | 200/503 | Redis PING. |
| `GET /metrics` | none | 200/503 | Process counters plus Redis gauges; no partial success on gauge failure. |
| `POST /api/v1/jobs` | `{ "type": string, "payload": object, "queue": string, "priority": integer? }` | 201 committed queued Job and `Location` | Required nonblank type/queue and valid object payload; priority defaults 0 and fits Java int. Immediate submission only; no delay/schedule field. Uses `QueueCoordinator.submit`. |
| `GET /api/v1/jobs/{id}` | UUID | 200 Job / 404 | Invalid UUID 400; `JobRepository`. |
| `GET /api/v1/queues` | none | 200 `{"queues":[{"name":"...","depth":0}]}` | Pending handler. Uses implemented `QueueRegistry` and `JobQueue.size`; lexical name order. |
| `GET /api/v1/workers` | none | 200 `{"workers":[{"id":"...","status":"RUNNING","startedAt":"..."}]}` | `WorkerRegistry.listActiveWorkers`; heartbeat presence only, no processing state. |
| `GET /api/v1/dlq?offset=0&limit=50` | offset >= 0; limit 1..100 | 200 paginated `jobs` | Uses `DeadLetterQueue.listDeadJobs`; invalid pagination 400. |
| `POST /api/v1/dlq/{id}/requeue` | `{ "queue": string }` | 200 persisted queued Job | Uses `DeadLetterQueue.requeue`; malformed ID/blank queue 400, missing 404, state conflict 409 when distinguishable. |

Common errors: malformed JSON, wrong content type, invalid field, UUID or pagination 400; unknown route/resource 404; known path wrong method 405; revision/state conflict 409; Redis unavailable 503; generic sanitized 500. Request bodies are limited to 1 MiB; larger bodies return 413. The server uses a bounded executor and clean start/stop lifecycle.

Cancellation is excluded. `JobStatus` has no cancellation state, `JobQueue` cannot remove by ID, and Worker dequeue races with cancellation; CAS alone cannot coordinate these operations. Do not expose a fake cancellation endpoint.

## Security boundary

Bind to `127.0.0.1` by default; non-loopback bind requires explicit configuration. V1 has no built-in authentication. Remote use requires trusted private networking and an authenticated reverse proxy/firewall; the API is not safe for direct public exposure. Keep Redis credentials protected and never echo them. Require JSON content type, validate paths and inputs before Redis calls, constrain JSON parsing, cap body size at 1 MiB, and sanitize request/error logs.

`RedisConfig` has an optional password field. The HTTP readiness probe passes that password explicitly to Jedis and does not log or return it. Existing `CliContext` wiring still accepts only host/port and does not use the optional password.

`ApiConfig` defaults to `127.0.0.1:8080`, a 1 MiB request-body limit, 8 executor threads, and queue capacity 128. It validates address, port (including port 0 for ephemeral test binding), positive limits, and executor bounds. It contains no Redis credentials and constructing it does not start a server.

## Phase boundary

Implemented infrastructure: Jackson 3.3.0 and API configuration/error foundation; JDK HTTP server lifecycle and live/ready health routes; Redis queue registry with atomic insertion registration and legacy-key backfill; process-local counters instrumented at successful submission, completion, failed-attempt, retry-persistence, and DLQ-move boundaries. Pending Phase 9 work: Redis gauge endpoint and job/queue/worker/DLQ handlers. Deferred: Worker retry-to-Redis scheduler integration; CLI priority/delay flags; production poller bootstrap; cancellation; latency metrics; Prometheus exposition; durable cluster-wide counters; worker heartbeat timestamps/processing counts; built-in auth/public exposure.

## Implementation sequence

1. **Complete:** Upgrade and pin supported Jackson 3.3.0; verify compatibility with existing serialized Job records and tests.
2. **Complete:** Add immutable `ApiConfig` defaults and validation, stable API error codes/statuses, safe JSON error envelope, and shared Jackson codec.
3. **Complete:** Implement `QueueRegistry` Redis Set, atomic registration in every queue insertion Lua path, and idempotent cursor-based SCAN backfill.
4. **Complete:** Add process-local counters and instrument successful submit, completion, failed attempt, retry decision, and DLQ move boundaries. Enforce non-throwing metric recording.
5. **Complete:** Add JDK HTTP server, bounded executor, lifecycle, shared Jackson 3 JSON handling, safe errors, and live/ready health probes. Management routes remain in Step 6.
6. Add job submit/read, queue/worker inspection, and DLQ list/requeue handlers.
7. Add JSON metrics with Redis-backed gauge reads.
8. Add management endpoint tests for counters, registry, real-Redis behavior, and HTTP integration, including errors, Redis outage, limits, and shutdown. Counter unit and lifecycle tests are already part of Step 4.
9. Review dependency tree, security boundaries, documentation, and Phase 8 deferred scope.

## References

- [JDK 21 `jdk.httpserver`](https://docs.oracle.com/en/java/javase/21/docs/api/jdk.httpserver/module-summary.html), [HttpServer API](https://docs.oracle.com/en/java/javase/21/docs/api/jdk.httpserver/com/sun/net/httpserver/HttpServer.html)
- [Javalin documentation](https://javalin.io/documentation), [release history](https://github.com/javalin/javalin/releases)
- [Jackson release support policy](https://github.com/FasterXML/jackson/wiki/Jackson-Releases)
- [Observability requirements](../../requirements/observability.md), [HTTP API requirements](../../requirements/http-api.md)
- [ADR-012](../../decisions/ADR-012-jdk-http-server.md), [ADR-013](../../decisions/ADR-013-process-local-metrics.md), [ADR-014](../../decisions/ADR-014-redis-queue-registry.md)
