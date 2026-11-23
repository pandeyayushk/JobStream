# HTTP API Requirements

## Purpose and status

Specify the Phase 9 operator and management API. The specification is finalized. Jackson 3.3.0, the JDK `HttpServer` lifecycle, and live/ready health routes are implemented; management routes remain pending. The first version uses JDK `com.sun.net.httpserver.HttpServer` and Jackson 3 serialization, with no HTTP framework dependency. See [ADR-012](../decisions/ADR-012-jdk-http-server.md).

## Common contract

- All successful API responses use JSON and `Content-Type: application/json`.
- Errors use `{"error":{"code":"...","message":"..."}}`. Messages are sanitized; no stack traces, Redis addresses, credentials, or raw exception text.
- Invalid request syntax/content, invalid UUID, invalid field, unsupported content type, and invalid pagination return 400.
- Missing resources return 404; distinguishable state/revision conflicts return 409; unavailable Redis returns 503; unexpected internal failures return generic 500.
- Unknown paths return 404. Known paths with unsupported methods return 405.
- Request bodies are limited to 1 MiB; requests larger than the limit return 413. Validate before repository or Redis calls.
- Bind to loopback (`127.0.0.1`) by default. Non-loopback binding requires explicit configuration. There is no built-in authentication in v1; remote deployment requires a trusted network and authenticated reverse proxy/firewall. The API is not for direct public exposure.
- `RedisConfig` contains an optional password. The API readiness probe explicitly passes it to Jedis; the existing `CliContext` still constructs a credential-free Redis connection from host/port.
- Use a bounded executor and explicit start/stop lifecycle. Redis outages must not terminate the HTTP process.

`ApiConfig` defaults to `127.0.0.1:8080`, a 1 MiB request-body limit, 8 executor threads, and an executor queue capacity of 128. Bind address, port, body limit, and executor bounds are configurable and validated. No enable/disable flag is defined before server bootstrap exists; constructing configuration does not start a server.

## Endpoints

| Method and path | Request | Response and status | Backing capability and rules |
|---|---|---|---|
| `GET /health/live` | None | 200 `{"status":"UP"}` | Process response only; must not contact Redis. |
| `GET /health/ready` | None | 200 `{"status":"UP","dependencies":{"redis":"UP"}}`; 503 with DOWN when unavailable | Redis `PING`; catch failures without stopping the server. No `/health` alias. |
| `GET /metrics` | None | 200 JSON counters and gauges; 503 on Redis gauge failure | Process-local counters plus queue-depth and active-worker reads. Never report partial data as complete. |
| `POST /api/v1/jobs` | `{ "type": string, "payload": object, "queue": string, "priority": integer? }` | 201 committed queued Job snapshot and `Location` header | Uses `QueueCoordinator.submit`. Type/queue required and nonblank; payload must be valid object; priority defaults to 0 and fits Java `int`. No delay/scheduled field. |
| `GET /api/v1/jobs/{id}` | UUID path segment | 200 Job or 404 | Uses `JobRepository.findById`; malformed UUID is 400. |
| `GET /api/v1/queues` | None | 200 `{"queues":[{"name":"...","depth":0}]}` sorted by name | Pending HTTP handler; uses implemented `QueueRegistry` and computes depth with `JobQueue.size`. Redis failure is 503. |
| `GET /api/v1/workers` | None | 200 `{"workers":[{"id":"...","status":"RUNNING","startedAt":"..."}]}` | Uses `WorkerRegistry.listActiveWorkers`. Active means heartbeat key existed at lookup, not execution or health. No heartbeat timestamp or processing state. |
| `GET /api/v1/dlq?offset=0&limit=50` | Optional offset >= 0 and limit 1..100 | 200 `{"jobs":[...],"offset":0,"limit":50}` | Uses `DeadLetterQueue.listDeadJobs`. Invalid pagination is 400. |
| `POST /api/v1/dlq/{id}/requeue` | `{ "queue": "..." }` | 200 persisted queued Job snapshot | Uses `DeadLetterQueue.requeue`; invalid ID/blank queue 400; missing 404; state conflict 409 when distinguishable. |

Redis unavailability for repository, registry, queue, or DLQ calls returns 503. Malformed stored data returns a sanitized error. No `/health` alias is retained because liveness and readiness have different dependencies.

## Queue discovery

`JobQueue` supports named queue operations but not enumeration. `RedisQueueRegistry` implements `QueueRegistry` with Redis Set `jobstream:queues`. Every operation that inserts an active queue member registers its name via `SADD` in the same Lua operation. Registration is idempotent and names remain registered when depth reaches zero. `backfillExistingQueues()` cursor-scans the priority-queue prefix for ZSET keys and legacy active-queue prefix for List keys, excluding `jobstream:queue:dead-letter`. Each run starts from cursor zero; partial SADDs survive interruption and a later run safely repeats. New writes racing with SCAN register atomically. The explicit backfill is not yet called by a production bootstrap; invoke it before using registry listing to discover pre-existing queues. Do not use `KEYS`; do not remove names automatically in Phase 9.

## Cancellation

There is no cancellation endpoint. The lifecycle has no cancellation state, `JobQueue` has no remove-by-ID operation, and dequeue races with cancellation. Safe cancellation requires lifecycle and acquisition coordination beyond this API increment.

## Explicit exclusions

- No delayed or recurring submission endpoint. Worker retry backoff remains in-memory; durable retry integration and scheduler poller production bootstrap are deferred Phase 8 work.
- No CLI priority/delay flags.
- No purge/delete DLQ endpoint, queue mutation endpoint, or fake cancellation operation.
- No built-in authentication, public exposure, Prometheus format, or latency claims.
