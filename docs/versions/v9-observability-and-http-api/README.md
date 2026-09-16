# Phase 9 — Observability & HTTP API

## 1. Objective

Provide production observability (counters, gauges, throughput statistics) and an embedded lightweight RESTful HTTP API allowing external systems, dashboards, and automated tools to manage jobs, inspect queues, check worker health, and retrieve metrics.

## 2. Why This Phase Exists

While Phase 7 delivered an interactive CLI for human operators, production architectures require:
- Programmatic machine-to-machine HTTP interfaces (e.g., submitting jobs from Python, Go, Node.js, or frontend services).
- System health endpoints (`/health`, `/metrics`) for uptime probes (Kubernetes liveness/readiness, Prometheus monitoring).
- Aggregated real-time metrics (throughput, error rates, queue latency, active workers) to understand system performance and detect bottlenecks.

**Depends on:**
- Phase 1–8 (all core domain, queue, worker, retry, scheduler components)

## 3. Scope

**In scope:**
- Embedded lightweight HTTP server (evaluating `Javalin` vs `Sun HttpServer`)
- Metrics subsystem in `io.github.pandeyayushk.jobstream.metrics`:
  - `JobMetrics` collecting counters (submitted, completed, failed, retried, dead)
  - Queue depth and latency gauges
- REST endpoints:
  - `GET /health` — Liveness & readiness check (Redis connectivity)
  - `GET /metrics` — JSON / Prometheus metrics summary
  - `POST /api/v1/jobs` — Submit a job
  - `GET /api/v1/jobs/{id}` — Query job details
  - `GET /api/v1/queues` — List queues and current sizes
  - `GET /api/v1/workers` — List active registered workers and heartbeats
  - `GET /api/v1/dlq` — Query dead-letter jobs
  - `POST /api/v1/dlq/{id}/requeue` — Requeue dead job
- Structured JSON error responses (`{ "error": "...", "code": 404 }`)

**Out of scope:**
- Full OAuth2 / JWT authentication server (deferred or pluggable reverse-proxy concern)
- Container packaging and Docker Compose (Phase 10)

## 4. Prerequisites

- Phase 8 complete (priority and scheduling working)
- Redis instance running
- Understanding of REST principles and HTTP status codes (200, 201, 400, 404, 500)

## 5. Read Before Starting

- `docs/requirements/observability.md` — Metrics and observability specifications
- `docs/requirements/http-api.md` — HTTP API specifications
- `docs/architecture/project-structure.md` — Packages `metrics` and `api`
- `docs/architecture/system-overview.md` — API placement in system architecture

## 6. Concepts to Understand

- **Embedded HTTP Servers:** Running an HTTP daemon inside the Java process without needing an external servlet container like Tomcat.
  - Recommended: [Javalin](https://javalin.io/) (lightweight, micro-framework built on Jetty, excellent Java 21 support).
- **Meters, Counters, and Gauges:**
  - Counter: Monotonically increasing number (e.g. `jobs_completed_total`).
  - Gauge: Instantaneous reading (e.g. `queue_depth`, `active_workers`).
- **RESTful Resource Modeling:** Nouns for collections (`/jobs`, `/workers`), proper verb usage (POST to create, GET to retrieve).
- **Health Check Probes:** Distinguishing liveness (is process running?) from readiness (can process reach Redis?).

## 7. Design Decisions

### 7.1 HTTP Framework Selection

**What to decide:** Which library to embed for HTTP API?

**Options:**
- JDK built-in `com.sun.net.httpserver`: Zero dependencies, but primitive routing, no native JSON handling, verbose boilerplate.
- Spring Boot: Full enterprise stack, but massive footprint, heavy reflection, and violates learning/lightweight philosophy.
- Javalin: Lightweight (~5MB), fluent routing, native Jackson integration, fast startup, simple to test.

**Recommendation:** Use `io.javalin:javalin:6.1.3` with Jackson.

**Record this decision in:** ADR-011 (to be created during this phase).

### 7.2 Metrics Aggregation Strategy

**What to decide:** In-memory counters vs Redis-backed metrics?

**Recommendation:**
- Redis counters (`INCR jobstream:metrics:completed`) for distributed aggregate statistics across all nodes.
- Local in-memory meters for worker-level processing rates.
- Expose combined statistics at `/metrics`.

**Record this decision in:** ADR-011.

## 8. Requirements

### Functional Requirements

1. **`JobMetrics` Subsystem:**
   - Methods: `recordSubmission()`, `recordCompletion(Duration duration)`, `recordFailure()`, `recordRetry()`, `recordDeadLetter()`.
   - Thread-safe tracking backed by atomic Redis counters or concurrent aggregators.

2. **HTTP Server Lifecycle (`JobStreamApiServer`):**
   - Configurable port (default: 8080).
   - Methods: `start()`, `stop()`.
   - Clean shutdown hook integration.

3. **REST Endpoints:**
   - `GET /health`: Returns HTTP 200 `{"status": "UP", "redis": "CONNECTED"}` or HTTP 503 `{"status": "DOWN"}`.
   - `GET /metrics`: Returns JSON object with totals and current queue depths.
   - `POST /api/v1/jobs`: Body `{"type": "...", "payload": {...}, "priority": 1, "queue": "default"}` → Returns HTTP 201 with created `Job`.
   - `GET /api/v1/jobs/{id}`: Returns HTTP 200 with `Job` or HTTP 404 if missing.
   - `GET /api/v1/queues`: Returns array of queues with item count.
   - `GET /api/v1/workers`: Returns list of workers from `WorkerRegistry`.
   - `GET /api/v1/dlq`: Returns dead-letter jobs.
   - `POST /api/v1/dlq/{id}/requeue`: Requeues job, returns HTTP 200.

4. **Error Handling:**
   - Standardized error payload: `{"error": "message", "status": 4xx}` with proper HTTP response code.

### Non-Functional Requirements

- Low latency (< 10ms for health check and metrics).
- Thread-safe request handling via Javalin thread pool.

## 9. File Change Manifest

### CREATE

```
src/main/java/io/github/pandeyayushk/jobstream/metrics/JobMetrics.java
src/main/java/io/github/pandeyayushk/jobstream/metrics/RedisJobMetrics.java
src/main/java/io/github/pandeyayushk/jobstream/api/JobStreamApiServer.java
src/main/java/io/github/pandeyayushk/jobstream/api/controller/JobController.java
src/main/java/io/github/pandeyayushk/jobstream/api/controller/QueueController.java
src/main/java/io/github/pandeyayushk/jobstream/api/controller/WorkerController.java
src/main/java/io/github/pandeyayushk/jobstream/api/controller/DlqController.java
src/main/java/io/github/pandeyayushk/jobstream/api/dto/JobSubmitRequest.java
src/main/java/io/github/pandeyayushk/jobstream/api/dto/ErrorResponse.java
src/test/java/io/github/pandeyayushk/jobstream/metrics/RedisJobMetricsTest.java
src/test/java/io/github/pandeyayushk/jobstream/api/JobStreamApiServerTest.java
```

### MODIFY

```
pom.xml  — Add io.javalin:javalin:6.1.3
src/main/java/io/github/pandeyayushk/jobstream/worker/Worker.java — Record completion/failure in JobMetrics
```

### DELETE

None.

### DO NOT TOUCH

```
src/main/java/.../serialization/*
src/main/java/.../executor/*
src/main/java/.../priority/*
```

## 10. Implementation Order

1. **`pom.xml`** — Add Javalin dependency.
2. **`JobMetrics.java`** & **`RedisJobMetrics.java`** — Metric collection contracts and Redis counters.
3. **`RedisJobMetricsTest.java`** — Unit test metrics increments and retrieval.
4. **`ErrorResponse.java`** & **`JobSubmitRequest.java`** — DTOs for API payloads.
5. **Controllers (`JobController`, `QueueController`, `WorkerController`, `DlqController`)** — Request handlers mapping HTTP to domain operations.
6. **`JobStreamApiServer.java`** — Routing, server configuration, and lifecycle.
7. **`Worker.java`** — Wire metrics updates into the worker lifecycle.
8. **`JobStreamApiServerTest.java`** — Integration test endpoints using HTTP client.

## 11. Testing Requirements

### Unit Tests
- `RedisJobMetrics`: Verify metric counters increment correctly for submitted, completed, and failed jobs.

### Integration Tests (HTTP Server)
- Start `JobStreamApiServer` on random test port:
  - `GET /health` returns 200 and `"status": "UP"`.
  - `POST /api/v1/jobs` with valid JSON returns 201 and valid `jobId`.
  - `GET /api/v1/jobs/{id}` returns the newly created job.
  - `GET /api/v1/jobs/{non-existent-uuid}` returns 404 with formatted `ErrorResponse`.
  - `GET /api/v1/queues` returns list with correct queue sizes.
  - `GET /api/v1/workers` reflects currently registered test workers.
  - `GET /metrics` shows incremented counters.

## 12. Failure and Edge Cases

- **Redis Unreachable during Health Check:** `/health` must return HTTP 503 Service Unavailable with `{"status": "DOWN"}` without crashing server.
- **Port Conflict:** Provide clean error message if configured HTTP port is already bound.
- **Malformed JSON in POST:** Return HTTP 400 Bad Request with explanation, not HTTP 500.

## 13. Validation

```powershell
# Compile
mvn compile

# Run all test suites
mvn test

# Run API and Metrics tests
mvn test -Dtest="io.github.pandeyayushk.jobstream.api.**,io.github.pandeyayushk.jobstream.metrics.**"

# Manual verification using curl
curl http://localhost:8080/health
curl http://localhost:8080/metrics
curl -X POST http://localhost:8080/api/v1/jobs -H "Content-Type: application/json" -d '{"type":"email:send","payload":{"recipient":"test@example.com"}}'
```

## 14. Completion Criteria

- [ ] Javalin integrated cleanly.
- [ ] `JobMetrics` accurately tracks system throughput and error counts.
- [ ] `/health` endpoint reports Redis and system connectivity.
- [ ] REST API endpoints for jobs, queues, workers, DLQ, and metrics are operational.
- [ ] Proper HTTP status codes (200, 201, 400, 404, 503) and JSON error responses returned.
- [ ] HTTP integration tests pass cleanly.

## 15. Self-Review Checklist

- [ ] Does `JobStreamApiServer.stop()` cleanly shut down the Jetty engine and release ports?
- [ ] Are all API inputs validated before reaching domain layers?
- [ ] Is sensitive information (Redis passwords, internal exceptions) stripped from HTTP responses?
- [ ] Are all previous tests passing?

## 16. Documentation Updates

### Requirements documentation
- Update `docs/requirements/observability.md` and `docs/requirements/http-api.md` with final endpoint specs.

### Architecture documentation
- Update `docs/architecture/project-structure.md` marking `metrics` and `api` active.

### ADRs
- **Create ADR-011:** Javalin HTTP server adoption and REST API architecture.

### Final README Notes
- Embedded REST API for job submission, queue inspection, worker monitoring, and DLQ management.
- Standard health check and metrics endpoints for production monitoring.

## 17. Git Milestone

- **Branch:** `phase-9-observability-and-http-api`
- **Base:** `master` (after Phase 8 merge)
- **Implementation milestone:** `JobStreamApiServer`, controllers, and `JobMetrics` functional.
- **Testing milestone:** All REST endpoints verified via integration test suite.
- **Review milestone:** ADR-011 recorded, self-review complete.
- **Merge condition:** Clean build and test execution on master.

## 18. What This Phase Enables

With observability and the HTTP API in place:
- **Phase 10 (Deployment & Production Hardening)** can package JobStream into Docker containers with health checks, environment-driven configuration, and production readiness.
- Any external language or dashboard can integrate with JobStream.
