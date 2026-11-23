# ADR-013: Use Process-Local Counters for Phase 9 Operational Metrics

- **Status:** Accepted for Phase 9 implementation
- **Date:** 2026-11-23
- **Deciders:** JobStream maintainers

## Context

Phase 9 needs basic operational counters, while JobStream's durable state lives in Redis. Writing counters to Redis on every submission and worker lifecycle event would add network operations and failure coupling. Updating job state and counters atomically would require expanding multiple existing Lua scripts. The current system does not promise exact accounting or cross-instance aggregation.

## Decision

Use thread-safe process-local monotonic counters exposed via JSON. Increment only after the relevant durable operation returns successfully:

- submitted: committed `QueueCoordinator.submit` result;
- completed: successful persisted `COMPLETED` transition;
- failed: each successful persisted `FAILED` execution attempt, including retryable failures;
- retried: each successful persisted `RETRYING` decision, not the later requeue;
- moved to DLQ: successful DLQ move, not listing or requeueing out.

Metric recording must be non-throwing and cannot fail job processing. Queue-depth and active-worker count are read-time Redis-backed gauges, not maintained metrics.

## Alternatives considered

- **Redis counters:** cluster-wide but introduces hot-path Redis writes; not atomic with current Job transitions and can fail independently.
- **Hybrid local and Redis counters:** adds duplicate semantics and still cannot provide exact cross-system atomicity.
- **Local counters:** selected for isolation and low overhead; explicitly approximate and per process.

## Consequences

- Values reset on restart and are separate per JobStream process.
- A crash between a durable event and local increment can omit an event. These counters are not an audit or billing source.
- Concurrent counter updates are thread-safe; a multi-counter read is not a globally consistent snapshot.
- Metrics-write failure is structurally avoided for local counters; gauge read failures affect only the HTTP response (503), never job processing.
- Execution/queue latency is excluded until durable per-attempt and queued timestamps exist.
