# Observability Requirements

## Purpose and status

Specify Phase 9 operational metrics and inspection. The requirements are finalized; no Phase 9 implementation is present yet. Worker retry backoff remains process-local, CLI scheduling flags remain absent, and scheduler-poller runtime bootstrap remains deferred from Phase 8.

## Counters

Counters are process-local, thread-safe, monotonic during one process lifetime, and reset at restart. They are approximate operational telemetry, not durable audit or billing records. A process crash between a durable job operation and the following counter update can omit an event. Multiple JobStream processes expose separate values; no global aggregation is promised.

| Metric | Increment condition | Semantics |
|---|---|---|
| `jobs_submitted_total` | Successful `QueueCoordinator.submit` returns its committed snapshot | Immediate submissions only; failed/conflicted submissions do not count. |
| `jobs_completed_total` | Worker successfully persists `COMPLETED` | Successfully completed Jobs. |
| `jobs_failed_total` | Worker successfully persists `FAILED` after an execution attempt | Counts failed attempts, including retryable attempts; not terminally failed unique Jobs. |
| `jobs_retried_total` | Worker successfully persists `RETRYING` after choosing another attempt | Counts retry decisions once; later queue requeue does not add another retry. |
| `jobs_moved_to_dlq_total` | DLQ move operation succeeds | Listing and requeueing out of DLQ do not increment it. |

Increment only after the corresponding operation succeeds. Process-local counter recording must not throw or cause job processing to fail. Counter updates are not atomic with existing Job persistence. Retries intentionally add one failed attempt and one retry decision; queue requeue is not another retry.

## Gauges

- Per-queue and total ready queue depth, read via `JobQueue.size` for names in `QueueRegistry`.
- Active worker registrations, read via `WorkerRegistry.listActiveWorkers`.
- Processing/idle worker counts are unavailable and must not be reported.

`GET /metrics` presents counters and gauges as JSON. Queue and worker gauges require Redis. If gauge retrieval fails, return 503 with the standard error body rather than returning partial data as complete. Counter recording itself is independent of Redis.

## Latency

Execution latency and queue waiting latency are out of scope. The Job record has no durable per-attempt start/completion timestamps and no `queuedAt`; `updatedAt` is overwritten at later lifecycle transitions. `createdAt` and current `updatedAt` cannot yield accurate execution or queue-wait measurements. Add explicit event timestamps before introducing these metrics.

## Storage and consistency

Use process-local `LongAdder`-style counters. Do not write Redis on hot job lifecycle paths in Phase 9. Local counters are thread-safe but snapshots across several counters are not a single consistent point in time. Redis-backed counters could aggregate across processes but couple processing to Redis writes and can still diverge from Job commits unless every lifecycle script is expanded. A hybrid adds duplicate semantics without a current requirement and is not selected.

Counter values reset at process restart and are per instance. A crash between durable operation and local increment can omit an event; this is accepted for best-effort telemetry. Metrics failures never cause submission, execution, retry, or DLQ handling to fail. Redis gauge failures surface only to the API caller as 503.

## Worker inspection

Worker listing may return ID, stored status, `startedAt`, and active registration as inferred from heartbeat-key TTL. It cannot return last-heartbeat timestamp or processing state because the current registry does not store those values. Stale heartbeat records are excluded by `listActiveWorkers`, subject to normal expiry races.

## Phase boundary

Phase 9 does not connect Worker retry backoff to durable scheduling, add CLI scheduling flags, or bootstrap a production scheduler poller. Prometheus exposition, durable cluster-wide counters, latency metrics, and richer worker state are deferred.
