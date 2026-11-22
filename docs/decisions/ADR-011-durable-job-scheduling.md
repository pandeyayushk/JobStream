# ADR-011: Durable Redis Job Scheduling

- **Status:** Accepted; implemented in Phase 8, Step 3
- **Date:** 2026-11-22

## Context

Jobs need durable delayed execution without placing serialized Jobs in scheduler indexes or transitioning them to `QUEUED` before a scheduler poller exists. Scheduling also needs to retain the destination queue and update the authoritative Job record as one atomic operation.

## Decision

Store scheduled Job IDs in the Redis Sorted Set `jobstream:schedule:delayed`, with execution epoch milliseconds as the score. Store each destination in the Hash `jobstream:schedule:targets`, keyed by JobId. The Job record remains authoritative and is serialized through the existing `JobSerializer`.

Jobs carry a persisted monotonically increasing revision, defaulting to zero for legacy records. Every production Job writer uses an atomic Lua compare-and-set on that revision, increments it, and updates related status or queue indexes in the same script. This includes `RedisJobRepository.save`, queue submission, scheduler updates, and dead-letter move/requeue operations. `save` returns the persisted Job snapshot, and Worker lifecycle updates continue from that returned revision. Stale snapshots fail explicitly with a concurrency exception.

`RedisJobScheduler` reads and deserializes the current record, accepts only `PENDING` or `RETRYING` Jobs, and creates an immutable copy with `scheduledAt` and the next revision. One Lua script compares the complete current JSON snapshot and atomically writes the updated Job JSON, due Sorted Set member, and destination queue. If a competing repository write wins between read and script, the scheduler detects the compare conflict, rereads the newer Job, and reapplies scheduling while preserving its fields. Repeated scheduling uses the JobId as the Sorted Set member, updating its due score rather than adding a duplicate.

Scores are limited to epoch-millisecond values exactly representable as Redis doubles. Instants outside the `long` epoch-millisecond range or outside Redis's exact integer range are rejected. The complete Instant, including sub-millisecond precision, is retained in the serialized Job.

Relative delays use an injected `Clock` (system UTC by default), reject negative durations, and calculate the due Instant before invoking the same atomic scheduling path. Zero delay is allowed.

## Consequences

- The due timestamp and destination survive application restarts.
- All Job record writers share one revision protocol, so stale repository, submission, scheduler, and dead-letter snapshots cannot silently overwrite a newer Job.
- Existing serialized records without a revision are read as revision zero; their first update writes a revisioned record.
- Scheduling does not modify Job status; pending jobs remain `PENDING` and retry jobs remain `RETRYING` until a future poller handles them.
- A failed compare-and-set does not partially update the Job or scheduler indexes; key types are validated before mutation in the script.
- Scheduler storage holds JobId references only, while the Job record retains payload, priority, retry metadata, and the full timestamp.
- Scheduling permits only `PENDING` and `RETRYING` states; queued, processing, and terminal Jobs are rejected.

## Alternatives Considered

- **Save the Job then add the Sorted Set member:** rejected because a crash between commands can leave a Job marked scheduled but absent from the scheduler.
- **Protect only the scheduler with compare-and-set:** rejected because ordinary repository, submission, or dead-letter writes could still overwrite its update.
- **Merge `scheduledAt` during ordinary saves:** rejected because it would preserve only one field and could still lose newer priority, retry, payload, or metadata changes.
- **Use an external lock:** rejected because every writer would need to coordinate lock ownership and recovery; atomic Redis scripts and revisions provide a single datastore-serialized protocol.
- **Store serialized Jobs in the Sorted Set:** rejected because persistence remains authoritative in the existing Job key and duplicate copies could diverge.
