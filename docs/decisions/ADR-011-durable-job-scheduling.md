# ADR-011: Durable Redis Job Scheduling

- **Status:** Accepted; implemented in Phase 8, Steps 3–4
- **Date:** 2026-11-22

## Context

Jobs need durable delayed execution without placing serialized Jobs in scheduler indexes or transitioning them to `QUEUED` before a scheduler poller exists. Scheduling also needs to retain the destination queue and update the authoritative Job record as one atomic operation.

## Decision

Store scheduled Job IDs in the Redis Sorted Set `jobstream:schedule:delayed`, with execution epoch milliseconds as the score. Store each destination in the Hash `jobstream:schedule:targets`, keyed by JobId. The Job record remains authoritative and is serialized through the existing `JobSerializer`.

Jobs carry a persisted monotonically increasing revision, defaulting to zero for legacy records. Every production Job writer uses an atomic Lua compare-and-set on that revision, increments it, and updates related status or queue indexes in the same script. This includes `RedisJobRepository.save`, queue submission, scheduler updates, and dead-letter move/requeue operations. `save` returns the persisted Job snapshot, and Worker lifecycle updates continue from that returned revision. Stale snapshots fail explicitly with a concurrency exception.

`RedisJobScheduler` reads and deserializes the current record, accepts only `PENDING` or `RETRYING` Jobs, and creates an immutable copy with `scheduledAt` and the next revision. One Lua script compares the complete current JSON snapshot and atomically writes the updated Job JSON, due Sorted Set member, and destination queue. If a competing repository write wins between read and script, the scheduler detects the compare conflict, rereads the newer Job, and reapplies scheduling while preserving its fields. Repeated scheduling uses the JobId as the Sorted Set member, updating its due score rather than adding a duplicate.

Scores are limited to epoch-millisecond values exactly representable as Redis doubles. Instants outside the `long` epoch-millisecond range or outside Redis's exact integer range are rejected. The complete Instant, including sub-millisecond precision, is retained in the serialized Job.

Relative delays use an injected `Clock` (system UTC by default), reject negative durations, and calculate the due Instant before invoking the same atomic scheduling path. Zero delay is allowed.

`RedisSchedulerPoller.pollOnce(batchSize)` selects at most the requested number of candidate IDs using an injected Clock. To prevent retained malformed records or invalid destinations from monopolizing the first batch, candidate selection uses a persistent rotating rank stored at `jobstream:schedule:poll-cursor`. One Lua script counts due entries, normalizes the rank modulo the current due count, selects one bounded page with `ZRANGEBYSCORE LIMIT`, and advances the rank atomically. Pollers share progression through Redis; the cursor survives process restarts. Insertions, removals, repairs, and reschedules can shift ranks, so each selection normalizes against the current due count and wraps. After a finite set of mutations, repeated polls over the stable due set visit every entry; newly due entries are included in the next scan cycle even if they were inserted before the current rank. The batch size limits candidate IDs returned to Java, deserialized, and given release attempts, so no invocation attempts more than `batchSize` Jobs. Redis may traverse the skipped rank offset internally when serving a later page; the bound is on candidate processing, not constant Redis command cost as the due set grows.

Each candidate is released by one Lua script that validates its due entry, destination, authoritative serialized Job snapshot and Redis key types, then performs the revisioned `QUEUED` update, status-index change, schedule cleanup, and priority-queue insertion atomically. Queue insertion calls the same Lua helper used by normal submissions, preserving Stream-ID FIFO tie-breaking and the JobId membership hash. Competing pollers coordinate candidate windows through the atomic shared cursor, while the per-Job release script remains the final duplicate-release guard. Missing Jobs and Jobs that are no longer schedulable are removed from scheduler indexes; malformed records and missing destinations remain indexed for repair rather than being discarded. A failed release leaves the schedule and authoritative Job unchanged. Batch work is atomic per Job, so already released jobs remain committed if a later candidate fails.

## Consequences

- The due timestamp and destination survive application restarts.
- All Job record writers share one revision protocol, so stale repository, submission, scheduler, and dead-letter snapshots cannot silently overwrite a newer Job.
- Existing serialized records without a revision are read as revision zero; their first update writes a revisioned record.
- Scheduling does not modify Job status; pending jobs remain `PENDING` and retry jobs remain `RETRYING` until a future poller handles them.
- A failed compare-and-set does not partially update the Job or scheduler indexes; key types are validated before mutation in the script.
- Scheduler storage holds JobId references only, while the Job record retains payload, priority, retry metadata, and the full timestamp.
- A bounded poller releases due `PENDING` and `RETRYING` Jobs to the configured priority queue; it does not execute them.
- Deferred entries remain recoverable and are revisited as the persistent candidate rank rotates; poller restarts and concurrent instances use the same Redis cursor.
- Scheduling permits only `PENDING` and `RETRYING` states; queued, processing, and terminal Jobs are rejected.

## Alternatives Considered

- **Save the Job then add the Sorted Set member:** rejected because a crash between commands can leave a Job marked scheduled but absent from the scheduler.
- **Protect only the scheduler with compare-and-set:** rejected because ordinary repository, submission, or dead-letter writes could still overwrite its update.
- **Merge `scheduledAt` during ordinary saves:** rejected because it would preserve only one field and could still lose newer priority, retry, payload, or metadata changes.
- **Use an external lock:** rejected because every writer would need to coordinate lock ownership and recovery; atomic Redis scripts and revisions provide a single datastore-serialized protocol.
- **Store serialized Jobs in the Sorted Set:** rejected because persistence remains authoritative in the existing Job key and duplicate copies could diverge.
