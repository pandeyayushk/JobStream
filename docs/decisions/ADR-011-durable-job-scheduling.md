# ADR-011: Durable Redis Job Scheduling

- **Status:** Accepted; implemented in Phase 8, Step 3
- **Date:** 2026-11-22

## Context

Jobs need durable delayed execution without placing serialized Jobs in scheduler indexes or transitioning them to `QUEUED` before a scheduler poller exists. Scheduling also needs to retain the destination queue and update the authoritative Job record as one atomic operation.

## Decision

Store scheduled Job IDs in the Redis Sorted Set `jobstream:schedule:delayed`, with execution epoch milliseconds as the score. Store each destination in the Hash `jobstream:schedule:targets`, keyed by JobId. The Job record remains authoritative and is serialized through the existing `JobSerializer`.

`RedisJobScheduler` reads and deserializes the current record, accepts only `PENDING` or `RETRYING` Jobs, and creates an immutable copy with `scheduledAt`. One Lua script compares the stored JSON to the version read by the scheduler; if it still matches, the script writes the updated Job JSON, updates the Sorted Set member, and stores the target queue. A compare conflict causes the scheduler to reread and retry, so concurrent Job changes are not overwritten by a stale snapshot. Repeated scheduling uses the JobId as the Sorted Set member, updating its due score rather than adding a duplicate.

Scores are limited to epoch-millisecond values exactly representable as Redis doubles. Instants outside the `long` epoch-millisecond range or outside Redis's exact integer range are rejected. The complete Instant, including sub-millisecond precision, is retained in the serialized Job.

Relative delays use an injected `Clock` (system UTC by default), reject negative durations, and calculate the due Instant before invoking the same atomic scheduling path. Zero delay is allowed.

## Consequences

- The due timestamp and destination survive application restarts.
- Scheduling does not modify Job status; pending jobs remain `PENDING` and retry jobs remain `RETRYING` until a future poller handles them.
- A failed compare-and-set does not partially update the Job or scheduler indexes; key types are validated before mutation in the script.
- Scheduler storage holds JobId references only, while the Job record retains payload, priority, retry metadata, and the full timestamp.
- Scheduling permits only `PENDING` and `RETRYING` states; queued, processing, and terminal Jobs are rejected.

## Alternatives Considered

- **Save the Job then add the Sorted Set member:** rejected because a crash between commands can leave a Job marked scheduled but absent from the scheduler.
- **Use `repository.save()` within the scheduler:** rejected because the current repository operation is not atomic with scheduler index updates and would overwrite a concurrent Job update without compare-and-set.
- **Store serialized Jobs in the Sorted Set:** rejected because persistence remains authoritative in the existing Job key and duplicate copies could diverge.
