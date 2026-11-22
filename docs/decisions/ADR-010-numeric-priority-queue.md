# ADR-010: Numeric Priority Queue Ordering

- **Status:** Accepted; implemented in Phase 8, Step 2
- **Date:** 2026-11-22

## Context

Phase 7 queues use Redis Lists at `jobstream:queue:<name>`, with `LPUSH`/`RPOP` FIFO behavior. Phase 8 adds an integer priority to persisted Jobs. Queue submission and Worker acquisition must honor that value while keeping equal-priority jobs FIFO, preserving ID-only queue entries, and retaining existing queued work.

## Decision

Represent each active named queue as a Redis Sorted Set at `jobstream:priority-queue:<name>`. The score is the negated Job priority, so the smallest score (the `ZPOPMIN` order) represents the highest priority. Integer priorities are exactly representable as Redis doubles because their full range is far below the IEEE-754 exact-integer limit; sequence data is never added to the score.

Each member contains a fixed-width encoding of a Redis Stream ID followed by the Job ID:

```text
<20-digit milliseconds>-<20-digit stream sequence>:<job-id>
```

The per-queue stream at `jobstream:priority-sequence:<name>` allocates monotonic, unique IDs using Redis `XADD *`. Its generated stream entry is immediately `XDEL`eted; Redis retains the stream's last-generated ID as the allocator high-water mark. Members with the same priority score are ordered lexicographically, which gives FIFO tie-breaking without floating-point sequence arithmetic. A per-queue hash at `jobstream:priority-members:<name>` maps Job IDs to their current member, preventing duplicate entries and allowing the index to be cleared atomically on dequeue.

Enqueue, dequeue, and legacy migration use Lua scripts. Normal submission persists the queued Job, updates status indexes, allocates its ordering ID, and adds its Job ID to the Sorted Set in one script. `RedisJobQueue.enqueue(JobId, queueName)` reads priority from the authoritative serialized Job record; if a record is absent (as in low-level queue use), priority defaults to zero. Worker retry and shutdown requeue paths therefore retain priority without moving Redis operations into Worker.

## Existing FIFO data compatibility

The old `jobstream:queue:<name>` List keys are retained as the legacy namespace. On the first queue operation or submission for a name, one atomic Lua migration walks the old list from its right edge to its left edge (the original FIFO dequeue order), reads priority from each authoritative Job record, and inserts IDs into the new structures. Missing or pre-priority Job records receive priority zero. Duplicate IDs are represented once. The script deletes the old List only after migrating its entries, so existing queued jobs are not stranded. New code does not write active queue entries to legacy List keys. The separate DLQ List key remains unchanged.

## Consequences

- Larger integer priorities are consumed first; negative and boundary integer values remain valid.
- FIFO order among equal priorities is the Redis script execution order, including concurrent producers.
- Redis serializes each script, making stream ID allocation, duplicate checks, and insertion atomic across concurrent submissions.
- Each named queue has separate queue, sequence, and member-index keys.
- Queue inspection must use the JobQueue abstraction or the new priority queue key; legacy List keys are removed once migrated.
- The sequence allocator retains a small Stream key per queue as its monotonic high-water mark.

## Alternatives Considered

- **Combine priority and sequence into one floating-point score:** rejected because arbitrary sequence values can exceed exact floating-point integer precision and corrupt tie ordering.
- **Use priority alone as the score and sequence in the member:** selected in principle, with a Redis Stream ID as the exact tie-break value; this keeps the score limited to a small exact integer range.
- **Keep the Redis List and scan/reorder on dequeue:** rejected because producers and consumers would need additional coordination and queue operations would no longer have a simple atomic ordering structure.
