# ADR-005: Atomic Redis Job Submission with MULTI/EXEC

**Status:** Accepted

## Context

Submitting a job changes its authoritative Redis record and status index while making its `JobId` available to consumers. Performing those writes separately can strand a `QUEUED` job outside a queue or expose an ID whose persisted state was not updated.

## Problem

The submission boundary must persist the queued job, remove the ID from its prior status index, add it to the `QUEUED` index, and add it to the named Redis list without another Redis client observing an interleaved partial sequence.

## Options considered

- Redis `MULTI`/`EXEC` transaction with the four fixed writes.
- A Redis Lua script performing the same writes.
- Separate repository and queue operations.

## Decision

Use Redis `MULTI`/`EXEC` in `RedisJobSubmissionStore`. The transaction queues and executes:

1. `SET jobstream:job:<id> <queued-job-json>`
2. `SREM jobstream:status:<original-status> <id>`
3. `SADD jobstream:status:QUEUED <id>`
4. `LPUSH jobstream:queue:<queueName> <id>`

The original job identifies the status set to remove; the queued job is the persisted state and supplies the queued ID. `QueueCoordinatorImp` owns validation and transition preparation, then delegates this Redis-specific operation through `JobSubmissionStore`.

## Rationale

Persistence and queue storage use the same Redis instance. Submission is a fixed sequence of commands and currently needs no complex server-side conditional logic. `MULTI`/`EXEC` is simpler to read and maintain than Lua for that sequence.

## Consequences

- The four submission writes execute sequentially without interleaving by other Redis clients.
- Job records remain authoritative in `JobRepository`; queues contain only IDs.
- `JobQueue` and `JobRepository` remain independent, and Redis-specific submission logic stays outside `RedisJobQueue`.
- This is not a traditional rollback transaction. Redis does not undo commands already executed if a later command fails at execution time; `MULTI`/`EXEC` supplies ordered, non-interleaved execution rather than compensating rollback.

## Alternatives rejected

- **Lua:** Capable of atomic server-side logic, but unnecessary for the fixed, unconditional Phase 3 write sequence and less direct for maintainers to inspect.
- **Separate writes:** Rejected because failures or interleaving could violate the persisted-state and queue-reference invariant.

## Future considerations

Lua remains an option if submission becomes conditional or otherwise complex enough to benefit from server-side logic. Any future change must preserve the boundary between authoritative job persistence, `JobId` queue operations, and application-level coordination.
