# ADR-003: Job Entity Representation and Immutability

**Status:** Accepted

## Context

`Job` is a domain entity identified by `JobId` and moves through a controlled lifecycle. It must preserve valid construction state while allowing lifecycle state to change without exposing shared mutable state.

## Decision

Implement `Job` as an immutable final Java class. Constructor validation establishes its invariants: non-null ID, status, payload, and timestamps; a non-null, non-blank type; and defensively copied immutable metadata.

Lifecycle changes use copy/with semantics through `withStatus(JobStatus)`. It validates the requested transition, refreshes `updatedAt`, and returns a new `Job` while preserving the entity identity and all unaffected state.

This implementation does not expose mutable fields or mutable metadata and does not mutate a job during a transition. Those guarantees avoid shared mutable state and make concurrent use safer.

## Alternatives Considered

- **Mutable class with setters:** Rejected because arbitrary setters could bypass lifecycle validation and expose partially updated or shared mutable state.
- **Java record:** Rejected as the primary representation for `Job`. Records suit value-oriented data carriers, but JobStream treats `Job` as a lifecycle entity with controlled transition behavior. The class retains immutable state while making those operations explicit.

## Consequences

- Callers must retain the instance returned by `withStatus(...)`.
- Entity identity remains stable across lifecycle copies through `JobId`.
- Immutability here follows from final state, defensive copying, and the absence of mutators; it is not a general claim that all records or all Java classes are inherently thread-safe.

