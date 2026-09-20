# ADR-004: Domain Payload Contract

**Status:** Accepted

## Context

Jobs need structured execution input while the core domain must remain independent of serialization and infrastructure.

## Decision

Implement `Payload` as an immutable domain value object backed by `Map<String, Object>`. `Payload.of(...)` takes a defensive copy with `Map.copyOf(...)`, and `asMap()` exposes the resulting immutable map.

The contract provides `getString`, `getInt`, and `getBoolean` typed accessors, plus `containsKey`. Null maps and keys are rejected. A missing value or a value of the wrong requested type returns `Optional.empty()`.

Payload is responsible only for domain data. It has no JSON, Jackson, Redis, byte-array, or persistence knowledge. Serialization is owned by Phase 2.

## Alternatives Considered

- **Raw `Map<String, Object>` passed throughout the system:** Rejected because callers could mutate or interpret data inconsistently and would not share typed access or validation behavior.
- **JSON or `String` payload in the domain:** Rejected because encoding concerns would leak into the domain and force consumers to parse infrastructure representations.
- **`byte[]` payload in the domain:** Rejected because it is opaque to domain consumers, mutable by default, and couples the domain to a serialization boundary.

## Consequences

- Domain consumers receive one immutable structured representation with predictable optional typed access.
- Serialization adapters in Phase 2 convert this representation at the persistence boundary.
- Payload values themselves may be reference objects; the contract protects the map structure and its exposure, not deep immutability of arbitrary contained objects.

