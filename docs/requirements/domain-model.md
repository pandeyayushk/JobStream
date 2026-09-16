# Domain Model Requirements

## 1. Purpose

Defines the core logical entities for the job domain: what a `Job` is, how it is identified, how its lifecycle states are represented, and what its execution data (`Payload`) means, completely independent of serialization, storage, or execution infrastructure.

---

## 2. Functional Requirements

### 2.1 Job Identity (`JobId`)
- A Job must have a globally unique, immutable identity (`JobId`).
- Must wrap a standard `UUID` to ensure type safety (preventing accidental parameter swaps with worker IDs or queue names).
- Must support string serialization and deserialization (`JobId.fromString(...)`, `toString()`).
- Value object semantics: equality and hash code based strictly on the wrapped UUID.

### 2.2 Job Status (`JobStatus`)
- A Job must have a status representing its current state within the domain lifecycle.
- The `JobStatus` enum defines the complete closed set of 7 states as the system-wide domain vocabulary:
  `PENDING`, `QUEUED`, `PROCESSING`, `COMPLETED`, `FAILED`, `RETRYING`, `DEAD`.
- Must provide transition validation logic (e.g. `isValidTransition(JobStatus target)`).
- *Phase boundary rule:* Defining the states in the domain model does **not** imply that Phase 1 can execute or enqueue jobs. Subsystems causing transitions are implemented in their respective phases.

### 2.3 Job Payload (`Payload`)
- A Job must carry a `Payload` representing the domain input data required for execution.
- **Stable Domain Contract:** `Payload` is an immutable, key-value data structure encapsulating a `Map<String, Object>`.
- Must provide type-safe accessors (e.g., `getString(key)`, `getInt(key)`, `getBoolean(key)`, `asMap()`).
- Must enforce immutability at construction via defensive copying (`Map.copyOf`).
- Must reject null keys or null payload maps.
- **Separation of Concerns:** The domain model does **not** define or care how this payload is encoded on the wire or disk (no JSON, no Jackson annotations, no byte arrays).

### 2.4 Job Entity (`Job`)
- The primary domain entity representing a unit of work.
- Attributes:
  - `JobId id` (mandatory, immutable)
  - `String type` (mandatory, non-empty, represents the work category, e.g. `"email:send"`)
  - `JobStatus status` (mandatory, initial state: `PENDING`)
  - `Payload payload` (mandatory, immutable)
  - `Instant createdAt` (mandatory, UTC timestamp of creation)
  - `Instant updatedAt` (mandatory, UTC timestamp of last status change)
  - `Map<String, String> metadata` (optional key-value metadata for tracing/correlation)
- Must provide controlled lifecycle transition methods (e.g., `withStatus(JobStatus newStatus)`) returning a new or updated instance with updated timestamp.

---

## 3. Non-Functional Requirements

- **Zero Infrastructure Dependencies:** Core domain classes must not import or depend on Redis, Jackson, Jedis, Spring, configuration loaders, or database drivers.
- **Thread Safety:** Domain objects must be thread-safe through immutability or controlled state transition methods.
- **Strict Validation:** Fail-fast on invalid state (null IDs, empty types, null payloads, illegal state transitions).

---

## 4. Design Decisions (Phase 1 Ownership)

- **Job Representation:** Record vs Class. If a class is chosen, ensure the core fields (`id`, `type`, `payload`, `createdAt`) are immutable, and status updates are strictly controlled.
- **Payload Representation:** Encapsulated immutable Map-based domain value object.

---

## 5. Dependencies

- **None** (Core Domain depends only on the Java 21 Standard Library).

---

## 6. Phase Ownership

- **Phase 1 (Domain Model):** Establishes `Job`, `JobId`, `JobStatus`, and `Payload`.
