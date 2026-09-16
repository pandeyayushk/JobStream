# Phase 1 — Domain Model

## 1. Objective

Establish the core domain model for JobStream: the foundational types that represent a job, its identity, its full lifecycle contract, and its immutable payload. Every subsequent phase builds upon these types.

## 2. Why This Phase Exists

The domain model is the foundation of the entire system. Every component — persistence, queue, worker, executor — operates on jobs. Without a well-defined job domain, every other layer would need to invent its own representation, leading to inconsistency and fragile integration.

This phase has zero dependencies on external infrastructure (no Redis, no networking, no JSON libraries, no configuration frameworks). It is pure Java domain modeling.

## 3. Scope

**In scope:**
- Job identity value object (`JobId`)
- Complete job lifecycle status enum (`JobStatus`) defining all 7 lifecycle states
- Job type identifier (`String`, e.g. `"email:send"`)
- Domain payload value object (`Payload`) representing structured execution input
- Core Job domain entity (`Job`)
- Creation and update timestamps (`Instant`)
- Job metadata (`Map<String, String>` for tracing and correlation)
- Unit tests verifying domain validation, immutability, and equality

**Out of scope:**
- Executing jobs or triggering transitions (Phase 4, Phase 5)
- Serialization and JSON mapping (Phase 2)
- Persistence and Redis storage (Phase 2)
- Queue operations (Phase 3)
- Configuration loaders (Phase 2 / Phase 10)

## 4. Prerequisites

- Phase 0 complete (Maven project compiles and smoke test passes)
- JDK 21 installed
- Apache Maven 3.8+ installed

## 5. Read Before Starting

- `docs/requirements/domain-model.md` — Core domain requirements
- `docs/architecture/project-structure.md` — Package hierarchy (`job`, `payload` have zero dependencies)
- `docs/architecture/system-overview.md` — Architectural role of domain entities
- `docs/architecture/job-lifecycle.md` — Complete lifecycle states and transition rules

## 6. Concepts to Understand

Before implementing, ensure you understand:

- **Value Objects vs Entities (DDD):**
  - `JobId` is a value object (equality based strictly on the wrapped UUID).
  - `Payload` is a value object (equality based on contained data).
  - `Job` is an entity (identity defined by its `JobId`, attributes evolve across its lifecycle).
- **Java Records:**
  - Introduced in Java 16. Ideal for immutable value objects (`JobId`, `Payload`).
  - Reference: [JEP 395: Records](https://openjdk.org/jeps/395)
- **Java Enums for Closed State Machines:**
  - Enums enforce a closed set of domain states.
  - Reference: [Java Enum Types](https://docs.oracle.com/javase/tutorial/java/javaOO/enum.html)
- **Defensive Copying & Immutability:**
  - Using `Map.copyOf(...)` to guarantee that external callers cannot modify payload or metadata maps after construction.
- **Instant (UTC Timestamps):**
  - Using `java.time.Instant.now()` for machine-readable UTC timestamps.
  - Reference: [Instant JavaDoc](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/time/Instant.html)
- **Domain State vs Subsystem Execution:**
  - Defining a state in `JobStatus` establishes the system's vocabulary; it does not implement the subsystem that triggers that transition.

## 7. Design Decisions

### 7.1 Job: Entity Design & Immutability Strategy

**What to decide:** Should `Job` be a Java `record` or a traditional `class`?

**Why it matters:**
- Records generate boilerplate (`equals`, `hashCode`, `toString`) automatically and enforce shallow immutability.
- However, a Job transitions across multiple states (`PENDING` → `QUEUED` → `PROCESSING` → `COMPLETED`/`FAILED`).
- If `Job` is a record, state transitions require creating a copy with modified fields (wither pattern, e.g. `job.withStatus(JobStatus.QUEUED)`).
- If `Job` is a class, state transitions can be handled via package-private setters or controlled methods.

**Recommendation:** Implement `Job` as an immutable class (or record) using the copy/wither pattern (`job.withStatus(...)`). An immutable job entity guarantees complete thread safety across concurrent workers without synchronized locks.

**Record this decision in:** ADR-003 (to be created during this phase).

### 7.2 JobId: Wrapper Type vs Raw UUID

**What to decide:** Wrap UUID in a dedicated `JobId` type vs passing raw `UUID` / `String`?

**Recommendation:** Use a `JobId` record wrapping a `UUID`. It guarantees type safety, prevents parameter-order bugs in constructors, and centralizes ID parsing (`JobId.fromString(...)`).

### 7.3 Payload Contract: Pure Domain Value Object

**What to decide:** How should job payloads be represented in the domain layer?

**Decision:** `Payload` is a dedicated domain value object wrapping an immutable `Map<String, Object>`.
- Provides type-safe accessors: `getString(key)`, `getInt(key)`, `getBoolean(key)`, `asMap()`.
- Immutability enforced via `Map.copyOf`.
- **Strict Boundary:** The domain model does **not** know about JSON, Jackson, byte arrays, or Redis. Serialization is exclusively the responsibility of Phase 2.

**Record this decision in:** ADR-004 (to be created during this phase).

### 7.4 JobStatus Enum: Complete Lifecycle Vocabulary

**What to decide:** Which states should be in `JobStatus` in Phase 1?

**Decision:** Define all 7 lifecycle states in `JobStatus` from Phase 1:
`PENDING`, `QUEUED`, `PROCESSING`, `COMPLETED`, `FAILED`, `RETRYING`, `DEAD`.
- Defining all states in Phase 1 establishes the full domain vocabulary and transition validation rules once, preventing breaking enum changes in subsequent phases.
- Phase 1 tests verify validation rules, but do **not** execute jobs or simulate queue dispatch.

## 8. Requirements

### Functional Requirements

1. **`JobId` (package `io.github.pandeyayushk.jobstream.job`):**
   - Factory method `JobId.generate()` producing a random UUID.
   - Factory method `JobId.fromString(String uuidString)` parsing a UUID string with validation.
   - Methods: `value()` returning `UUID`, `toString()` returning UUID string format.
   - Value equality and hash code based on the wrapped `UUID`.

2. **`JobStatus` (package `io.github.pandeyayushk.jobstream.job`):**
   - Enum values: `PENDING`, `QUEUED`, `PROCESSING`, `COMPLETED`, `FAILED`, `RETRYING`, `DEAD`.
   - Method `boolean isTerminal()` returning `true` for `COMPLETED` and `DEAD`.
   - Method `boolean isValidTransition(JobStatus target)` enforcing legal pathways per `job-lifecycle.md`.

3. **`Payload` (package `io.github.pandeyayushk.jobstream.payload`):**
   - Factory methods: `Payload.of(Map<String, Object> data)` and `Payload.empty()`.
   - Defensive copy on creation (`Map.copyOf`).
   - Accessors: `Optional<String> getString(String key)`, `Optional<Integer> getInt(String key)`, `Optional<Boolean> getBoolean(String key)`, `Map<String, Object> asMap()`.
   - Method `boolean containsKey(String key)`.
   - Rejects null keys and null input maps with `NullPointerException`.

4. **`Job` (package `io.github.pandeyayushk.jobstream.job`):**
   - Factory method: `Job.create(String type, Payload payload)`:
     - Generates new `JobId`.
     - Sets initial status to `PENDING`.
     - Sets `createdAt` and `updatedAt` to `Instant.now()`.
     - Initializes empty metadata map.
   - Full constructor / factory for reconstruction from storage:
     - `Job.reconstitute(JobId id, String type, JobStatus status, Payload payload, Instant createdAt, Instant updatedAt, Map<String, String> metadata)`
   - State transition helper: `Job withStatus(JobStatus newStatus)`:
     - Validates transition using `status.isValidTransition(newStatus)`.
     - Returns new `Job` instance with updated status and updated `updatedAt` timestamp.
   - Validations:
     - `id` != null
     - `type` != null and not blank
     - `status` != null
     - `payload` != null
     - `createdAt` != null
     - `updatedAt` != null

### Non-Functional Requirements

- **Zero dependencies:** No external libraries in `pom.xml`.
- **Thread safety:** Guaranteed via immutability.
- **Fail-fast:** Descriptive exception messages for invalid arguments.

## 9. File Change Manifest

### CREATE

```
src/main/java/io/github/pandeyayushk/jobstream/job/JobId.java
src/main/java/io/github/pandeyayushk/jobstream/job/JobStatus.java
src/main/java/io/github/pandeyayushk/jobstream/job/Job.java
src/main/java/io/github/pandeyayushk/jobstream/payload/Payload.java
src/test/java/io/github/pandeyayushk/jobstream/job/JobIdTest.java
src/test/java/io/github/pandeyayushk/jobstream/job/JobStatusTest.java
src/test/java/io/github/pandeyayushk/jobstream/job/JobTest.java
src/test/java/io/github/pandeyayushk/jobstream/payload/PayloadTest.java
```

### MODIFY

None. Phase 1 does not require modifying existing Phase 0 files.

### DELETE

None.

### DO NOT TOUCH

```
pom.xml                         — No dependencies to add
src/main/java/.../Main.java      — Phase 0 entry point unchanged
src/test/java/.../MainTest.java  — Phase 0 smoke test unchanged
```

## 10. Implementation Order

1. **`JobId.java`** — Identity value object.
2. **`JobIdTest.java`** — Unit tests for generation, parsing, equality.
3. **`JobStatus.java`** — Complete 7-state enum with transition validation logic.
4. **`JobStatusTest.java`** — Unit tests for terminal states and transition rules.
5. **`Payload.java`** — Domain payload container with defensive copying.
6. **`PayloadTest.java`** — Unit tests for typed accessors, immutability, validation.
7. **`Job.java`** — Core entity with validation and `withStatus` transition.
8. **`JobTest.java`** — Unit tests for job creation, validation, and immutability.

## 11. Testing Requirements

### Unit Tests

**JobIdTest:**
- `generate()` creates unique non-null UUID.
- Two separately generated IDs are not equal.
- `fromString(uuid.toString())` reproduces equal `JobId`.
- Malformed UUID string throws `IllegalArgumentException`.

**JobStatusTest:**
- Contains all 7 states: `PENDING`, `QUEUED`, `PROCESSING`, `COMPLETED`, `FAILED`, `RETRYING`, `DEAD`.
- `isTerminal()` returns `true` only for `COMPLETED` and `DEAD`.
- `isValidTransition` returns `true` for valid transitions (e.g. `PENDING` → `QUEUED`).
- `isValidTransition` returns `false` for illegal transitions (e.g. `COMPLETED` → `QUEUED`).

**PayloadTest:**
- Stored map values are accessible via `getString`, `getInt`, `getBoolean`.
- Modifying the original input map after payload creation does not affect the payload (defensive copy).
- Modifying map returned by `asMap()` throws `UnsupportedOperationException`.
- Null keys or null map throw `NullPointerException`.

**JobTest:**
- `Job.create("email:send", payload)` sets status to `PENDING` and timestamps to approximately now.
- Blank type throws `IllegalArgumentException`.
- Null payload throws `NullPointerException`.
- `withStatus(QUEUED)` transitions status, updates `updatedAt`, and leaves original instance unchanged.
- Illegal transition (e.g. `COMPLETED` to `QUEUED`) throws `IllegalStateException`.

## 12. Failure and Edge Cases

- Blank or whitespace-only job type → rejected immediately.
- Corrupted UUID string in `JobId.fromString` → fails fast with clear error.
- Mutating payload data after construction → prevented by unmodifiable collection.
- Time travel / null timestamps in reconstitution → rejected with validation error.

## 13. Validation

```powershell
# Compile the project
mvn compile

# Run all tests
mvn test

# Run only Phase 1 tests
mvn test -Dtest="io.github.pandeyayushk.jobstream.job.**,io.github.pandeyayushk.jobstream.payload.**"
```

Expected result: All tests pass cleanly, 0 failures, 0 warnings.

## 14. Completion Criteria

- [ ] `JobId` implemented and passing all unit tests.
- [ ] `JobStatus` defines all 7 lifecycle states and transition validation.
- [ ] `Payload` provides type-safe accessors and immutable encapsulation.
- [ ] `Job` provides immutable entity mechanics and controlled state transitions.
- [ ] Zero external dependencies added to `pom.xml`.
- [ ] `mvn test` exits 0 with all Phase 0 and Phase 1 tests passing.

## 15. Self-Review Checklist

- [ ] Does `job` or `payload` import any class outside `java.*`? (Must be strictly NO).
- [ ] Is `Payload` decoupled from JSON and serialization concerns?
- [ ] Are all 7 lifecycle states present in `JobStatus`?
- [ ] Does `Job` reject invalid transitions per `job-lifecycle.md`?
- [ ] Is immutability preserved across all domain types?

## 16. Documentation Updates

### Requirements documentation
- Confirm `docs/requirements/domain-model.md` reflects implemented accessor methods.

### Architecture documentation
- Verify `docs/architecture/job-lifecycle.md` matches `JobStatus.isValidTransition` logic.

### ADRs
- **Create ADR-003:** Job Entity Representation & Immutability Strategy.
- **Create ADR-004:** Domain Payload Contract.

### Final README Notes
- Domain model implemented with pure Java 21 value objects (`JobId`, `Payload`) and entities (`Job`).
- Closed lifecycle state machine (`JobStatus`) enforcing valid transitions.

## 17. Git Milestone

- **Branch:** `phase-1-domain-model`
- **Base:** `master` (Phase 0)
- **Implementation milestone:** Domain classes and unit tests created.
- **Testing milestone:** 100% unit test pass rate.
- **Review milestone:** Self-review checklist verified, ADR-003 and ADR-004 recorded.
- **Merge condition:** Clean Maven build and test pass on master.

## 18. What This Phase Enables

With the core domain model established:
- **Phase 2 (Serialization & Persistence)** has stable, immutable domain entities to serialize into JSON and persist in Redis.
- The entire system now shares a unified, type-safe domain vocabulary.
