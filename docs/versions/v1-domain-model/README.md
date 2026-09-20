# Phase 1 — Domain Model

## Status

**Complete.** The Phase 1 domain model and its unit tests are implemented.

## 1. Objective

Establish the core domain model for JobStream: the types that represent a job, its identity, lifecycle contract, and immutable payload. Later phases build on these types without introducing infrastructure concerns into the domain.

## 2. Implemented Scope

- `JobId` is a Java record wrapping a non-null `UUID`. It provides `generate()`, `fromString(String)`, the record accessor `value()`, and `toString()`.
- `JobStatus` defines `PENDING`, `QUEUED`, `PROCESSING`, `COMPLETED`, `FAILED`, `RETRYING`, and `DEAD`, including terminal-state and transition validation.
- `Payload` is an immutable final value-object class backed by `Map<String, Object>`, with typed optional accessors and defensive copying.
- `Job` is an immutable final entity class. `create(...)` establishes a new pending job, `reconstitute(...)` restores supplied state, and `withStatus(...)` returns a new instance after validating the lifecycle transition.
- Phase 1 unit tests exist for `JobId`, `JobStatus`, `Payload`, and `Job`.

## 3. Domain and Infrastructure Boundary

The core domain has zero production/runtime infrastructure dependencies: no Redis, networking, JSON libraries, configuration frameworks, persistence APIs, or execution frameworks. JUnit 5 remains the project test dependency; this rule does not prohibit test dependencies.

`Payload` has no knowledge of JSON, Jackson, Redis, byte arrays, or persistence. Serialization belongs to Phase 2. Phase 1 defines lifecycle vocabulary and validates transitions; later phases implement the producer, queue, worker, execution, retry, and operator subsystems that trigger them.

## 4. Implemented API

### `JobId`

Package: `io.github.pandeyayushk.jobstream.job`

- `JobId.generate()`
- `JobId.fromString(String)`
- `UUID value()`
- `String toString()`
- record equality and hash code

Null UUID values are rejected.

### `JobStatus`

Package: `io.github.pandeyayushk.jobstream.job`

- `boolean isTerminal()` is `true` only for `COMPLETED` and `DEAD`.
- `boolean isValidTransition(JobStatus target)` permits exactly:

  - `PENDING -> QUEUED`
  - `QUEUED -> PROCESSING`
  - `PROCESSING -> COMPLETED`
  - `PROCESSING -> FAILED`
  - `FAILED -> RETRYING`
  - `FAILED -> DEAD`
  - `RETRYING -> QUEUED`
  - `DEAD -> QUEUED`

`COMPLETED` has no outgoing transition.

### `Payload`

Package: `io.github.pandeyayushk.jobstream.payload`

- `Payload.of(Map<String, Object>)` and `Payload.empty()`
- `Optional<String> getString(String)`
- `Optional<Integer> getInt(String)`
- `Optional<Boolean> getBoolean(String)`
- `Map<String, Object> asMap()`
- `boolean containsKey(String)`

Construction uses `Map.copyOf(...)`. Null input maps and null keys are rejected; missing values and wrong typed access return `Optional.empty()`. The exposed map is immutable.

### `Job`

Package: `io.github.pandeyayushk.jobstream.job`

Fields and accessors:

- `JobId id()`
- `String type()`
- `JobStatus status()`
- `Payload payload()`
- `Instant createdAt()`
- `Instant updatedAt()`
- `Map<String, String> metadata()`

`Job.create(String, Payload)` generates an ID, assigns `PENDING`, uses one initial `Instant` for both timestamps, and starts with empty metadata. `Job.reconstitute(...)` preserves the supplied identity, status, timestamps, and metadata. `Job.withStatus(JobStatus)` validates through `JobStatus.isValidTransition(...)`, preserves identity, type, payload, creation time, and metadata, refreshes `updatedAt`, and returns a new job without mutating the original.

The constructor/factories enforce non-null ID, status, payload, and timestamps; a non-null, non-blank type; and defensively copied immutable metadata.

## 5. Testing

The completed Phase 1 tests cover:

- `JobId` generation, parsing, equality, and invalid input.
- `JobStatus` state membership, terminal states, and legal/illegal transitions.
- `Payload` typed access, wrong-type optional results, defensive copying, immutable map exposure, and null validation.
- `Job` creation defaults, reconstitution, validation, immutable metadata, valid copy-on-transition behavior, and rejected transitions.

## 6. Completion Criteria

- [x] `JobId` implemented and tested.
- [x] `JobStatus` defines all seven states and exact transition validation.
- [x] `Payload` provides typed accessors and immutable encapsulation without serialization knowledge.
- [x] `Job` provides immutable entity mechanics and controlled `withStatus(...)` transitions.
- [x] Phase 1 tests for `JobId`, `JobStatus`, `Payload`, and `Job` exist and pass.
- [x] No production/runtime infrastructure dependency was added for the domain model.
- [x] Requirements, lifecycle architecture, package status, ADRs, and root roadmap were reconciled with Phase 1.

## 7. Final Review

Implementation and tests are complete. The domain model is ready for Phase 2 serialization and persistence work, which remains responsible for encoding and storage.
