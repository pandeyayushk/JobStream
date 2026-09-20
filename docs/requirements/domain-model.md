# Domain Model Requirements

## 1. Purpose

Defines the core job-domain API independently of serialization, storage, queueing, and execution infrastructure.

## 2. `JobId`

`io.github.pandeyayushk.jobstream.job.JobId` is a Java record wrapping a non-null `UUID`. It provides `generate()`, `fromString(String)`, the record accessor `value()`, `toString()`, and record equality/hash-code semantics.

## 3. `JobStatus`

`io.github.pandeyayushk.jobstream.job.JobStatus` defines the closed lifecycle vocabulary: `PENDING`, `QUEUED`, `PROCESSING`, `COMPLETED`, `FAILED`, `RETRYING`, and `DEAD`. It provides `isTerminal()` and `isValidTransition(JobStatus target)`. Legal transitions are defined by [the lifecycle matrix](../architecture/job-lifecycle.md); all other transitions are rejected.

Phase 1 defines and validates this lifecycle contract only. The later queue, worker, execution, reliability, and operator subsystems trigger the corresponding transitions.

## 4. `Payload`

`io.github.pandeyayushk.jobstream.payload.Payload` is an immutable final domain value-object class backed by `Map<String, Object>`, not a Java record. Its public API is:

- `Payload.of(Map<String, Object>)`
- `Payload.empty()`
- `Optional<String> getString(String)`
- `Optional<Integer> getInt(String)`
- `Optional<Boolean> getBoolean(String)`
- `Map<String, Object> asMap()`
- `boolean containsKey(String)`

Payload defensively copies supplied data using `Map.copyOf(...)`; its exposed map is immutable. Null input maps and null keys are rejected. Missing keys and values requested through an incompatible typed accessor return `Optional.empty()`.

Payload is domain-only. It has no JSON, Jackson, Redis, byte-array, or persistence knowledge. Serialization is Phase 2 responsibility.

## 5. `Job`

`io.github.pandeyayushk.jobstream.job.Job` is an immutable final Java entity class identified by `JobId`. Its accessors are:

- `JobId id()`
- `String type()`
- `JobStatus status()`
- `Payload payload()`
- `Instant createdAt()`
- `Instant updatedAt()`
- `Map<String, String> metadata()`

Its construction and lifecycle API is:

- `Job.create(String type, Payload payload)` generates an ID, starts in `PENDING`, assigns the same initial `Instant` to both timestamps, and creates empty metadata.
- `Job.reconstitute(JobId id, String type, JobStatus status, Payload payload, Instant createdAt, Instant updatedAt, Map<String, String> metadata)` preserves the supplied persisted state without generating identity or timestamps and without resetting status.
- `Job.withStatus(JobStatus newStatus)` validates the transition through `JobStatus.isValidTransition(...)` and returns a new `Job`. It retains the ID, type, payload, creation time, and metadata while updating status and `updatedAt`.

Job validates non-null ID, status, payload, creation time, and update time; type must be non-null and non-blank. Metadata is defensively copied with `Map.copyOf(...)` and is immutable when returned. A transition never mutates the original instance.

## 6. Dependencies

The production domain depends only on the Java standard library. It must not depend on Redis, Jackson, persistence drivers, configuration loaders, queueing, networking, or execution frameworks. JUnit 5 is retained as the test dependency.
