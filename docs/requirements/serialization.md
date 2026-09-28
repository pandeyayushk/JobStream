# Serialization Requirements

## 1. Purpose

Defines how `Job` and `Payload` cross persistence boundaries as JSON without adding serialization dependencies to the domain model.

## 2. Serialization boundary

`JobSerializer` provides `serialize(Job)` and `deserialize(String)`. `JacksonJobSerializer` uses Jackson Databind 3.2.2 and a private `JobData` record as the mapping boundary. Domain classes contain no Jackson annotations or dependencies.

## 3. Persisted Job representation

The JSON representation includes the Job ID, type, status, payload, `createdAt`, `updatedAt`, metadata, and Phase 6 retry state:

- `retryCount`
- `maxRetries`
- `lastErrorReason`
- `lastFailedAt`

The retry fields round-trip through Redis persistence and reload, so a persisted Job retains the attempt allowance and failure diagnostics used by retry and DLQ handling. Nullable failure fields remain null when no failure has occurred.

## 4. Fidelity and errors

Serialization round trips preserve identity, type, status, payload values, timestamps, metadata, and retry state. Jackson failures and invalid Job IDs are translated to `SerializationException`. Unknown JSON property compatibility is not configured.

## 5. Dependencies and phase ownership

The serialization package depends on the domain model. Phase 2 introduced the serializer and Redis persistence boundary; Phase 6 extended the persisted Job representation with retry state.
