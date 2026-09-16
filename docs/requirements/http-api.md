# HTTP API Requirements

## Purpose
Provides a RESTful interface for external applications and dashboards to integrate with and manage the JobStream system.

## Functional Requirements
- REST API for job management: submit, query, cancel.
- REST API for queue inspection: list queues, queue depth.
- REST API for worker inspection: list workers, worker status.
- REST API for dead-letter management: list, re-enqueue, delete.
- REST API for statistics: system-wide and per-queue metrics.
- All endpoints must return JSON.
- Error responses must include meaningful error codes and messages.
- The API must support basic health check endpoint.

## Non-Functional Requirements
- Low latency responses for standard queries.
- Proper HTTP status codes for errors.

## Constraints
- Must integrate cleanly with the underlying application state.

## Design Questions
- Deferred: HTTP framework choice, authentication.

## Dependencies
- Observability (Phase 9) for statistics

## Phase Ownership
- Phase 9 (Observability & API)
