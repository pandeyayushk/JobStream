# Scheduling and Priority Requirements

## Purpose
Defines requirements for delaying job execution until a future time and for executing jobs based on relative priority levels.

## Functional Requirements
- Jobs must support delayed execution (execute after a specified time).
- Jobs must support priority levels.
- Higher-priority jobs must be dequeued before lower-priority jobs.
- Scheduled jobs must not be visible to workers until their scheduled time.
- Priority must be a numeric value (higher number = higher priority).
- Default priority must be defined for jobs without explicit priority.

## Non-Functional Requirements
- Efficient polling or pushing of delayed jobs into the ready queue.

## Constraints
- Redis sorted sets are suitable for both scheduling and priority.
- Scheduling precision is best-effort (not real-time guarantees).

## Design Questions
- Deferred: Exact priority levels and scheduling implementation.

## Dependencies
- Queue Semantics (Phase 3)

## Phase Ownership
- Phase 8 (Scheduling & Priority). Exact priority levels and implementation owned here.
