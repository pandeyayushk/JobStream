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
- A bounded scheduler poll must release due jobs into their configured ready queues.
- Polling must preserve job priority and equal-priority FIFO ordering.
- Releasing a scheduled job, updating its status and revision, clearing scheduler indexes, and inserting queue membership must be atomic.
- Missing jobs and jobs that have left `PENDING` or `RETRYING` are cleaned from scheduler indexes; malformed jobs or invalid destinations remain scheduled for recovery without monopolizing later poll batches.
- Candidate selection uses a persistent rotating rank and progresses through the due set across polls, including when deferred entries remain present.
- Polling uses an injectable clock for deterministic due-time decisions.

## Constraints
- Redis sorted sets are suitable for both scheduling and priority.
- Scheduling precision is best-effort (not real-time guarantees).

## Design Questions
- Job scheduling data is indexed by ID and destination, while the repository Job record remains authoritative.
- Scheduler releases are performed in bounded batches; polling never executes jobs.

## Dependencies
- Queue Semantics (Phase 3)

## Phase Ownership
- Phase 8 (Scheduling & Priority). Exact priority levels and implementation owned here.
