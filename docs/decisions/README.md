# Architecture Decision Records (ADR)

This directory contains Architecture Decision Records for the JobStream project.

## What is an ADR?
An Architecture Decision Record (ADR) is a short text file that captures an important architectural decision made along with its context and consequences.

## Naming Convention
Files should be named `ADR-NNN-short-description.md` where NNN is a sequential number.

## Status Values
- **Proposed**: The decision is proposed but not yet accepted.
- **Accepted**: The decision has been accepted and is (or will be) implemented.
- **Superseded**: The decision was accepted but has since been replaced by a newer decision.
- **Deprecated**: The decision is no longer relevant.

## Template Structure
Each ADR should follow this structure:
- **Title**: The name of the decision
- **Status**: Proposed, Accepted, Superseded, Deprecated
- **Context**: The forces at play, including technological, political, social, and project local.
- **Decision**: The response to these forces.
- **Alternatives Considered**: Other options that were evaluated and why they were not chosen.
- **Consequences**: The resulting context, after applying the decision.

## Index of ADRs
- [ADR-001: Use Java 21 LTS and Maven as the Foundation](ADR-001-java-21-and-maven.md)
- [ADR-002: Use Redis as the Persistence and Queue Backend](ADR-002-redis-as-persistence-and-queue-backend.md) — Accepted for Phase 2 persistence and Phase 3 queueing
- [ADR-003: Job Entity Representation and Immutability](ADR-003-job-entity-representation-and-immutability.md)
- [ADR-004: Domain Payload Contract](ADR-004-domain-payload-contract.md)
- [ADR-005: Atomic Redis Job Submission with MULTI/EXEC](ADR-005-atomic-redis-job-submission-with-multi-exec.md)
- [ADR-006: Worker Lifecycle, Graceful Shutdown, and Heartbeat](ADR-006-worker-lifecycle-graceful-shutdown-and-heartbeat.md)
- [ADR-007: Executor System](ADR-007-executor-system.md)
- [ADR-008: Retry, Failure Handling, and Dead-Letter Queue](ADR-008-retry-and-failure.md) — Accepted; implemented in Phase 6
- [ADR-009: Picocli CLI Framework](ADR-009-cli-framework.md) — Accepted; implemented in Phase 7
- [ADR-010: Numeric Priority Queue Ordering](ADR-010-numeric-priority-queue.md) — Accepted; implemented in Phase 8, Step 2
- [ADR-011: Durable Redis Job Scheduling](ADR-011-durable-job-scheduling.md) — Accepted; implemented in Phase 8, Steps 3–4

## Phase 9 Decisions
- [ADR-012: JDK HTTP Server for Phase 9](ADR-012-jdk-http-server.md) — Accepted for Phase 9 implementation
- [ADR-013: Process-Local Operational Metrics](ADR-013-process-local-metrics.md) — Accepted for Phase 9 implementation
- [ADR-014: Redis Queue Registry](ADR-014-redis-queue-registry.md) — Accepted for Phase 9 implementation

## Rule of Thumb

Trivial decisions don't need ADRs. ADRs are for decisions that affect system architecture, are hard to reverse, or have significant trade-offs.
