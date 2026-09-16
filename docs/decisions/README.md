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
- [ADR-002: Use Redis as the Persistence and Queue Backend](ADR-002-redis-as-persistence-and-queue-backend.md)

## Rule of Thumb
Trivial decisions don't need ADRs. ADRs are for decisions that affect system architecture, are hard to reverse, or have significant trade-offs.
