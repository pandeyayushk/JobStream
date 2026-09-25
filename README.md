# JobStream

A learning-oriented, production-minded distributed job queue written in Java, backed by Redis.

## What is JobStream?

JobStream is a distributed job queue system designed to explore and implement core concepts of distributed systems: job lifecycle management, persistent queuing, concurrent worker processing, retry mechanisms, and operational observability.

The project is being built incrementally, phase by phase, with thorough documentation guiding every step.

## Current Status

**Phase 4 - Worker Foundation** (implemented)

- Maven project: `io.github.pandeyayushk:jobstream:1.0-SNAPSHOT`
- Java 21 LTS, Maven, JUnit 5
- Application entry point: `io.github.pandeyayushk.jobstream.Main`
- Domain model: `JobId`, `JobStatus`, `Payload`, and `Job`
- JSON serialization via Jackson Databind 3.2.2, without Jackson annotations on domain classes
- Redis persistence via Jedis 8.0.1 `RedisClient`, with authoritative job records and secondary status indexes
- Redis queueing via `JobQueue` and `RedisJobQueue`, using `LPUSH` with `RPOP`/`BRPOP` for FIFO `JobId` dispatch
- Atomic queue submission through `QueueCoordinator` and `RedisJobSubmissionStore`, using Redis `MULTI`/`EXEC`
- Worker lifecycle, registry, TTL heartbeat, graceful shutdown, and bounded concurrent processing
- Phase 4 execution boundary through `WorkerJobHandler`; production `JobExecutor` dispatch belongs to Phase 5
- Retry behavior belongs to Phase 6

Production executor dispatch, retry, and later operational features are not implemented yet.

## Technology Stack

| Component | Technology |
|---|---|
| Language | Java 21 LTS |
| Build System | Apache Maven |
| Testing | JUnit 5 |
| Serialization | Jackson Databind 3.2.2 |
| Persistence | Redis via Jedis 8.0.1 |
| Queue Backend | Redis Lists via Jedis 8.0.1 |

## Requirements

- JDK 21
- Apache Maven 3.8+
- Redis at `localhost:6379` for the Redis integration tests (for example, via Docker)

```powershell
java -version
mvn -version
```

## Build

```powershell
mvn package
```

## Test

```powershell
mvn test
```

## Run

```powershell
mvn compile
java -cp target/classes io.github.pandeyayushk.jobstream.Main
```

Expected output: `JobStream starting...`

## Documentation

JobStream uses a structured documentation system to guide development:

| Directory | Purpose |
|---|---|
| `docs/requirements/` | What the system must do |
| `docs/architecture/` | How the system is structured |
| `docs/decisions/` | Why architectural choices were made (ADRs) |
| `docs/versions/` | Phase-by-phase implementation guides |

### Key Documents

- [Project Structure](docs/architecture/project-structure.md) - Canonical package and repository layout
- [System Overview](docs/architecture/system-overview.md) - High-level architecture
- [Job Lifecycle](docs/architecture/job-lifecycle.md) - Job states and transitions
- [ADR Index](docs/decisions/README.md) - Architecture decision records

## Development Roadmap

| Phase | Name | Status |
|---|---|---|
| 0 | Project Foundation | Complete |
| 1 | Domain Model | Complete |
| 2 | Serialization & Persistence | Complete |
| 3 | Queue System | Complete |
| 4 | Worker Foundation | Complete |
| 5 | Execution Engine | Planned |
| 6 | Reliability & Retry | Planned |
| 7 | CLI | Planned |
| 8 | Scheduling & Priority | Planned |
| 9 | Observability & HTTP API | Planned |
| 10 | Deployment & Hardening | Planned |

Each phase has a detailed guide in `docs/versions/`. See those guides for prerequisites, requirements, file manifests, and validation criteria.

## Development Philosophy

JobStream follows a **build-first** approach:

```
READ -> UNDERSTAND -> DESIGN -> IMPLEMENT -> TEST -> REVIEW -> VALIDATE -> MERGE
```

The documentation guides the developer through each phase. AI assistance is reserved for architecture review, debugging, concurrency problems, and design trade-offs - not for routine questions about what to build next.

## Git Workflow

```
master (stable milestones)
  └── phase-N-name (implementation branch)
       └── implement -> test -> review -> validate -> merge
```

## License

This project is a personal learning and portfolio project.
