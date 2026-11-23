# JobStream

A learning-oriented, production-minded distributed job queue written in Java, backed by Redis.

## What is JobStream?

JobStream is a distributed job queue system designed to explore and implement core concepts of distributed systems: job lifecycle management, persistent queuing, concurrent worker processing, retry mechanisms, and operational observability.

The project is being built incrementally, phase by phase, with thorough documentation guiding every step.

## Current Status

**Phase 8 - Scheduling & Priority infrastructure** (complete)

- Maven project: `io.github.pandeyayushk:jobstream:1.0-SNAPSHOT`
- Java 21 LTS, Maven, JUnit 5
- Application entry point: `io.github.pandeyayushk.jobstream.Main`
- Domain model: `JobId`, `JobStatus`, `Payload`, and `Job`
- JSON serialization via Jackson Databind 3.3.0, without Jackson annotations on domain classes
- Redis persistence via Jedis 8.0.1 `RedisClient`, with authoritative job records and secondary status indexes
- Numeric priority queueing via `JobQueue`, `RedisJobQueue`, and `RedisPriorityQueueStore`; Redis Sorted Sets order higher integer priorities first, with FIFO order for equal priorities
- Durable delayed scheduling via `JobScheduler` and `RedisJobScheduler`; scheduler indexes store JobIds and destination queue names while Job records remain authoritative
- Bounded `SchedulerPoller.pollOnce(batchSize)` with injected Clock, atomic per-Job release, revision updates, and a persistent rotating cursor
- Revision-based optimistic concurrency for Job writes; stale snapshots are rejected and repository/submission operations return persisted Job snapshots
- Legacy active queue Lists migrate to the priority ZSET on first queue use; the dead-letter queue remains a Redis List
- Atomic queue submission through `QueueCoordinator` and `RedisJobSubmissionStore`, using Redis Lua compare-and-set
- Worker lifecycle, registry, TTL heartbeat, graceful shutdown, and bounded concurrent processing
- Type-based execution through `ExecutorRegistry` and `JobExecutor`, with `ExecutionResult` outcomes persisted by Worker
- Retry policies: no retry, fixed delay, and exponential backoff with jitter
- In-memory scheduled retry requeueing that leaves processing threads available
- Retry state persisted as part of each Job record
- Redis Dead-Letter Queue (DLQ), failure handling, inspection, purge, and manual requeue
- Picocli command-line interface for job submission/status, queue inspection, worker listing, and DLQ operations

Phases 0-8 infrastructure are complete. Phase 9 is in progress: the JDK HTTP server with live/ready health checks and Redis queue registry/backfill are implemented; metrics and management endpoints remain pending. Phase 8 integrations that connect Worker retry backoff, CLI scheduling options, and production poller lifecycle remain deferred. Deployment/hardening (Phase 10) is planned.

## Technology Stack

| Component | Technology |
|---|---|
| Language | Java 21 LTS |
| Build System | Apache Maven |
| Testing | JUnit 5 |
| Serialization | Jackson Databind 3.3.0 |
| Persistence | Redis via Jedis 8.0.1 |
| HTTP server | JDK `com.sun.net.httpserver.HttpServer` |
| Queue Backend | Redis Sorted Sets for priority queues; Redis Set for queue-name registry; Redis Lists for legacy queue migration and the DLQ |

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

### Interactive CLI (Windows)

Build the executable JAR and start the interactive shell:

```powershell
mvn package
.\js.bat
```

The shell connects to Redis at `localhost:6379` by default. The one-shot Picocli commands remain available by passing arguments, for example `java -jar target/jobstream-1.0-SNAPSHOT.jar --help`. To run `js.bat` from any directory, add the JobStream project directory to your Windows `PATH`. For the current PowerShell session, use `$env:Path += ";C:\path\to\JobStream"`; replace the example with the actual project directory. For a persistent entry, add that directory under **Environment Variables > Path** in Windows Settings.

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
- [Phase 6 Reliability & Retry](docs/versions/v6-reliability-and-retry/README.md) - Implemented retry and DLQ behavior
- [Phase 7 CLI](docs/versions/v7-cli/README.md) - Implemented Picocli operational commands
- [Phase 8 Scheduling & Priority](docs/versions/v8-scheduling-and-priority/README.md) - Implemented priority queue and durable scheduler infrastructure, with integrations still deferred
- [ADR Index](docs/decisions/README.md) - Architecture decision records

## Development Roadmap

| Phase | Name | Status |
|---|---|---|
| 0 | Project Foundation | Complete |
| 1 | Domain Model | Complete |
| 2 | Serialization & Persistence | Complete |
| 3 | Queue System | Complete |
| 4 | Worker Foundation | Complete |
| 5 | Execution Engine | Complete |
| 6 | Reliability & Retry | Complete |
| 7 | CLI | Complete |
| 8 | Scheduling & Priority Infrastructure | Complete |
| 9 | Observability & HTTP API | Planned |
| 10 | Deployment & Hardening | Planned |

Phase 8's Worker retry integration, CLI scheduling flags, and production scheduler bootstrap remain deferred. Each phase has a guide in `docs/versions/`; completed guides describe the implemented scope and deferred integrations.

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
