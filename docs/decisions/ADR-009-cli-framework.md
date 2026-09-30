# ADR-009: Picocli CLI Framework

- **Status:** Accepted; implemented in Phase 7
- **Date:** 2026-10-01

## Context

JobStream needs a command-based operator interface for submitting jobs and inspecting jobs, queues, workers, and dead-letter work. These operations already have domain and application abstractions; the interface should compose them without moving their responsibilities into the CLI. A hierarchical command parser, generated help, argument validation, and predictable error handling are needed.

## Decision

Use Picocli for the `jobstream` command and its nested commands. Implement the CLI in `io.github.pandeyayushk.jobstream.cli` as an interface/application layer over existing abstractions.

### Command hierarchy

```text
jobstream
├── job: submit, status
├── queue: size, peek
├── worker: list
└── dlq: list, requeue, purge
```

`job submit` delegates to `QueueCoordinator`; `job status` reads the authoritative `JobRepository`; queue commands use `JobQueue`; worker listing uses `WorkerRegistry`; DLQ operations use `DeadLetterQueue`.

### Options and context lifecycle

Picocli provides `--help` and `--version`. Global Redis options are `--host` and `--port`, defaulting to `localhost` and `6379`. A shared lazy `CliContext` initializes Redis-related infrastructure only when required and the root CLI centrally closes its Redis client. Help and version therefore do not require Redis.

### Error handling

Execution exceptions are translated into user-facing messages. Jedis failures include the underlying detail when available and otherwise use a generic Redis connection message. Parameter/usage errors are handled separately, show usage, and return a non-zero invalid-input exit code.

### Destructive DLQ purge

Purge checks the DLQ size first and reports an empty queue without prompting. For a populated queue it requires `y` or `yes`, case-insensitive. Declining, blank input, and EOF cancel; purge is invoked only after confirmation.

## Why Picocli

Picocli supplies annotation-based nested commands, option and parameter validation, built-in help/version options, and configurable execution/parameter exception handlers. These fit the implemented command tree while keeping command code focused on calls to existing JobStream abstractions.

## Consequences

- Operators can use a consistent hierarchical CLI without introducing an interactive shell.
- CLI behavior composes existing domain and infrastructure operations rather than replacing them.
- Redis resources are shared for a command execution and closed centrally; commands that need infrastructure initialize it lazily.
- Invalid input and Redis/application failures have distinct user-facing handling.
- Populated DLQ data cannot be purged through the CLI without affirmative operator input.
- Redis connection customization is limited to host and port; password, database, and environment-based settings are not provided.

## Alternatives Considered

- **Hand-written argument parsing:** avoids a framework dependency but requires custom nested command parsing, help, and validation behavior.
- **Other CLI libraries (including args4j):** not selected; Picocli's command hierarchy and built-in help/version support directly match this CLI.
