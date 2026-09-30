# Phase 7 - Command-Line Interface (CLI)

**Status: Complete.** This guide records the Phase 7 behavior implemented in the repository.

## 1. Objective

Provide operators a command-based interface for submitting jobs and inspecting queues, workers, job state, and dead-letter work. The CLI is an interface/application layer over existing JobStream abstractions; it does not replace the domain model or infrastructure responsibilities.

## 2. Architecture

`io.github.pandeyayushk.jobstream.cli.JobStreamCli` is the Picocli root command. Commands delegate to existing abstractions: submission uses `QueueCoordinator`, job status uses the authoritative `JobRepository`, queue inspection uses `JobQueue`, worker listing uses `WorkerRegistry`, and dead-letter operations use `DeadLetterQueue`.

The root command creates a shared `CliContext` lazily. Redis-related infrastructure and its Jedis `RedisClient` are initialized only when a command needs them; the CLI closes the client centrally after command execution. Consequently, root and subcommand `--help` and root `--version` work without connecting to Redis.

## 3. Command hierarchy

```text
jobstream
├── job
│   ├── submit
│   └── status
├── queue
│   ├── size
│   └── peek
├── worker
│   └── list
└── dlq
    ├── list
    ├── requeue
    └── purge
```

- `job submit --type <type> --payload <json> [--queue <name>]` accepts a JSON object payload, defaults the queue to `default`, submits through `QueueCoordinator`, and prints the created Job ID and queue.
- `job status <jobId>` loads the Job from `JobRepository` and prints type, status, retry counts, timestamps, and available failure diagnostics. Invalid IDs and unknown jobs produce clear errors.
- `queue size <name>` reports queue length. `queue peek <name> [--limit <n>]` displays queued IDs without removing them; absent a limit it requests the full current queue.
- `worker list` displays active workers from `WorkerRegistry` using the available worker information.
- `dlq list [--offset <n>] [--limit <n>]` lists dead-letter jobs (default offset 0, limit 100).
- `dlq requeue <jobId> --target-queue <name>` delegates manual requeue to `DeadLetterQueue`.
- `dlq purge` first checks the DLQ size. An empty DLQ is reported without prompting. Otherwise it removes entries only after `y` or `yes` (case-insensitive); any other input, empty input, or EOF cancels.

## 4. Redis connection and errors

Global options are `--host` and `--port`, defaulting to `localhost` and `6379`. No password, database, or environment-variable connection configuration is implemented.

Execution failures are translated to user-facing messages. When a Jedis failure is present, its underlying detail is included where available; otherwise a generic Redis connection message is used. Parameter/usage failures are handled separately, show command usage, and return Picocli's invalid-input exit code. Help and version do not require Redis.

## 5. Validation and completion

Phase 7 completion was gated with 289 tests: 0 failures, 0 errors, 0 skipped. `mvn -DskipTests package` completed successfully. Current validation commands:

```powershell
mvn clean test
mvn -DskipTests package
```

## 6. Related documents

- [CLI Requirements](../../requirements/cli.md)
- [Project Structure](../../architecture/project-structure.md)
- [System Overview](../../architecture/system-overview.md)
- [ADR-009: Picocli CLI Framework](../../decisions/ADR-009-cli-framework.md)
