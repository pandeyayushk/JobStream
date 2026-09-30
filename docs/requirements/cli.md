# Command Line Interface (CLI) Requirements

## Purpose

Provides a command-based operator interface over the existing JobStream domain and application/infrastructure abstractions.

## Implemented Commands

- `job submit --type <type> --payload <json> [--queue <name>]`: submits a JSON object payload through `QueueCoordinator`; queue defaults to `default`.
- `job status <jobId>`: reads the authoritative Job from `JobRepository` and reports state, retry data, timestamps, and available failure diagnostics. Invalid IDs and unknown jobs are reported as errors.
- `queue size <name>`: reports queue size.
- `queue peek <name> [--limit <n>]`: displays queued Job IDs without consuming them.
- `worker list`: displays active worker information through `WorkerRegistry`.
- `dlq list [--offset <n>] [--limit <n>]`: lists dead-letter jobs.
- `dlq requeue <jobId> --target-queue <name>`: manually requeues a dead-letter job through `DeadLetterQueue`.
- `dlq purge`: checks DLQ size and requires an affirmative `y` or `yes` confirmation before removing populated DLQ entries. Declining, empty input, or EOF cancels; an empty DLQ does not prompt.

## Connection and Error Behavior

The Picocli root command supports `--help`, `--version`, `--host`, and `--port`. Redis defaults are `localhost:6379`. Help/version work without Redis. Connection failures are translated to user-facing messages with underlying Jedis detail when available, or a generic Redis connection message when no useful detail exists. Parameter errors are handled separately and return a non-zero invalid-input exit code.

A shared lazy `CliContext` constructs Redis-related infrastructure on demand and centrally closes the Redis client. The CLI is an interface layer; domain, queue, repository, worker registry, and DLQ abstractions remain responsible for their existing operations.

## Constraints and Non-Goals

- No password, database, or environment-variable Redis configuration is implemented.
- The CLI is command-based; it does not provide an interactive shell or REPL.
- Worker functionality is limited to listing; there are no worker management commands.
- DLQ purge is operator-confirmed when the queue contains entries.
- Scheduling, priority, HTTP API, and daemon supervision are outside Phase 7.

## Dependencies

Picocli and the existing JobStream application/domain/infrastructure abstractions.

## Phase Ownership

Phase 7 (CLI) - Complete. See [Phase 7 documentation](../versions/v7-cli/README.md) and [ADR-009](../decisions/ADR-009-cli-framework.md).
