# Phase 7 — Command-Line Interface (CLI)

## 1. Objective

Provide an administrative and operational CLI tool for operators to interact with JobStream: submit jobs, inspect job status, manage queues, view active workers and heartbeats, inspect dead-letter queue contents, and trigger dead-letter requeueing.

## 2. Why This Phase Exists

Up to Phase 6, interactions with JobStream require programmatic code or unit/integration tests. Operators and developers need a command-line tool to:
- Enqueue test jobs without writing Java test code.
- Check queue depth and worker health in running environments.
- Inspect failed jobs in the dead-letter queue and diagnose root causes.
- Re-queue dead jobs once transient issues or downstream services are resolved.
- Provide a clean administrative entry point to the system.

**Depends on:**
- Phase 1 (`Job`, `JobStatus`, `Payload`)
- Phase 2 (`JobRepository`, `RedisConfig`)
- Phase 3 (`JobQueue`)
- Phase 4 (`WorkerRegistry`)
- Phase 6 (`DeadLetterQueue`)

## 3. Scope

**In scope:**
- CLI command parsing framework integration (evaluating `picocli` vs standard parsing)
- CLI entry point command: `jobstream`
- Subcommands:
  - `job submit --type <type> --payload <json> [--queue <name>]`
  - `job get <jobId>`
  - `queue list` / `queue size <name>`
  - `worker list`
  - `dlq list [--limit <n>]`
  - `dlq requeue <jobId> [--target-queue <name>]`
  - `dlq purge`
- Formatted tabular / JSON console output
- Configurable Redis connection options (`--host`, `--port`, `--password`) or environment variable fallback

**Out of scope:**
- HTTP REST API (Phase 9)
- Real-time curses/TUI dashboard (Phase 9/10)
- Daemon worker process supervision (Phase 10)

## 4. Prerequisites

- Phase 6 complete (queues, workers, DLQ operational)
- Redis instance running
- Terminal / command line environment (PowerShell, Bash)

## 5. Read Before Starting

- `docs/requirements/cli.md` — Functional specifications for CLI
- `docs/architecture/project-structure.md` — Package hierarchy for `cli`
- `docs/requirements/configuration.md` — Configuration and CLI parameters
- `docs/decisions/ADR-001-java-21-and-maven.md` — Dependency conventions

## 6. Concepts to Understand

- **Command Pattern & Subcommand Architectures:** Structuring hierarchical CLI tools (`jobstream <noun> <verb>`).
- **Picocli Framework:** Industry standard for Java CLI tools with annotations, subcommands, tab completion, and help generation.
  - Reference: [Picocli Documentation](https://picocli.info/)
- **Exit Codes:** Standard UNIX exit codes (`0` for success, `1` for general error, `2` for usage error).
- **Console Output Formatting:** Human-readable tables vs structured machine-readable JSON (`--json` flag).

## 7. Design Decisions

### 7.1 CLI Framework Selection

**What to decide:** Custom command-line parser vs third-party library (`picocli` vs `args4j`).

**Key considerations:**
- Manual parsing: Zero external dependencies, but tedious to support subcommands, validation, and help text.
- `picocli`: Extremely mature, rich annotation support, automatic ANSI color output, zero runtime overhead, excellent documentation.

**Recommendation:** Add `info.picocli:picocli:4.7.5` to `pom.xml`.

**Record this decision in:** ADR-009 (to be created during this phase).

### 7.2 Entry Point Structure

**What to decide:** Should the CLI be a separate jar, or an alternate subcommand / main class in the existing jar?

**Recommendation:** Include CLI classes in package `io.github.pandeyayushk.jobstream.cli`, executable either by specifying `io.github.pandeyayushk.jobstream.cli.JobStreamCli` or configuring `Main.java` to dispatch to CLI if arguments are provided.

## 8. Requirements

### Functional Requirements

1. **Root CLI Runner (`JobStreamCli`):**
   - Top-level command `jobstream` with global options `--host`, `--port`.
   - Built-in `--help` and `--version`.

2. **Job Commands (`JobCommand`):**
   - `submit`: Creates a new `Job` with specified type and JSON payload, enqueues it, outputs generated `JobId`.
   - `status`: Fetches `Job` by ID from `JobRepository` and displays status, timestamps, and retry count.

3. **Queue Commands (`QueueCommand`):**
   - `size`: Returns current length of named queue.
   - `peek`: Shows head jobs in queue without consuming them.

4. **Worker Commands (`WorkerCommand`):**
   - `list`: Queries `WorkerRegistry` and displays active workers, assigned queues, status, and last heartbeat age.

5. **DLQ Commands (`DlqCommand`):**
   - `list`: Lists jobs currently in `jobstream:queue:dead-letter`.
   - `requeue`: Moves job from DLQ back to active queue for re-processing.
   - `purge`: Clears all jobs in DLQ after operator confirmation.

### Non-Functional Requirements

- Clear error reporting for malformed JSON or invalid UUIDs.
- Friendly error message if Redis connection cannot be established.
- Output formatting readable on 80-column terminal.

## 9. File Change Manifest

### CREATE

```
src/main/java/io/github/pandeyayushk/jobstream/cli/JobStreamCli.java
src/main/java/io/github/pandeyayushk/jobstream/cli/command/JobCommand.java
src/main/java/io/github/pandeyayushk/jobstream/cli/command/QueueCommand.java
src/main/java/io/github/pandeyayushk/jobstream/cli/command/WorkerCommand.java
src/main/java/io/github/pandeyayushk/jobstream/cli/command/DlqCommand.java
src/main/java/io/github/pandeyayushk/jobstream/cli/format/TableFormatter.java
src/test/java/io/github/pandeyayushk/jobstream/cli/JobStreamCliTest.java
src/test/java/io/github/pandeyayushk/jobstream/cli/TableFormatterTest.java
```

### MODIFY

```
pom.xml  — Add info.picocli:picocli:4.7.5 dependency
```

### DELETE

None.

### DO NOT TOUCH

```
src/main/java/.../job/*
src/main/java/.../queue/*
src/main/java/.../worker/*
src/main/java/.../executor/*
src/main/java/.../retry/*
```

## 10. Implementation Order

1. **`pom.xml`** — Add `picocli` dependency.
2. **`TableFormatter.java`** — Simple utility to format rows/columns as ASCII tables.
3. **`JobStreamCli.java`** — Root command setup and Redis connection wiring.
4. **`JobCommand.java`** — Job submission and status inquiry subcommands.
5. **`QueueCommand.java`** & **`WorkerCommand.java`** — Queue length and worker listing subcommands.
6. **`DlqCommand.java`** — Dead-letter management subcommands.
7. **`JobStreamCliTest.java`** — Unit tests for CLI parsing, command execution, and exit codes.

## 11. Testing Requirements

### Unit Tests
- CLI option parsing: `--host`, `--port` parsed and defaulted correctly.
- Missing required arguments (e.g. `job submit` without `--type`) returns non-zero exit code.
- `TableFormatterTest`: Verify alignment and header formatting for console tables.

### Integration Tests
- Execute `JobStreamCli` against test Redis:
  - Submit job via CLI → JobId returned → Verify job exists in Redis with status `QUEUED`.
  - Check queue size via CLI → returns count.
  - Query worker list via CLI → shows registered test workers.
  - List DLQ via CLI → shows dead jobs.
  - Requeue dead job via CLI → job leaves DLQ and enters specified queue.

## 12. Failure and Edge Cases

- **Redis Unreachable:** CLI prints `"Error: Could not connect to Redis at <host>:<port>. Ensure Redis is running."` and exits with code 1 instead of dumping raw stack trace.
- **Invalid UUID in `job get <id>`:** Prints `"Invalid Job ID format: <id>"` and exits with code 2.
- **Malformed JSON payload:** Prints `"Error: Invalid JSON payload: <error>"` and exits without creating job.

## 13. Validation

```powershell
# Compile
mvn compile

# Test CLI commands via java -cp
java -cp "target/classes;target/dependency/*" io.github.pandeyayushk.jobstream.cli.JobStreamCli --help

# Submit a job
java -cp "target/classes;target/dependency/*" io.github.pandeyayushk.jobstream.cli.JobStreamCli job submit --type email:send --payload '{"to":"dev@example.com"}'

# Query queue
java -cp "target/classes;target/dependency/*" io.github.pandeyayushk.jobstream.cli.JobStreamCli queue size default

# Query DLQ
java -cp "target/classes;target/dependency/*" io.github.pandeyayushk.jobstream.cli.JobStreamCli dlq list
```

## 14. Completion Criteria

- [ ] Picocli integrated cleanly in `pom.xml`.
- [ ] Root `JobStreamCli` runs and displays help text.
- [ ] `job submit` and `job get` subcommands functional.
- [ ] `queue size` and `peek` subcommands functional.
- [ ] `worker list` subcommands display registered workers.
- [ ] `dlq list`, `requeue`, and `purge` subcommands functional.
- [ ] Friendly error messages on network or argument failure.
- [ ] All tests pass (`mvn test` exits 0).

## 15. Self-Review Checklist

- [ ] Does CLI exit with proper exit codes (0 for success, 1/2 for errors)?
- [ ] Is password/sensitive config protected from console logging?
- [ ] Does CLI close Jedis connections cleanly upon exit?
- [ ] Are all CLI subcommands documented in `--help`?
- [ ] No regression in previous phase tests.

## 16. Documentation Updates

### Requirements documentation
- Update `docs/requirements/cli.md` confirming implemented commands.

### Architecture documentation
- Update `docs/architecture/project-structure.md` marking `cli` package active.

### ADRs
- **Create ADR-009:** Picocli adoption and CLI subcommand structure.

### Final README Notes
- JobStream provides a built-in CLI for job submission, queue inspection, worker monitoring, and DLQ management.

## 17. Git Milestone

- **Branch:** `phase-7-cli`
- **Base:** `master` (after Phase 6 merge)
- **Implementation milestone:** Full suite of CLI commands functional and verified against Redis.
- **Testing milestone:** Unit and integration tests covering CLI commands pass.
- **Review milestone:** ADR-009 recorded, self-review complete.
- **Merge condition:** Clean build and test execution on master.

## 18. What This Phase Enables

With the CLI in place:
- **Phase 8 (Scheduling & Priority)** can expose priority submission and schedule inspection directly through the CLI.
- Operators have a complete tool to test, debug, and monitor running JobStream instances.
