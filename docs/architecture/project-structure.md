# Project Structure

This is the canonical guide to the implemented Java packages and their responsibilities. Phase 8 priority infrastructure lives in `queue`; scheduling infrastructure lives in `schedule`. There is no `priority` package.

## Repository layout

```text
src/main/java/io/github/pandeyayushk/jobstream/
  Main.java
  cli/           Picocli commands and shared CLI context
  config/        RedisConfig
  executor/      JobExecutor, ExecutionResult, registry
  job/           Job, JobId, JobStatus
  payload/       Payload domain abstraction
  persistence/   JobRepository and RedisJobRepository
  queue/         JobQueue, RedisJobQueue, priority queue storage,
                 submission coordinator and Redis submission store
  retry/         retry policies, DeadLetterQueue and RedisDeadLetterQueue
  schedule/      JobScheduler, RedisJobScheduler, SchedulerPoller,
                 RedisSchedulerPoller and SchedulerPollResult
  serialization/ JobSerializer and JacksonJobSerializer
  worker/        Worker lifecycle, configuration and registry

src/test/java/io/github/pandeyayushk/jobstream/
  cli/ config/ executor/ job/ payload/ persistence/ queue/
  retry/ schedule/ serialization/ worker/
```

Metrics and HTTP API packages are not present in `src/main/java`; they remain planned for Phase 9. Deployment and hardening work remains Phase 10.

## Package responsibilities and status

| Package | Responsibility and implemented types | Status |
|---|---|---|
| `job` | Immutable `Job`, `JobId`, `JobStatus`; priority, optional `scheduledAt`, retry metadata, and persisted revision | Implemented, Phase 1 and Phase 8 domain extension |
| `payload` | Structured execution payload | Implemented, Phase 1 |
| `serialization` | `JobSerializer`, `JacksonJobSerializer`; legacy-compatible persisted Job JSON | Implemented, Phase 2 |
| `persistence` | `JobRepository`, `RedisJobRepository`, revision-aware save and status indexes | Implemented, Phases 2 and 8 concurrency updates |
| `queue` | `JobQueue`, `RedisJobQueue`, `RedisPriorityQueueStore`, `QueueCoordinator`, `QueueCoordinatorImp`, `JobSubmissionStore`, `RedisJobSubmissionStore` | Implemented, Phases 3 and 8 priority queue |
| `worker` | Worker acquisition, execution lifecycle, registry and heartbeat | Implemented, Phases 4–5; not wired to durable retry scheduling |
| `executor` | `JobExecutor`, `ExecutionResult`, executor registry | Implemented, Phase 5 |
| `retry` | Retry policy and DLQ abstractions/implementation | Implemented, Phase 6; retry delays remain Worker-local and in-memory |
| `cli` | Picocli operator commands for submission, status, queue inspection, workers and DLQ | Implemented, Phase 7; no scheduling or priority flags |
| `schedule` | `JobScheduler`, `RedisJobScheduler`, `SchedulerPoller`, `RedisSchedulerPoller`, `SchedulerPollResult`, `JobSchedulingException` | Infrastructure implemented, Phase 8; no runtime bootstrap |
| `config` | `RedisConfig` | Implemented; centralized application configuration remains future work |

`RedisPriorityQueueStore` owns the active priority queue's Redis key layout, ordering scripts, duplicate-membership handling, and legacy List migration. `RedisSchedulerPoller` shares that queue storage helper and owns bounded due-candidate progression and atomic release. The `Job` domain package remains independent of Redis and Jackson.

## Data ownership and dependencies

- The serialized Job at `jobstream:job:<jobId>` is authoritative. Status sets, active queues, the DLQ, and scheduler indexes contain JobId references or routing data.
- `QueueCoordinatorImp` prepares the `QUEUED` Job and delegates submission to `JobSubmissionStore`. `RedisJobSubmissionStore` atomically checks revision, persists the returned Job snapshot, updates status indexes, and inserts the JobId into its priority queue.
- `RedisJobRepository`, queue submission, scheduling, and DLQ updates use optimistic concurrency when writing Job records. Stale revisions fail rather than replacing newer Job fields.
- `RedisJobScheduler` owns durable scheduling writes. `RedisSchedulerPoller` reads authoritative Jobs and, in one release script per Job, changes status/revision, updates indexes, removes scheduling references, and inserts into the priority queue.
- `Worker` consumes through the `JobQueue` abstraction and loads the authoritative Job through the repository. Its existing retry scheduler is still in-memory and is not connected to `RedisJobScheduler`.

```text
cli -> QueueCoordinator -> JobSubmissionStore -> Redis Job + status indexes + priority queue
worker <- JobQueue <- priority queue
worker -> JobRepository -> authoritative Job

JobScheduler -> delayed ZSET + target Hash -> SchedulerPoller
                                   -> atomic release -> priority queue -> Worker
```

## Deferred Phase 8 integrations

1. Connecting Worker retry backoff to `RedisJobScheduler`.
2. Adding delayed and priority submission flags to the CLI.
3. Production runtime bootstrap and automatic poller lifecycle management.
