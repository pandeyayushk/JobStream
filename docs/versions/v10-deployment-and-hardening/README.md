# Phase 10 — Deployment & Production Hardening

## 1. Objective

Harden JobStream for production deployment. Deliver centralized 12-Factor configuration (environment variables and YAML/properties), multi-stage Docker packaging, unified executable Fat JAR build, JVM signal handling for zero-downtime shutdown, and Docker Compose orchestration for local multi-worker topologies.

## 2. Why This Phase Exists

A distributed job queue running on localhost via `mvn` commands is an engineering prototype. For production viability:
- Applications must follow 12-Factor App principles (strict configuration separation from code via environment variables).
- Packaging must produce self-contained, reproducible deployment artifacts (Docker container, standalone fat JAR).
- Operating systems send signals (`SIGTERM`, `SIGINT`) during container rotation or pod terminations; workers must gracefully drain active jobs without abandoning tasks in `PROCESSING`.
- Operators need automated multi-node local environments (via `docker-compose.yml`) to test horizontal scaling, network partitions, and failovers.

**Depends on:**
- All prior phases (Phase 1 through Phase 9)

## 3. Scope

**In scope:**
- Centralized `JobStreamConfig` loading from environment variables, system properties, and optional `application.yaml` / `.properties`
- Fat JAR packaging via `maven-shade-plugin` or `maven-assembly-plugin`
- Multi-stage Dockerfile based on Eclipse Temurin 21 (JRE Alpine / Slim)
- `docker-compose.yml` orchestrating Redis, JobStream API Server, and multiple Worker instances
- JVM Shutdown Hook handling `SIGTERM` / `SIGINT` ensuring workers finish active executions before process exit
- Production documentation and final root `README.md` synthesis

**Out of scope:**
- Kubernetes Helm charts / Terraform scripts (external infrastructure concerns)
- Distributed consensus engines (Raft, etcd)

## 4. Prerequisites

- Phases 1 through 9 complete and passing
- Docker and Docker Compose installed
- JDK 21 and Maven 3.8+

## 5. Read Before Starting

- `docs/requirements/configuration.md` — Configuration requirements
- `docs/architecture/project-structure.md` — Final package organization
- `docs/architecture/system-overview.md` — Complete system architecture
- 12-Factor App methodology: [Config](https://12factor.net/config) and [Disposability](https://12factor.net/disposability)

## 6. Concepts to Understand

- **12-Factor App Configuration:** Storing config in the environment (`JOBSTREAM_REDIS_HOST`, `JOBSTREAM_WORKER_CONCURRENCY`).
- **Multi-Stage Docker Builds:** Separating build-time JDK dependencies from minimal runtime JRE images to reduce image size and attack surface.
- **Graceful Shutdown & Signal Trapping:** Intercepting POSIX `SIGTERM` (Docker stop) and giving workers a deadline (e.g. 30 seconds) to complete tasks before forceful termination.
  - Reference: `Runtime.getRuntime().addShutdownHook(...)`
- **JVM in Containers:** Memory and CPU cgroup awareness in modern JDKs (`-XX:MaxRAMPercentage`, `-XX:+UseContainerSupport`).

## 7. Design Decisions

### 7.1 Standalone Packaging Tool

**What to decide:** How to package JobStream into an executable standalone artifact?

**Options:**
- `maven-shade-plugin`: Industry standard for executable uber-jars with shaded dependencies.
- `maven-assembly-plugin`: Creates zip/tar.gz with lib directory.
- `Jib`: Google container tool building images directly without Docker daemon.

**Recommendation:** Use `maven-shade-plugin` to build `target/jobstream-1.0-SNAPSHOT-shaded.jar` with `Main-Class` manifest. This produces both a local executable jar and a clean artifact for Dockerfile consumption.

**Record this decision in:** ADR-012 (to be created during this phase).

### 7.2 Configuration Framework

**What to decide:** Pure Java environment/property lookup vs external library (Typesafe Config / SnakeYAML)?

**Recommendation:** Build a robust `ConfigLoader` in `config` that inspects:
1. Environment variables (`JOBSTREAM_*`)
2. System properties (`-Djobstream.*`)
3. Classpath defaults (`jobstream.properties`)
This achieves zero added dependencies while adhering strictly to 12-Factor principles.

**Record this decision in:** ADR-012.

## 8. Requirements

### Functional Requirements

1. **`JobStreamConfig`:**
   - Redis: host, port, password, pool size, timeout.
   - Worker: concurrency, queue names, heartbeat interval, shutdown timeout.
   - API: port, host.
   - Retry: default max retries, base backoff duration.
   - Validation: Fails fast on invalid port ranges or negative timeouts.

2. **Unified Entry Point (`Main.java`):**
   - Refactor `Main.java` to support running modes based on CLI arguments:
     - `server` — Launches `JobStreamApiServer` and background scheduler poller.
     - `worker` — Launches worker thread pool polling configured queues.
     - `cli <args>` — Executes CLI commands.
     - `all` (default) — Runs embedded API server and workers concurrently.

3. **Graceful Shutdown Hook:**
   - Registered on startup in `Main.java`.
   - On `SIGTERM`/`SIGINT`: stops accepting new jobs, signals workers, awaits termination up to `shutdownTimeout`, stops API server, closes Jedis pool cleanly.

4. **Multi-Stage `Dockerfile`:**
   - Stage 1 (Builder): Maven + JDK 21 compiles and builds shaded jar.
   - Stage 2 (Runner): `eclipse-temurin:21-jre-alpine` running as non-root user.
   - Exposes port 8080.
   - Healthcheck querying `/health`.

5. **`docker-compose.yml`:**
   - Services: `redis`, `jobstream-server` (API), `jobstream-worker-1`, `jobstream-worker-2`.
   - Health checks, network bridge, volume persistence for Redis.

### Non-Functional Requirements

- Docker image size < 250 MB.
- Graceful shutdown completes without dropping in-progress tasks within timeout.
- Clean shutdown on `docker compose down`.

## 9. File Change Manifest

### CREATE

```
src/main/java/io/github/pandeyayushk/jobstream/config/JobStreamConfig.java
src/main/java/io/github/pandeyayushk/jobstream/config/ConfigLoader.java
src/main/resources/jobstream.properties
Dockerfile
docker-compose.yml
.dockerignore
src/test/java/io/github/pandeyayushk/jobstream/config/ConfigLoaderTest.java
```

### MODIFY

```
pom.xml                             — Add maven-shade-plugin configuration
src/main/java/.../Main.java         — Refactor into production entry point with modes and shutdown hooks
src/test/java/.../MainTest.java     — Update test for production Main entry point
```

### DELETE

None.

### DO NOT TOUCH

```
src/main/java/.../job/*
src/main/java/.../persistence/*
src/main/java/.../queue/*
src/main/java/.../executor/*
```

## 10. Implementation Order

1. **`ConfigLoader.java`** & **`JobStreamConfig.java`** — Environment/property parser with validation.
2. **`ConfigLoaderTest.java`** — Unit tests verifying environment overrides and defaults.
3. **`jobstream.properties`** — Default configuration file in resources.
4. **`pom.xml`** — Configure `maven-shade-plugin` with `Main` manifest.
5. **`Main.java`** — Wire configuration, mode dispatch (`server`, `worker`, `all`), and shutdown hook.
6. **`Dockerfile`** & **`.dockerignore`** — Multi-stage container build.
7. **`docker-compose.yml`** — Compose cluster topology.

## 11. Testing Requirements

### Unit Tests
- `ConfigLoaderTest`:
  - Defaults match expected values when no env vars provided.
  - Environment variable overrides take precedence over properties file.
  - Invalid configuration values (e.g. negative port) throw validation errors.

### Integration & Operational Verification
- **Shaded Jar Execution:**
  - Build shaded jar (`mvn package`).
  - Run `java -jar target/jobstream-1.0-SNAPSHOT-shaded.jar --help`.
- **Graceful Shutdown:**
  - Start worker processing a long-running job.
  - Send `SIGINT` (Ctrl+C).
  - Verify job finishes and status becomes `COMPLETED` before JVM terminates.
- **Docker Compose Cluster:**
  - `docker compose up --build -d`
  - Verify all containers are healthy.
  - Submit job via HTTP API; verify worker container logs processing of the job.

## 12. Failure and Edge Cases

- **Redis Unreachable on Boot:** Fail fast with clean log message before launching workers or servers.
- **SIGKILL vs SIGTERM:** Document that `kill -9` cannot run shutdown hooks (unavoidable OS constraint; addressed via heartbeat timeouts from Phase 4).
- **Worker Hangs on Shutdown:** If jobs do not complete before `shutdownTimeout`, force termination to avoid zombie processes.

## 13. Validation

```powershell
# Build fat jar
mvn clean package

# Run standalone shaded jar
java -jar target/jobstream-1.0-SNAPSHOT-shaded.jar --mode=all

# Build Docker image
docker build -t jobstream:latest .

# Run Docker Compose stack
docker compose up -d

# Verify cluster health
curl http://localhost:8080/health

# Clean up
docker compose down
```

## 14. Completion Criteria

- [ ] `JobStreamConfig` loads from environment variables and properties files with validation.
- [ ] `maven-shade-plugin` generates working standalone executable JAR.
- [ ] `Main.java` supports `server`, `worker`, and `all` execution modes.
- [ ] JVM shutdown hook cleanly drains active jobs on termination signals.
- [ ] Multi-stage `Dockerfile` produces lightweight, non-root runner image.
- [ ] `docker-compose.yml` launches functional multi-container cluster.
- [ ] Complete test suite passes (`mvn test` exits 0).

## 15. Self-Review Checklist

- [ ] Are sensitive configuration credentials never printed in logs?
- [ ] Does Docker image run as a non-root user?
- [ ] Does `.dockerignore` prevent leaking `.git`, `target`, and IDE folders into build context?
- [ ] Is shutdown timeout bounded to prevent indefinite hangs?
- [ ] Is root `README.md` synthesized with complete production instructions?

## 16. Documentation Updates

### Requirements documentation
- Update `docs/requirements/configuration.md` with final environment variable list.

### Architecture documentation
- Finalize `docs/architecture/project-structure.md` and `docs/architecture/system-overview.md`.

### ADRs
- **Create ADR-012:** Packaging, 12-Factor configuration, and deployment architecture.

### Final README Notes
- Synthesize all accumulated notes into the final root `README.md`:
  - Quickstart with Docker Compose.
  - Complete architecture diagrams.
  - CLI and REST API reference.
  - Configuration options table.

## 17. Git Milestone

- **Branch:** `phase-10-deployment-and-hardening`
- **Base:** `master` (after Phase 9 merge)
- **Implementation milestone:** Docker, Compose, packaging, configuration, and shutdown hooks complete.
- **Testing milestone:** Verified multi-container topology processing jobs via Docker Compose.
- **Review milestone:** ADR-012 written, root README finalized.
- **Merge condition:** Clean build, passing tests, and verified container run.

## 18. What This Phase Enables

With Phase 10 complete:
- The entire JobStream distributed job queue system is fully designed, documented, and ready for incremental, independent implementation.
- Any engineer can execute the roadmap from Phase 1 to Phase 10 following the self-guided documentation.
