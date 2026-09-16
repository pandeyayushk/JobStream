# Executor System Requirements

## 1. Purpose

Defines how domain-specific job execution logic is decoupled from worker machinery, organized, and invoked, enabling new job types to be added without modifying worker or queue infrastructure.

---

## 2. Canonical Relationship: Decoupled Strategy Pattern

The dependency between worker and executor is strictly one-way:
```
[ Worker ] ──> depends on ──> [ JobExecutor Abstraction ]
                                        │
                                        ▼
                                 [ Job / Payload ]
```
- **Rule:** The `JobExecutor` abstraction knows **nothing** about `Worker`, threads, Redis, or queues.
- An executor receives a `Job` (containing its `Payload`) and returns an `ExecutionResult`.
- The `Worker` queries the `ExecutorRegistry` for the appropriate executor and invokes it.

---

## 3. Functional Requirements

### 3.1 JobExecutor Abstraction
- Single-method interface or functional interface:
  ```java
  public interface JobExecutor {
      ExecutionResult execute(Job job);
  }
  ```
- Receives the full `Job` domain entity, providing direct access to `job.getPayload()`, `job.getId()`, and `job.getMetadata()`.
- Must be thread-safe (stateless or safely thread-safe), as multiple worker threads may invoke the same executor instance concurrently.

### 3.2 Execution Result Model (`ExecutionResult`)
- Value object representing the outcome of job execution:
  - `ExecutionResult.success()`: Execution finished successfully.
  - `ExecutionResult.failure(String reason)`: Execution encountered a business or operational failure.
  - `ExecutionResult.failure(Throwable cause)`: Execution threw an unexpected exception.
- Inspectable methods: `isSuccess()`, `getErrorMessage()`, `getErrorCause()`.

### 3.3 Executor Registry / Factory (`ExecutorRegistry`)
- Thread-safe registry mapping job type strings to `JobExecutor` instances:
  - `void register(String jobType, JobExecutor executor)`
  - `Optional<JobExecutor> get(String jobType)`
  - `boolean hasExecutor(String jobType)`
- Adheres to the **Open-Closed Principle (OCP)**: Adding new job types requires only registering a new `JobExecutor`, without altering worker loops or queue consumers.

### 3.4 Fault Containment & Error Boundary
- Worker invokes `JobExecutor.execute(job)` inside a defensive try-catch block intercepting `Throwable`.
- Any exception thrown by an executor is converted to an `ExecutionResult.failure(cause)` and never propagates out to kill the worker thread.
- Unregistered job types fail fast with status `FAILED` and an error indicating missing executor registration.

---

## 4. Non-Functional Requirements

- Zero runtime overhead for executor dispatch (O(1) map lookup).
- Zero memory leakage during execution.

---

## 5. Design Decisions (Phase 5 Ownership)

- **Registration Style:** Programmatic registration into `DefaultExecutorRegistry` vs classpath scanning. Programmatic is recommended for explicitness and testability.
- **Result Model:** `ExecutionResult` object vs throwing exceptions. Returning a typed result gives finer control over failure reasons and retry classifications.

---

## 6. Dependencies

- Domain Model (Phase 1: `Job`, `Payload`, `JobId`, `JobStatus`)
- Explicitly: **Zero dependency on Worker, Queue, or Redis**.

---

## 7. Phase Ownership

- **Phase 5 (Execution Engine):** Implements `JobExecutor`, `ExecutionResult`, `ExecutorRegistry`, and integrates them into the worker loop.
