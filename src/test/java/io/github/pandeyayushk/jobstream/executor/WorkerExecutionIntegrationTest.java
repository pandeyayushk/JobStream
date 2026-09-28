package io.github.pandeyayushk.jobstream.executor;

import io.github.pandeyayushk.jobstream.job.Job;
import io.github.pandeyayushk.jobstream.job.JobId;
import io.github.pandeyayushk.jobstream.job.JobStatus;
import io.github.pandeyayushk.jobstream.payload.Payload;
import io.github.pandeyayushk.jobstream.persistence.RedisJobRepository;
import io.github.pandeyayushk.jobstream.queue.RedisJobQueue;
import io.github.pandeyayushk.jobstream.retry.DeadLetterQueue;
import io.github.pandeyayushk.jobstream.retry.NoRetryPolicy;
import io.github.pandeyayushk.jobstream.retry.RedisDeadLetterQueue;
import io.github.pandeyayushk.jobstream.retry.RetryPolicy;
import io.github.pandeyayushk.jobstream.serialization.JacksonJobSerializer;
import io.github.pandeyayushk.jobstream.serialization.JobSerializer;
import io.github.pandeyayushk.jobstream.worker.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.RedisClient;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class WorkerExecutionIntegrationTest {

    private RedisClient client;
    private RedisJobRepository repository;
    private RedisJobQueue queue;
    private RedisWorkerRegistry workerRegistry;
    private DefaultExecutorRegistry executorRegistry;
    private Worker worker;
    private RetryPolicy retryPolicy;
    private DeadLetterQueue deadLetterQueue;

    @BeforeEach
    void setUp() {
        client = RedisClient.builder()
                .hostAndPort("localhost", 6379)
                .build();

        client.flushDB();

        JobSerializer serializer =
                new JacksonJobSerializer();

        repository =
                new RedisJobRepository(
                        client,
                        serializer
                );

        queue =
                new RedisJobQueue(client);

        workerRegistry =
                new RedisWorkerRegistry(
                        client,
                        Duration.ofSeconds(30)
                );

        executorRegistry =
                new DefaultExecutorRegistry();

        /*
         * These tests are validating the Worker execution engine,
         * not automatic retry behavior.
         *
         * NoRetryPolicy means an execution failure is routed to
         * the Phase 6 Dead-Letter Queue.
         */
        retryPolicy =
                new NoRetryPolicy();

        deadLetterQueue =
                new RedisDeadLetterQueue(
                        client,
                        serializer
                );
    }

    @AfterEach
    void tearDown() {
        if (worker != null
                && worker.status() != WorkerStatus.STOPPED) {
            worker.stop();
        }

        client.flushDB();
    }

    @Test
    void successfulExecutionCompletesJob()
            throws Exception {

        Job job =
                queuedJob("test-job");

        repository.save(job);

        queue.enqueue(
                job.id(),
                "default"
        );

        executorRegistry.register(
                "test-job",
                new NoOpExecutor()
        );

        worker = createWorker();

        worker.start();

        waitForStatus(
                job.id(),
                JobStatus.COMPLETED
        );

        assertEquals(
                JobStatus.COMPLETED,
                findJob(job.id()).status()
        );

        assertEquals(
                WorkerStatus.RUNNING,
                worker.status()
        );
    }

    @Test
    void executorFailureMovesJobToDeadLetterQueue()
            throws Exception {

        Job job =
                queuedJob("test-job");

        repository.save(job);

        queue.enqueue(
                job.id(),
                "default"
        );

        executorRegistry.register(
                "test-job",
                new FailingExecutor(
                        "execution failed"
                )
        );

        worker = createWorker();

        worker.start();

        waitForStatus(
                job.id(),
                JobStatus.DEAD
        );

        assertEquals(
                JobStatus.DEAD,
                findJob(job.id()).status()
        );

        assertEquals(
                1,
                deadLetterQueue.size()
        );

        assertEquals(
                WorkerStatus.RUNNING,
                worker.status()
        );
    }

    @Test
    void executorExceptionMovesJobToDeadLetterQueueAndWorkerContinues()
            throws Exception {

        Job failingJob =
                queuedJob("throwing-job");

        Job successfulJob =
                queuedJob("successful-job");

        repository.save(failingJob);
        repository.save(successfulJob);

        queue.enqueue(
                failingJob.id(),
                "default"
        );

        queue.enqueue(
                successfulJob.id(),
                "default"
        );

        executorRegistry.register(
                "throwing-job",
                job -> {
                    throw new IllegalStateException(
                            "executor crashed"
                    );
                }
        );

        executorRegistry.register(
                "successful-job",
                new NoOpExecutor()
        );

        worker = createWorker();

        worker.start();

        waitForStatus(
                failingJob.id(),
                JobStatus.DEAD
        );

        waitForStatus(
                successfulJob.id(),
                JobStatus.COMPLETED
        );

        assertEquals(
                JobStatus.DEAD,
                findJob(failingJob.id()).status()
        );

        assertEquals(
                JobStatus.COMPLETED,
                findJob(successfulJob.id()).status()
        );

        assertEquals(
                WorkerStatus.RUNNING,
                worker.status()
        );

        assertEquals(
                1,
                deadLetterQueue.size()
        );
    }

    @Test
    void missingExecutorMovesJobToDeadLetterQueue()
            throws Exception {

        Job job =
                queuedJob("missing-executor");

        repository.save(job);

        queue.enqueue(
                job.id(),
                "default"
        );

        worker = createWorker();

        worker.start();

        waitForStatus(
                job.id(),
                JobStatus.DEAD
        );

        assertEquals(
                JobStatus.DEAD,
                findJob(job.id()).status()
        );

        assertEquals(
                1,
                deadLetterQueue.size()
        );

        assertEquals(
                WorkerStatus.RUNNING,
                worker.status()
        );
    }

    @Test
    void differentJobTypesUseDifferentExecutors()
            throws Exception {

        Job successJob =
                queuedJob("success-job");

        Job failureJob =
                queuedJob("failure-job");

        repository.save(successJob);
        repository.save(failureJob);

        queue.enqueue(
                successJob.id(),
                "default"
        );

        queue.enqueue(
                failureJob.id(),
                "default"
        );

        AtomicInteger successExecutions =
                new AtomicInteger();

        AtomicInteger failureExecutions =
                new AtomicInteger();

        executorRegistry.register(
                "success-job",
                job -> {
                    successExecutions.incrementAndGet();
                    return ExecutionResult.success();
                }
        );

        executorRegistry.register(
                "failure-job",
                job -> {
                    failureExecutions.incrementAndGet();

                    return ExecutionResult.failure(
                            "expected failure"
                    );
                }
        );

        worker = createWorker();

        worker.start();

        waitForStatus(
                successJob.id(),
                JobStatus.COMPLETED
        );

        waitForStatus(
                failureJob.id(),
                JobStatus.DEAD
        );

        assertEquals(
                JobStatus.COMPLETED,
                findJob(successJob.id()).status()
        );

        assertEquals(
                JobStatus.DEAD,
                findJob(failureJob.id()).status()
        );

        assertEquals(
                1,
                successExecutions.get()
        );

        assertEquals(
                1,
                failureExecutions.get()
        );

        assertEquals(
                1,
                deadLetterQueue.size()
        );
    }

    @Test
    void workerContinuesAfterFailedJob()
            throws Exception {

        Job firstJob =
                queuedJob("test-job");

        Job secondJob =
                queuedJob("test-job");

        repository.save(firstJob);
        repository.save(secondJob);

        queue.enqueue(
                firstJob.id(),
                "default"
        );

        queue.enqueue(
                secondJob.id(),
                "default"
        );

        executorRegistry.register(
                "test-job",
                job -> {
                    if (job.id().equals(firstJob.id())) {
                        return ExecutionResult.failure(
                                "first execution failed"
                        );
                    }

                    return ExecutionResult.success();
                }
        );

        worker = createWorker();

        worker.start();

        waitForStatus(
                firstJob.id(),
                JobStatus.DEAD
        );

        waitForStatus(
                secondJob.id(),
                JobStatus.COMPLETED
        );

        assertEquals(
                JobStatus.DEAD,
                findJob(firstJob.id()).status()
        );

        assertEquals(
                JobStatus.COMPLETED,
                findJob(secondJob.id()).status()
        );

        assertEquals(
                WorkerStatus.RUNNING,
                worker.status()
        );

        assertEquals(
                1,
                deadLetterQueue.size()
        );
    }

    @Test
    void executorFailurePersistsFailureReason()
            throws Exception {

        Job job =
                queuedJob("test-job");

        repository.save(job);

        queue.enqueue(
                job.id(),
                "default"
        );

        executorRegistry.register(
                "test-job",
                new FailingExecutor(
                        "intentional executor failure"
                )
        );

        worker = createWorker();

        worker.start();

        waitForStatus(
                job.id(),
                JobStatus.DEAD
        );

        Job deadJob =
                repository.findById(job.id())
                        .orElseThrow();

        assertEquals(
                JobStatus.DEAD,
                deadJob.status()
        );

        assertEquals(
                "intentional executor failure",
                deadJob.metadata()
                        .get("failure.reason")
        );

        assertEquals(
                1,
                deadLetterQueue.size()
        );
    }

    private Worker createWorker() {
        WorkerConfig config =
                new WorkerConfig(
                        "default",
                        2,
                        Duration.ofMillis(100),
                        Duration.ofSeconds(5),
                        Duration.ofSeconds(3)
                );

        return new Worker(
                WorkerId.generate(),
                queue,
                repository,
                workerRegistry,
                executorRegistry,
                retryPolicy,
                deadLetterQueue,
                config
        );
    }

    private Job queuedJob(String type) {
        return Job.create(
                type,
                Payload.empty()
        ).withStatus(
                JobStatus.QUEUED
        );
    }

    private Job findJob(JobId jobId) {
        return repository.findById(jobId)
                .orElseThrow(
                        () -> new AssertionError(
                                "Job not found: " + jobId
                        )
                );
    }

    private void waitForStatus(
            JobId jobId,
            JobStatus expectedStatus
    ) throws InterruptedException {

        long deadline =
                System.currentTimeMillis() + 5000;

        while (System.currentTimeMillis() < deadline) {

            Optional<Job> job =
                    repository.findById(jobId);

            if (job.isPresent()
                    && job.get().status() == expectedStatus) {
                return;
            }

            Thread.sleep(20);
        }

        Job actual =
                repository.findById(jobId)
                        .orElse(null);

        fail(
                "Job "
                        + jobId
                        + " did not reach status "
                        + expectedStatus
                        + ". Actual status: "
                        + (actual == null
                        ? "MISSING"
                        : actual.status())
        );
    }
}