package io.github.pandeyayushk.jobstream.retry;

import io.github.pandeyayushk.jobstream.executor.DefaultExecutorRegistry;
import io.github.pandeyayushk.jobstream.executor.ExecutionResult;
import io.github.pandeyayushk.jobstream.job.Job;
import io.github.pandeyayushk.jobstream.job.JobId;
import io.github.pandeyayushk.jobstream.job.JobStatus;
import io.github.pandeyayushk.jobstream.payload.Payload;
import io.github.pandeyayushk.jobstream.persistence.RedisJobRepository;
import io.github.pandeyayushk.jobstream.queue.RedisJobQueue;
import io.github.pandeyayushk.jobstream.serialization.JacksonJobSerializer;
import io.github.pandeyayushk.jobstream.serialization.JobSerializer;
import io.github.pandeyayushk.jobstream.worker.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.RedisClient;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class WorkerRetryIntegrationTest {

    private static final String QUEUE_NAME = "default";

    private static final Duration STATUS_TIMEOUT =
            Duration.ofSeconds(3);

    private static final Duration POLL_INTERVAL =
            Duration.ofMillis(10);

    private RedisClient client;
    private RedisJobRepository repository;
    private RedisJobQueue queue;
    private RedisWorkerRegistry workerRegistry;
    private DefaultExecutorRegistry executorRegistry;
    private DeadLetterQueue deadLetterQueue;

    private Worker worker;

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

        deadLetterQueue =
                new RedisDeadLetterQueue(
                        client,
                        serializer
                );
    }

    @AfterEach
    void tearDown() {
        try {
            if (worker != null
                    && worker.status() != WorkerStatus.STOPPED) {

                worker.stop();
            }

            if (client != null) {
                client.flushDB();
            }
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }

    @Test
    void failedJobIsRetriedAndEventuallyCompletes()
            throws Exception {

        AtomicInteger executionCount =
                new AtomicInteger();

        Job job =
                createQueuedJob(
                        "retry-job",
                        2
                );

        repository.save(job);

        queue.enqueue(
                job.id(),
                QUEUE_NAME
        );

        executorRegistry.register(
                "retry-job",
                currentJob -> {

                    int attempt =
                            executionCount.incrementAndGet();

                    if (attempt == 1) {
                        return ExecutionResult.failure(
                                new IllegalStateException(
                                        "transient failure"
                                )
                        );
                    }

                    return ExecutionResult.success();
                }
        );

        RetryPolicy retryPolicy =
                new FixedDelayRetryPolicy(
                        Duration.ofMillis(250)
                );

        worker =
                createWorker(
                        retryPolicy
                );

        worker.start();

        waitForStatus(
                job.id(),
                JobStatus.RETRYING
        );

        Job retryingJob =
                findJob(job.id());

        assertEquals(
                JobStatus.RETRYING,
                retryingJob.status()
        );

        assertEquals(
                1,
                retryingJob.retryCount()
        );

        assertEquals(
                Optional.of("transient failure"),
                retryingJob.lastErrorReason()
        );

        waitForStatus(
                job.id(),
                JobStatus.COMPLETED
        );

        assertEquals(
                JobStatus.COMPLETED,
                findJob(job.id()).status()
        );

        assertEquals(
                2,
                executionCount.get()
        );

        assertEquals(
                0,
                deadLetterQueue.size()
        );
    }

    @Test
    void retryDoesNotBlockWorkerFromProcessingAnotherJob()
            throws Exception {

        AtomicInteger retryingJobExecutions =
                new AtomicInteger();

        AtomicInteger successfulJobExecutions =
                new AtomicInteger();

        Job retryingJob =
                createQueuedJob(
                        "retry-job",
                        2
                );

        Job successfulJob =
                createQueuedJob(
                        "successful-job",
                        2
                );

        repository.save(retryingJob);
        repository.save(successfulJob);

        queue.enqueue(
                retryingJob.id(),
                QUEUE_NAME
        );

        queue.enqueue(
                successfulJob.id(),
                QUEUE_NAME
        );

        executorRegistry.register(
                "retry-job",
                currentJob -> {

                    int attempt =
                            retryingJobExecutions.incrementAndGet();

                    if (attempt == 1) {
                        return ExecutionResult.failure(
                                new IllegalStateException(
                                        "temporary failure"
                                )
                        );
                    }

                    return ExecutionResult.success();
                }
        );

        executorRegistry.register(
                "successful-job",
                currentJob -> {
                    successfulJobExecutions.incrementAndGet();

                    return ExecutionResult.success();
                }
        );

        /*
         * 250ms is long enough to prove that the retry
         * is scheduled asynchronously.
         *
         * It is deliberately short so the test suite
         * does not waste seconds waiting for a retry.
         */
        RetryPolicy retryPolicy =
                new FixedDelayRetryPolicy(
                        Duration.ofMillis(250)
                );

        worker =
                createWorker(
                        retryPolicy
                );

        worker.start();

        /*
         * Wait until the first job has failed and entered
         * RETRYING.
         */
        waitForStatus(
                retryingJob.id(),
                JobStatus.RETRYING
        );

        assertEquals(
                1,
                retryingJobExecutions.get()
        );

        /*
         * The second job must be processed while the first
         * job is waiting for its retry.
         */
        waitForStatus(
                successfulJob.id(),
                JobStatus.COMPLETED
        );

        assertEquals(
                1,
                successfulJobExecutions.get()
        );

        /*
         * Eventually the scheduled retry executes.
         */
        waitForStatus(
                retryingJob.id(),
                JobStatus.COMPLETED
        );

        assertEquals(
                2,
                retryingJobExecutions.get()
        );
    }

    @Test
    void exhaustedRetriesMoveJobToDeadLetterQueue()
            throws Exception {

        AtomicInteger executionCount =
                new AtomicInteger();

        Job job =
                createQueuedJob(
                        "failing-job",
                        2
                );

        repository.save(job);

        queue.enqueue(
                job.id(),
                QUEUE_NAME
        );

        executorRegistry.register(
                "failing-job",
                currentJob -> {

                    executionCount.incrementAndGet();

                    return ExecutionResult.failure(
                            new IllegalStateException(
                                    "permanent failure"
                            )
                    );
                }
        );

        RetryPolicy retryPolicy =
                new FixedDelayRetryPolicy(
                        Duration.ZERO
                );

        worker =
                createWorker(
                        retryPolicy
                );

        worker.start();

        waitForStatus(
                job.id(),
                JobStatus.DEAD
        );

        Job deadJob =
                findJob(job.id());

        assertEquals(
                JobStatus.DEAD,
                deadJob.status()
        );

        /*
         * Current Job semantics:
         *
         * initial attempt
         * retry 1
         * retry 2
         *
         * Therefore, maxRetries=2 produces three
         * executions before DEAD.
         */
        assertEquals(
                3,
                executionCount.get()
        );

        assertEquals(
                1,
                deadLetterQueue.size()
        );

        List<Job> deadJobs =
                deadLetterQueue.listDeadJobs(
                        0,
                        10
                );

        assertEquals(
                1,
                deadJobs.size()
        );

        assertEquals(
                job.id(),
                deadJobs.get(0).id()
        );

        assertEquals(
                JobStatus.DEAD,
                deadJobs.get(0).status()
        );

        assertEquals(
                Optional.of("permanent failure"),
                Optional.ofNullable(
                        deadJobs.get(0)
                                .metadata()
                                .get("failure.reason")
                )
        );

        assertEquals(
                0,
                queue.size(QUEUE_NAME)
        );
    }

    @Test
    void deadLetterJobCanBeManuallyRequeued()
            throws Exception {

        AtomicInteger executionCount =
                new AtomicInteger();

        Job job =
                createQueuedJob(
                        "manual-requeue-job",
                        1
                );

        repository.save(job);

        queue.enqueue(
                job.id(),
                QUEUE_NAME
        );

        executorRegistry.register(
                "manual-requeue-job",
                currentJob -> {

                    int attempt =
                            executionCount.incrementAndGet();

                    if (attempt == 1) {
                        return ExecutionResult.failure(
                                new IllegalStateException(
                                        "first execution failed"
                                )
                        );
                    }

                    return ExecutionResult.success();
                }
        );

        /*
         * No automatic retry.
         *
         * The first failure goes directly to the DLQ.
         */
        RetryPolicy retryPolicy =
                new NoRetryPolicy();

        worker =
                createWorker(
                        retryPolicy
                );

        worker.start();

        waitForStatus(
                job.id(),
                JobStatus.DEAD
        );

        assertEquals(
                1,
                deadLetterQueue.size()
        );

        worker.stop();

        Optional<Job> requeued =
                deadLetterQueue.requeue(
                        job.id(),
                        QUEUE_NAME
                );

        assertTrue(
                requeued.isPresent()
        );

        assertEquals(
                JobStatus.QUEUED,
                requeued.orElseThrow().status()
        );

        assertEquals(
                0,
                requeued.orElseThrow().retryCount()
        );

        assertEquals(
                0,
                deadLetterQueue.size()
        );

        assertEquals(
                1,
                queue.size(QUEUE_NAME)
        );

        /*
         * Start a new worker and process the manually
         * requeued job.
         */
        worker =
                createWorker(
                        retryPolicy
                );

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
                2,
                executionCount.get()
        );
    }

    @Test
    void retryStatePreservesFailureDiagnostics()
            throws Exception {

        Job job =
                createQueuedJob(
                        "diagnostic-job",
                        1
                );

        repository.save(job);

        queue.enqueue(
                job.id(),
                QUEUE_NAME
        );

        executorRegistry.register(
                "diagnostic-job",
                currentJob ->
                        ExecutionResult.failure(
                                new IllegalArgumentException(
                                        "invalid payload"
                                )
                        )
        );

        RetryPolicy retryPolicy =
                new NoRetryPolicy();

        worker =
                createWorker(
                        retryPolicy
                );

        worker.start();

        waitForStatus(
                job.id(),
                JobStatus.DEAD
        );

        Job deadJob =
                findJob(job.id());

        assertEquals(
                JobStatus.DEAD,
                deadJob.status()
        );

        assertEquals(
                Optional.of("invalid payload"),
                Optional.ofNullable(
                        deadJob.metadata()
                                .get("failure.reason")
                )
        );
    }

    @Test
    void zeroMaxRetriesIsRejected() {

        Job base =
                Job.create(
                        "invalid-job",
                        Payload.empty()
                );

        assertThrows(
                IllegalArgumentException.class,
                () -> Job.reconstitute(
                        base.id(),
                        base.type(),
                        JobStatus.QUEUED,
                        base.payload(),
                        base.createdAt(),
                        base.updatedAt(),
                        base.metadata(),
                        0,
                        0,
                        null,
                        null
                )
        );
    }

    @Test
    void negativeMaxRetriesIsRejected() {

        Job base =
                Job.create(
                        "invalid-job",
                        Payload.empty()
                );

        assertThrows(
                IllegalArgumentException.class,
                () -> Job.reconstitute(
                        base.id(),
                        base.type(),
                        JobStatus.QUEUED,
                        base.payload(),
                        base.createdAt(),
                        base.updatedAt(),
                        base.metadata(),
                        0,
                        -1,
                        null,
                        null
                )
        );
    }

    @Test
    void requeueMissingDeadLetterJobReturnsEmpty() {

        Optional<Job> result =
                deadLetterQueue.requeue(
                        JobId.generate(),
                        QUEUE_NAME
                );

        assertTrue(
                result.isEmpty()
        );

        assertEquals(
                0,
                deadLetterQueue.size()
        );
    }

    private Worker createWorker(
            RetryPolicy retryPolicy
    ) {

        worker =
                new Worker(
                        WorkerId.generate(),
                        queue,
                        repository,
                        workerRegistry,
                        executorRegistry,
                        retryPolicy,
                        deadLetterQueue,
                        new WorkerConfig(
                                QUEUE_NAME,
                                1,
                                Duration.ofSeconds(1),
                                Duration.ofSeconds(3),
                                Duration.ofSeconds(1)
                        )
                );

        return worker;
    }

    private Job createQueuedJob(
            String type,
            int maxRetries
    ) {

        Job base =
                Job.create(
                        type,
                        Payload.empty()
                );

        return Job.reconstitute(
                base.id(),
                base.type(),
                JobStatus.QUEUED,
                base.payload(),
                base.createdAt(),
                base.updatedAt(),
                base.metadata(),
                0,
                maxRetries,
                null,
                null
        );
    }

    private Job findJob(
            JobId jobId
    ) {

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
                System.currentTimeMillis()
                        + STATUS_TIMEOUT.toMillis();

        while (
                System.currentTimeMillis()
                        < deadline
        ) {

            Optional<Job> job =
                    repository.findById(jobId);

            if (
                    job.isPresent()
                            && job.get().status()
                            == expectedStatus
            ) {
                return;
            }

            Thread.sleep(
                    POLL_INTERVAL.toMillis()
            );
        }

        fail(
                "Job " + jobId
                        + " did not reach status "
                        + expectedStatus
                        + ". Current status: "
                        + repository.findById(jobId)
                        .map(Job::status)
                        .orElse(null)
        );
    }
}