package io.github.pandeyayushk.jobstream.worker;

import io.github.pandeyayushk.jobstream.executor.DefaultExecutorRegistry;
import io.github.pandeyayushk.jobstream.executor.ExecutionResult;
import io.github.pandeyayushk.jobstream.job.Job;
import io.github.pandeyayushk.jobstream.job.JobId;
import io.github.pandeyayushk.jobstream.job.JobStatus;
import io.github.pandeyayushk.jobstream.payload.Payload;
import io.github.pandeyayushk.jobstream.persistence.RedisJobRepository;
import io.github.pandeyayushk.jobstream.queue.QueueCoordinator;
import io.github.pandeyayushk.jobstream.queue.QueueCoordinatorImp;
import io.github.pandeyayushk.jobstream.queue.RedisJobQueue;
import io.github.pandeyayushk.jobstream.queue.RedisJobSubmissionStore;
import io.github.pandeyayushk.jobstream.queue.RedisPriorityQueueStore;
import io.github.pandeyayushk.jobstream.retry.DeadLetterQueue;
import io.github.pandeyayushk.jobstream.retry.FixedDelayRetryPolicy;
import io.github.pandeyayushk.jobstream.retry.RedisDeadLetterQueue;
import io.github.pandeyayushk.jobstream.serialization.JacksonJobSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.RedisClient;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class PriorityWorkerIntegrationTest {

    private RedisClient client;
    private RedisJobRepository repository;
    private RedisJobQueue queue;
    private RedisWorkerRegistry workerRegistry;
    private DefaultExecutorRegistry executors;
    private DeadLetterQueue deadLetterQueue;
    private QueueCoordinator coordinator;
    private String queueName;
    private final List<JobId> jobIds = new CopyOnWriteArrayList<>();
    private Worker worker;

    @BeforeEach
    void setUp() {
        client = RedisClient.create("redis://localhost:6379");
        queueName = "worker-priority-" + UUID.randomUUID();
        JacksonJobSerializer serializer = new JacksonJobSerializer();
        repository = new RedisJobRepository(client, serializer);
        queue = new RedisJobQueue(client);
        workerRegistry = new RedisWorkerRegistry(client, Duration.ofSeconds(5));
        executors = new DefaultExecutorRegistry();
        deadLetterQueue = new RedisDeadLetterQueue(client, serializer);
        coordinator = new QueueCoordinatorImp(new RedisJobSubmissionStore(client, serializer));
    }

    @AfterEach
    void tearDown() {
        if (worker != null && worker.status() != WorkerStatus.STOPPED) {
            worker.stop();
        }
        client.del(
                RedisPriorityQueueStore.legacyKey(queueName),
                RedisPriorityQueueStore.queueKey(queueName),
                RedisPriorityQueueStore.sequenceKey(queueName),
                RedisPriorityQueueStore.membersKey(queueName)
        );
        for (JobId jobId : jobIds) {
            client.del("jobstream:job:" + jobId);
            for (JobStatus status : JobStatus.values()) {
                client.srem("jobstream:status:" + status, jobId.toString());
            }
        }
        client.close();
    }

    @Test
    void workerExecutesSubmissionsInPriorityOrder() throws Exception {
        Job low = submit("low", -1);
        Job high = submit("high", 20);
        Job medium = submit("medium", 4);
        List<String> executed = new CopyOnWriteArrayList<>();
        CountDownLatch finished = new CountDownLatch(3);

        executors.register("priority-task", job -> {
            executed.add((String) job.payload().asMap().get("name"));
            finished.countDown();
            return ExecutionResult.success();
        });
        startWorker(new FixedDelayRetryPolicy(Duration.ZERO));

        assertTrue(finished.await(5, TimeUnit.SECONDS));
        assertEquals(List.of("high", "medium", "low"), executed);
        assertEquals(JobStatus.COMPLETED, repository.findById(high.id()).orElseThrow().status());
    }

    @Test
    void retryRequeueKeepsPriorityAheadOfLowerPriorityWork() throws Exception {
        Job retryJob = submit("retry", 30);
        Job blockingJob = submit("blocking", 20);
        Job lowerJob = submit("lower", 10);
        AtomicInteger retryAttempts = new AtomicInteger();
        CountDownLatch blockingStarted = new CountDownLatch(1);
        CountDownLatch releaseBlocking = new CountDownLatch(1);
        List<String> executed = new CopyOnWriteArrayList<>();
        CountDownLatch finished = new CountDownLatch(4);

        executors.register("priority-task", job -> {
            String name = (String) job.payload().asMap().get("name");
            executed.add(name);
            finished.countDown();
            if (name.equals("retry") && retryAttempts.incrementAndGet() == 1) {
                return ExecutionResult.failure("retry once");
            }
            if (name.equals("blocking")) {
                blockingStarted.countDown();
                try {
                    releaseBlocking.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return ExecutionResult.failure(e);
                }
            }
            return ExecutionResult.success();
        });

        startWorker(new FixedDelayRetryPolicy(Duration.ofMillis(300)));
        try {
            assertTrue(blockingStarted.await(5, TimeUnit.SECONDS));

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            List<JobId> waiting;
            do {
                waiting = queue.peek(queueName, 10);
                if (waiting.contains(retryJob.id())) {
                    break;
                }
                Thread.sleep(10);
            } while (System.nanoTime() < deadline);

            assertEquals(List.of(retryJob.id(), lowerJob.id()), waiting);
        } finally {
            releaseBlocking.countDown();
        }

        assertTrue(finished.await(5, TimeUnit.SECONDS));
        assertEquals(List.of("retry", "blocking", "retry", "lower"), executed);
        assertEquals(30, repository.findById(retryJob.id()).orElseThrow().priority());
        assertEquals(JobStatus.COMPLETED, repository.findById(retryJob.id()).orElseThrow().status());
    }

    private Job submit(String name, int priority) {
        Job job = Job.create(
                        "priority-task",
                        Payload.of(Map.of("name", name))
                )
                .withPriority(priority);
        jobIds.add(job.id());
        coordinator.submit(job, queueName);
        return job;
    }

    private void startWorker(FixedDelayRetryPolicy retryPolicy) {
        worker = new Worker(
                WorkerId.generate(),
                queue,
                repository,
                workerRegistry,
                executors,
                retryPolicy,
                deadLetterQueue,
                new WorkerConfig(
                        queueName,
                        1,
                        Duration.ofMillis(100),
                        Duration.ofSeconds(5),
                        Duration.ofSeconds(3)
                )
        );
        worker.start();
    }
}
