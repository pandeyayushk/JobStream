package io.github.pandeyayushk.jobstream.queue;

import io.github.pandeyayushk.jobstream.job.Job;
import io.github.pandeyayushk.jobstream.job.JobId;
import io.github.pandeyayushk.jobstream.job.JobStatus;
import io.github.pandeyayushk.jobstream.payload.Payload;
import io.github.pandeyayushk.jobstream.persistence.RedisJobRepository;
import io.github.pandeyayushk.jobstream.serialization.JacksonJobSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.RedisClient;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

class RedisPriorityQueueIntegrationTest {

    private RedisClient client;
    private JacksonJobSerializer serializer;
    private RedisJobRepository repository;
    private RedisJobQueue queue;
    private QueueCoordinator coordinator;
    private String queuePrefix;
    private final ConcurrentLinkedQueue<JobId> createdJobIds = new ConcurrentLinkedQueue<>();

    @BeforeEach
    void setUp() {
        client = RedisClient.create("redis://localhost:6379");
        serializer = new JacksonJobSerializer();
        repository = new RedisJobRepository(client, serializer);
        queue = new RedisJobQueue(client);
        coordinator = new QueueCoordinatorImp(
                new RedisJobSubmissionStore(client, serializer)
        );
        queuePrefix = "priority-test-" + UUID.randomUUID();
    }

    @AfterEach
    void tearDown() {
        for (String queueName : List.of(queuePrefix, queuePrefix + "-a", queuePrefix + "-b")) {
            client.srem(RedisQueueRegistry.REDIS_KEY, queueName);
            client.del(
                    RedisPriorityQueueStore.legacyKey(queueName),
                    RedisPriorityQueueStore.queueKey(queueName),
                    RedisPriorityQueueStore.sequenceKey(queueName),
                    RedisPriorityQueueStore.membersKey(queueName)
            );
        }
        for (JobId jobId : createdJobIds) {
            client.del("jobstream:job:" + jobId);
            for (JobStatus status : JobStatus.values()) {
                client.srem("jobstream:status:" + status, jobId.toString());
            }
        }
        client.close();
    }

    @Test
    void higherPriorityDequeuesBeforeLowerPriority() {
        Job low = submit(queuePrefix, -2);
        Job medium = submit(queuePrefix, 0);
        Job high = submit(queuePrefix, 8);

        assertEquals(List.of(high.id(), medium.id(), low.id()), queue.peek(queuePrefix, 10));
        assertEquals(high.id(), queue.dequeueNonBlocking(queuePrefix).orElseThrow());
        assertEquals(medium.id(), queue.dequeueNonBlocking(queuePrefix).orElseThrow());
        assertEquals(low.id(), queue.dequeueNonBlocking(queuePrefix).orElseThrow());
        assertTrue(queue.dequeueNonBlocking(queuePrefix).isEmpty());
    }

    @Test
    void equalPrioritiesRetainFifoSubmissionOrder() {
        Job first = submit(queuePrefix, 5);
        Job second = submit(queuePrefix, 5);
        Job third = submit(queuePrefix, 5);

        assertEquals(List.of(first.id(), second.id(), third.id()), queue.peek(queuePrefix, 10));
    }

    @Test
    void negativeAndIntegerBoundaryPrioritiesOrderCorrectly() {
        Job minimum = submit(queuePrefix, Integer.MIN_VALUE);
        Job negative = submit(queuePrefix, -1);
        Job zero = submit(queuePrefix, 0);
        Job maximum = submit(queuePrefix, Integer.MAX_VALUE);

        assertEquals(
                List.of(maximum.id(), zero.id(), negative.id(), minimum.id()),
                queue.peek(queuePrefix, 10)
        );
    }

    @Test
    void namedQueuesKeepIndependentOrdering() {
        String queueA = queuePrefix + "-a";
        String queueB = queuePrefix + "-b";
        Job aHigh = submit(queueA, 10);
        Job aLow = submit(queueA, 1);
        Job bHigh = submit(queueB, 2);
        Job bLow = submit(queueB, -1);

        assertEquals(List.of(aHigh.id(), aLow.id()), queue.peek(queueA, 10));
        assertEquals(List.of(bHigh.id(), bLow.id()), queue.peek(queueB, 10));
    }

    @Test
    void concurrentSubmissionsRemainOrderedAndUnique() throws Exception {
        int count = 120;
        ExecutorService executor = Executors.newFixedThreadPool(8);
        ConcurrentLinkedQueue<Job> submitted = new ConcurrentLinkedQueue<>();
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < count; i++) {
                int priority = (i % 11) - 5;
                futures.add(executor.submit(() -> submitted.add(submit(queuePrefix, priority))));
            }
            for (Future<?> future : futures) {
                future.get();
            }
        } finally {
            executor.shutdownNow();
        }

        List<JobId> ordered = queue.peek(queuePrefix, count);
        assertEquals(count, ordered.size());
        assertEquals(count, new HashSet<>(ordered).size());
        assertEquals(new HashSet<>(submitted.stream().map(Job::id).toList()), new HashSet<>(ordered));

        int previousPriority = Integer.MAX_VALUE;
        for (JobId id : ordered) {
            int priority = repository.findById(id).orElseThrow().priority();
            assertTrue(priority <= previousPriority, "Priority order must be descending");
            previousPriority = priority;
        }
    }

    @Test
    void legacyFifoListIsMigratedWithoutStrandingJobs() {
        String legacyKey = RedisPriorityQueueStore.legacyKey(queuePrefix);
        Job first = persistQueued(0);
        Job second = persistQueued(0);
        Job higher = persistQueued(4);
        JobId oldFormatId = JobId.generate();
        createdJobIds.add(oldFormatId);
        client.set(
                "jobstream:job:" + oldFormatId,
                """
                        {"id":"%s","type":"legacy","status":"QUEUED","payload":{},"createdAt":"2026-01-01T00:00:00Z","updatedAt":"2026-01-01T00:00:00Z","metadata":{},"retryCount":0,"maxRetries":3,"lastErrorReason":null,"lastFailedAt":null}
                        """.formatted(oldFormatId)
        );

        client.lpush(legacyKey, first.id().toString());
        client.lpush(legacyKey, second.id().toString());
        client.lpush(legacyKey, oldFormatId.toString());
        client.lpush(legacyKey, higher.id().toString());

        assertEquals(
                List.of(higher.id(), first.id(), second.id(), oldFormatId),
                queue.peek(queuePrefix, 10)
        );
        assertFalse(client.exists(legacyKey));
        assertEquals(4, queue.size(queuePrefix));
    }

    @Test
    void enqueueOfSameJobIdCreatesOnlyOneQueueEntry() {
        Job persisted = persistQueued(3);

        queue.enqueue(persisted.id(), queuePrefix);
        queue.enqueue(persisted.id(), queuePrefix);

        assertEquals(1, queue.size(queuePrefix));
        assertEquals(List.of(persisted.id()), queue.peek(queuePrefix, 10));
    }

    @Test
    void emptyQueueOperationsReturnEmptyAndValidateInputs() {
        assertEquals(0, queue.size(queuePrefix));
        assertTrue(queue.peek(queuePrefix, 4).isEmpty());
        assertTrue(queue.dequeueNonBlocking(queuePrefix).isEmpty());
        assertThrows(IllegalArgumentException.class, () -> queue.enqueue(null, queuePrefix));
        assertThrows(IllegalArgumentException.class, () -> queue.peek(queuePrefix, -1));
        assertThrows(IllegalArgumentException.class, () -> queue.size(" "));
    }

    private Job submit(String queueName, int priority) {
        Job job = Job.create("priority-test", Payload.empty()).withPriority(priority);
        createdJobIds.add(job.id());
        coordinator.submit(job, queueName);
        return job;
    }

    private Job persistQueued(int priority) {
        Job job = Job.create("priority-test", Payload.empty())
                .withPriority(priority)
                .withStatus(JobStatus.QUEUED);
        createdJobIds.add(job.id());
        repository.save(job);
        return job;
    }
}
