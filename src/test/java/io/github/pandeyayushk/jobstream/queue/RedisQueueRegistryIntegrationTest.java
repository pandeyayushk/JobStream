package io.github.pandeyayushk.jobstream.queue;

import io.github.pandeyayushk.jobstream.job.Job;
import io.github.pandeyayushk.jobstream.job.JobId;
import io.github.pandeyayushk.jobstream.job.JobStatus;
import io.github.pandeyayushk.jobstream.payload.Payload;
import io.github.pandeyayushk.jobstream.persistence.RedisJobRepository;
import io.github.pandeyayushk.jobstream.retry.RedisDeadLetterQueue;
import io.github.pandeyayushk.jobstream.serialization.JacksonJobSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.RedisClient;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RedisQueueRegistryIntegrationTest {
    private RedisClient client;
    private RedisQueueRegistry registry;
    private RedisJobQueue queue;
    private RedisJobRepository repository;
    private JacksonJobSerializer serializer;
    private String prefix;
    private final List<String> queueNames = new ArrayList<>();
    private final List<JobId> jobIds = new ArrayList<>();

    @BeforeEach
    void setUp() {
        client = RedisClient.create("redis://localhost:6379");
        registry = new RedisQueueRegistry(client);
        queue = new RedisJobQueue(client);
        serializer = new JacksonJobSerializer();
        repository = new RedisJobRepository(client, serializer);
        prefix = "registry-test-" + UUID.randomUUID();
    }

    @AfterEach
    void tearDown() {
        for (String name : queueNames) {
            client.srem(RedisQueueRegistry.REDIS_KEY, name);
            client.del(RedisPriorityQueueStore.legacyKey(name),
                    RedisPriorityQueueStore.queueKey(name),
                    RedisPriorityQueueStore.sequenceKey(name),
                    RedisPriorityQueueStore.membersKey(name));
        }
        for (JobId id : jobIds) {
            client.del("jobstream:job:" + id);
            for (JobStatus status : JobStatus.values()) {
                client.srem("jobstream:status:" + status, id.toString());
            }
        }
        client.close();
    }

    @Test
    void registrationIsIdempotentSortedAndRetainedWhenQueueIsEmpty() {
        String zebra = name("zebra");
        String alpha = name("alpha");
        registry.registerQueue(zebra);
        registry.registerQueue(alpha);
        registry.registerQueue(zebra);

        List<String> registered = registry.listQueues();
        assertTrue(registered.containsAll(List.of(alpha, zebra)));
        assertEquals(registered.stream().sorted().toList(), registered);
        assertEquals(0, queue.size(alpha));
        assertEquals(registered, registry.listQueues());
        assertThrows(IllegalArgumentException.class, () -> registry.registerQueue(" "));
        assertThrows(IllegalArgumentException.class, () -> registry.registerQueue(null));
    }

    @Test
    void directEnqueueAndAtomicSubmissionRegisterTheirQueueNames() {
        String directQueue = name("direct");
        Job directJob = persist(Job.create("registry", Payload.empty())
                .withStatus(JobStatus.QUEUED));
        queue.enqueue(directJob.id(), directQueue);
        assertEquals(directJob.id(), queue.dequeueNonBlocking(directQueue).orElseThrow());
        assertTrue(registry.listQueues().contains(directQueue));

        String submittedQueue = name("submitted");
        Job submitted = Job.create("registry", Payload.empty());
        jobIds.add(submitted.id());
        new QueueCoordinatorImp(new RedisJobSubmissionStore(client, serializer))
                .submit(submitted, submittedQueue);

        assertTrue(registry.listQueues().containsAll(List.of(directQueue, submittedQueue)));
    }

    @Test
    void dlqRequeueRegistersTheRegularDestinationWithoutRegisteringTheDlq() {
        String requeueTarget = name("dlq-destination");
        boolean deadLetterNameWasRegistered = client.sismember(
                RedisQueueRegistry.REDIS_KEY, "dead-letter"
        );

        Job failed = persist(Job.create("registry", Payload.empty())
                .withStatus(JobStatus.QUEUED)
                .withStatus(JobStatus.PROCESSING)
                .withStatus(JobStatus.FAILED));
        RedisDeadLetterQueue deadLetterQueue = new RedisDeadLetterQueue(client, serializer);
        deadLetterQueue.moveToDeadLetter(failed, "test failure");
        deadLetterQueue.requeue(failed.id(), requeueTarget).orElseThrow();

        assertTrue(registry.listQueues().contains(requeueTarget));
        assertEquals(deadLetterNameWasRegistered,
                client.sismember(RedisQueueRegistry.REDIS_KEY, "dead-letter"));
    }

    @Test
    void legacyQueueMigrationRegistersNameAndPreservesPriorityOrdering() {
        String queueName = name("legacy-migration");
        Job first = persist(Job.create("registry", Payload.empty()).withStatus(JobStatus.QUEUED));
        Job higher = persist(Job.create("registry", Payload.empty()).withPriority(9)
                .withStatus(JobStatus.QUEUED));
        client.lpush(RedisPriorityQueueStore.legacyKey(queueName), first.id().toString());
        client.lpush(RedisPriorityQueueStore.legacyKey(queueName), higher.id().toString());

        assertEquals(List.of(higher.id(), first.id()), queue.peek(queueName, 10));
        assertTrue(registry.listQueues().contains(queueName));
    }

    @Test
    void backfillFindsExistingPriorityAndLegacyQueuesButSkipsUnrelatedKeysAndDlq() {
        String priorityName = name("old-priority");
        String legacyName = name("old-legacy");
        String wrongTypeName = name("wrong-type");
        String unrelatedName = name("unrelated");
        client.zadd(RedisPriorityQueueStore.queueKey(priorityName), 0, "member");
        client.lpush(RedisPriorityQueueStore.legacyKey(legacyName), "legacy-job");
        client.set(RedisPriorityQueueStore.queueKey(wrongTypeName), "not-a-zset");
        client.set(RedisPriorityQueueStore.legacyKey(unrelatedName), "not-a-list");
        long firstBackfill = registry.backfillExistingQueues();
        long repeatedBackfill = registry.backfillExistingQueues();

        assertEquals(2, firstBackfill);
        assertEquals(0, repeatedBackfill);
        assertTrue(registry.listQueues().containsAll(List.of(priorityName, legacyName)));
        assertFalse(registry.listQueues().contains(wrongTypeName));
        assertFalse(registry.listQueues().contains(unrelatedName));
    }

    @Test
    void cursorScanContinuesAfterAnEmptyBatchUntilCursorReturnsToZero() {
        AtomicInteger calls = new AtomicInteger();
        RedisQueueRegistry.ScanPageSource scanner = (cursor, pattern) -> {
            calls.incrementAndGet();
            if (RedisQueueRegistry.PRIORITY_QUEUE_PATTERN.equals(pattern)) {
                return cursor.equals("0")
                        ? new RedisQueueRegistry.ScanPage("19", List.of())
                        : new RedisQueueRegistry.ScanPage("0", List.of());
            }
            return new RedisQueueRegistry.ScanPage("0", List.of());
        };

        RedisQueueRegistry withEmptyPages = new RedisQueueRegistry(client, scanner);
        assertEquals(0, withEmptyPages.backfillExistingQueues());
        assertEquals(3, calls.get());
    }

    @Test
    void backfillExplicitlySkipsDeadLetterKey() {
        boolean registeredBefore = client.sismember(RedisQueueRegistry.REDIS_KEY, "dead-letter");
        RedisQueueRegistry.ScanPageSource scanner = (cursor, pattern) ->
                new RedisQueueRegistry.ScanPage("0", List.of("jobstream:queue:dead-letter"));

        new RedisQueueRegistry(client, scanner).backfillExistingQueues();

        assertEquals(registeredBefore,
                client.sismember(RedisQueueRegistry.REDIS_KEY, "dead-letter"));
    }

    @Test
    void concurrentSubmissionDuringBackfillIsRegisteredEvenIfScanMissesItsKey() throws Exception {
        String concurrentQueue = name("created-during-scan");
        CountDownLatch scanStarted = new CountDownLatch(1);
        CountDownLatch resumeScan = new CountDownLatch(1);
        AtomicInteger priorityScans = new AtomicInteger();
        RedisQueueRegistry.ScanPageSource scanner = (cursor, pattern) -> {
            if (pattern.equals("jobstream:priority-queue:*")
                    && priorityScans.getAndIncrement() == 0) {
                scanStarted.countDown();
                try {
                    if (!resumeScan.await(5, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("Timed out waiting to resume scan");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Backfill scan interrupted", e);
                }
            }
            return new RedisQueueRegistry.ScanPage("0", List.of());
        };

        RedisQueueRegistry racingRegistry = new RedisQueueRegistry(client, scanner);
        try (var executor = Executors.newSingleThreadExecutor()) {
            Future<Long> backfill = executor.submit(racingRegistry::backfillExistingQueues);
            assertTrue(scanStarted.await(5, TimeUnit.SECONDS));

            Job job = Job.create("registry", Payload.empty());
            jobIds.add(job.id());
            new QueueCoordinatorImp(new RedisJobSubmissionStore(client, serializer))
                    .submit(job, concurrentQueue);
            resumeScan.countDown();

            assertEquals(0, backfill.get(5, TimeUnit.SECONDS));
            assertTrue(registry.listQueues().contains(concurrentQueue));
        } finally {
            resumeScan.countDown();
        }
    }

    private Job persist(Job job) {
        jobIds.add(job.id());
        return repository.save(job);
    }

    private String name(String suffix) {
        String queueName = prefix + "-" + suffix;
        queueNames.add(queueName);
        return queueName;
    }
}
