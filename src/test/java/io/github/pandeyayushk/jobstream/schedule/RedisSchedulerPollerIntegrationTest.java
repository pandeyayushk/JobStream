package io.github.pandeyayushk.jobstream.schedule;

import io.github.pandeyayushk.jobstream.job.Job;
import io.github.pandeyayushk.jobstream.job.JobId;
import io.github.pandeyayushk.jobstream.job.JobStatus;
import io.github.pandeyayushk.jobstream.payload.Payload;
import io.github.pandeyayushk.jobstream.persistence.RedisJobRepository;
import io.github.pandeyayushk.jobstream.queue.RedisJobQueue;
import io.github.pandeyayushk.jobstream.queue.RedisPriorityQueueStore;
import io.github.pandeyayushk.jobstream.serialization.JacksonJobSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.RedisClient;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

class RedisSchedulerPollerIntegrationTest {

    private static final Instant NOW = Instant.parse("2026-11-22T12:00:00.987654321Z");
    private RedisClient client;
    private JacksonJobSerializer serializer;
    private RedisJobRepository repository;
    private RedisJobQueue queue;
    private RedisJobScheduler scheduler;
    private RedisSchedulerPoller poller;
    private String scheduledKey;
    private String targetsKey;
    private String queuePrefix;
    private final List<JobId> ids = new ArrayList<>();
    private final List<String> queueNames = new ArrayList<>();

    @BeforeEach
    void setUp() {
        client = RedisClient.create("redis://localhost:6379");
        serializer = new JacksonJobSerializer();
        repository = new RedisJobRepository(client, serializer);
        queue = new RedisJobQueue(client);
        String keyBase = "jobstream:test:poll:" + UUID.randomUUID();
        queuePrefix = keyBase + ":queue:";
        scheduledKey = keyBase + ":delayed";
        targetsKey = keyBase + ":targets";
        Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
        scheduler = new RedisJobScheduler(client, serializer, clock, scheduledKey, targetsKey);
        poller = new RedisSchedulerPoller(client, serializer, clock, scheduledKey, targetsKey, () -> { });
    }

    @AfterEach
    void tearDown() {
        client.del(scheduledKey, targetsKey);
        for (String name : queueNames) {
            client.del(RedisPriorityQueueStore.sequenceKey(name),
                    RedisPriorityQueueStore.queueKey(name), RedisPriorityQueueStore.membersKey(name),
                    "jobstream:queue:" + name);
        }
        for (JobId id : ids) {
            client.del("jobstream:job:" + id);
            for (JobStatus status : JobStatus.values()) {
                client.srem("jobstream:status:" + status, id.toString());
            }
        }
        client.close();
    }

    @Test
    void futureJobIsNotReleasedAndDuePendingJobIsReleasedAtomically() {
        Job future = persist(Job.create("mail", Payload.empty()));
        Job due = persist(Job.create("mail", Payload.empty()).withPriority(5));
        schedule(future, NOW.plusMillis(1), "future");
        schedule(due, NOW, "ready");

        SchedulerPollResult result = poller.pollOnce(10);

        assertEquals(1, result.released());
        assertEquals(JobStatus.QUEUED, stored(due).status());
        assertEquals(due.revision() + 2, stored(due).revision()); // scheduler write, then release
        assertEquals(due.id(), queue.dequeueNonBlocking(queueName("ready")).orElseThrow());
        assertEquals(1, scheduler.scheduledCount());
        assertEquals(queueName("future"), client.hget(targetsKey, future.id().toString()));
    }

    @Test
    void dueRetryingJobPreservesFieldsAndMovesToConfiguredPriorityQueue() {
        Job retry = Job.create("mail", Payload.of(Map.of("to", "a@example.com")))
                .withPriority(-4).withMetadata("tenant", "acme")
                .withStatus(JobStatus.QUEUED).withStatus(JobStatus.PROCESSING)
                .withStatus(JobStatus.FAILED).withRetryAttempt("network");
        retry = persist(retry);
        long scheduledRevision = retry.revision() + 1;
        schedule(retry, NOW.minusSeconds(1), "retry-target");

        SchedulerPollResult result = poller.pollOnce(5);

        Job restored = stored(retry);
        assertEquals(1, result.released());
        assertEquals(JobStatus.QUEUED, restored.status());
        assertEquals(scheduledRevision + 1, restored.revision());
        assertEquals(-4, restored.priority());
        assertEquals(retry.payload().asMap(), restored.payload().asMap());
        assertEquals(retry.metadata(), restored.metadata());
        assertEquals(retry.retryCount(), restored.retryCount());
        assertEquals(retry.lastErrorReason(), restored.lastErrorReason());
        assertEquals(retry.lastFailedAt(), restored.lastFailedAt());
        assertEquals(NOW.minusSeconds(1), restored.scheduledAt().orElseThrow());
        assertEquals(retry.id(), queue.dequeueNonBlocking(queueName("retry-target")).orElseThrow());
        assertNull(client.zscore(scheduledKey, retry.id().toString()));
        assertNull(client.hget(targetsKey, retry.id().toString()));
    }

    @Test
    void existingPriorityOrderingAndFifoTiesAreUsed() {
        Job low = persist(Job.create("mail", Payload.empty()).withPriority(-3));
        Job first = persist(Job.create("mail", Payload.empty()).withPriority(8));
        Job second = persist(Job.create("mail", Payload.empty()).withPriority(8));
        schedule(low, NOW, "ordered");
        schedule(first, NOW, "ordered");
        schedule(second, NOW, "ordered");

        poller.pollOnce(10);

        List<JobId> expected = new ArrayList<>(List.of(first.id(), second.id()));
        expected.sort(java.util.Comparator.comparing(JobId::toString));
        expected.add(low.id());
        // The delayed ZSET orders equal due scores by JobId; priority queue FIFO then
        // preserves the order in which this bounded poll inserts equal priorities.
        assertEquals(expected, queue.peek(queueName("ordered"), 3));
    }

    @Test
    void batchLimitBoundsNumberOfCandidates() {
        List<Job> jobs = List.of(
                persist(Job.create("mail", Payload.empty())),
                persist(Job.create("mail", Payload.empty())),
                persist(Job.create("mail", Payload.empty()))
        );
        jobs.forEach(job -> schedule(job, NOW, "batch"));

        SchedulerPollResult result = poller.pollOnce(2);

        assertEquals(2, result.examined());
        assertEquals(2, result.released());
        assertEquals(1, scheduler.scheduledCount());
        assertThrows(IllegalArgumentException.class, () -> poller.pollOnce(0));
    }

    @Test
    void missingJobsAndNoLongerSchedulableJobsAreCleaned() {
        Job missing = persist(Job.create("mail", Payload.empty()));
        schedule(missing, NOW, "cleanup");
        repository.delete(missing.id());

        Job movedOn = persist(Job.create("mail", Payload.empty()));
        schedule(movedOn, NOW, "cleanup");
        Job scheduledMovedOn = stored(movedOn);
        repository.save(scheduledMovedOn.withStatus(JobStatus.QUEUED));

        SchedulerPollResult result = poller.pollOnce(10);

        assertEquals(2, result.cleaned());
        assertEquals(0, scheduler.scheduledCount());
        assertEquals(0, queue.size(queueName("cleanup")));
    }

    @Test
    void existingQueueMembershipDoesNotCreateDuplicate() {
        Job job = persist(Job.create("mail", Payload.empty()).withPriority(2));
        queue.enqueue(job.id(), queueName("already-there"));
        schedule(job, NOW, "already-there");

        SchedulerPollResult result = poller.pollOnce(1);

        assertEquals(1, result.released());
        assertEquals(1, queue.size(queueName("already-there")));
        assertEquals(job.id(), queue.dequeueNonBlocking(queueName("already-there")).orElseThrow());
    }

    @Test
    void invalidDestinationIsDeferredWithoutLosingSchedule() {
        Job job = persist(Job.create("mail", Payload.empty()));
        client.zadd(scheduledKey, NOW.toEpochMilli(), job.id().toString());
        client.hset(targetsKey, job.id().toString(), " ");

        SchedulerPollResult result = poller.pollOnce(1);

        assertEquals(1, result.deferred());
        assertEquals(1, scheduler.scheduledCount());
        assertEquals(JobStatus.PENDING, stored(job).status());
    }

    @Test
    void incompatibleQueueTypeDoesNotPartiallyReleaseJob() {
        Job job = persist(Job.create("mail", Payload.empty()));
        schedule(job, NOW, "wrong-type");
        client.set(RedisPriorityQueueStore.queueKey(queueName("wrong-type")), "not a zset");

        assertThrows(JobSchedulingException.class, () -> poller.pollOnce(1));

        assertEquals(JobStatus.PENDING, stored(job).status());
        assertEquals(1, scheduler.scheduledCount());
        assertEquals(queueName("wrong-type"), client.hget(targetsKey, job.id().toString()));
    }

    @Test
    void twoConcurrentPollersReleaseTheSameJobOnlyOnce() throws Exception {
        Job job = persist(Job.create("mail", Payload.empty()));
        schedule(job, NOW, "concurrent");
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch proceed = new CountDownLatch(1);
        Runnable barrier = () -> {
            ready.countDown();
            try {
                if (!proceed.await(5, java.util.concurrent.TimeUnit.SECONDS)) {
                    throw new AssertionError("Poller barrier timed out");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        };
        RedisSchedulerPoller first = new RedisSchedulerPoller(client, serializer,
                Clock.fixed(NOW, ZoneOffset.UTC), scheduledKey, targetsKey, barrier);
        RedisSchedulerPoller second = new RedisSchedulerPoller(client, serializer,
                Clock.fixed(NOW, ZoneOffset.UTC), scheduledKey, targetsKey, barrier);
        try (var executor = Executors.newFixedThreadPool(2)) {
            Future<SchedulerPollResult> a = executor.submit(() -> first.pollOnce(1));
            Future<SchedulerPollResult> b = executor.submit(() -> second.pollOnce(1));
            assertTrue(ready.await(5, java.util.concurrent.TimeUnit.SECONDS));
            proceed.countDown();
            a.get();
            b.get();
        }

        assertEquals(1, queue.size(queueName("concurrent")));
        assertEquals(job.id(), queue.dequeueNonBlocking(queueName("concurrent")).orElseThrow());
        assertEquals(0, scheduler.scheduledCount());
        assertEquals(3, stored(job).revision()); // initial save, schedule update, then release
    }

    @Test
    void malformedRecordIsDeferredAndSchedulingIndexesRemain() {
        Job job = persist(Job.create("mail", Payload.empty()));
        schedule(job, NOW, "malformed");
        client.set("jobstream:job:" + job.id(), "not-json");

        SchedulerPollResult result = poller.pollOnce(1);

        assertEquals(1, result.deferred());
        assertEquals(1, scheduler.scheduledCount());
        assertEquals(queueName("malformed"), client.hget(targetsKey, job.id().toString()));
    }

    private Job persist(Job job) {
        ids.add(job.id());
        return repository.save(job);
    }

    private void schedule(Job job, Instant at, String target) {
        scheduler.schedule(job.id(), at, queueName(target));
    }

    private String queueName(String name) {
        String uniqueName = queuePrefix + name;
        if (!queueNames.contains(uniqueName)) queueNames.add(uniqueName);
        return uniqueName;
    }

    private Job stored(Job job) {
        return repository.findById(job.id()).orElseThrow();
    }

}
