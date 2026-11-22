package io.github.pandeyayushk.jobstream.schedule;

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

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class RedisJobSchedulerIntegrationTest {

    private static final Instant FIXED_NOW = Instant.parse("2026-11-22T12:00:00.123456789Z");

    private RedisClient client;
    private JacksonJobSerializer serializer;
    private RedisJobRepository repository;
    private RedisJobScheduler scheduler;
    private String scheduledKey;
    private String targetsKey;
    private final List<JobId> jobIds = new ArrayList<>();

    @BeforeEach
    void setUp() {
        client = RedisClient.create("redis://localhost:6379");
        serializer = new JacksonJobSerializer();
        repository = new RedisJobRepository(client, serializer);
        String unique = "jobstream:test:schedule:" + UUID.randomUUID();
        scheduledKey = unique + ":delayed";
        targetsKey = unique + ":targets";
        scheduler = new RedisJobScheduler(
                client,
                serializer,
                Clock.fixed(FIXED_NOW, ZoneOffset.UTC),
                scheduledKey,
                targetsKey
        );
    }

    @AfterEach
    void tearDown() {
        client.del(scheduledKey, targetsKey);
        for (JobId id : jobIds) {
            client.del("jobstream:job:" + id);
            for (JobStatus status : JobStatus.values()) {
                client.srem("jobstream:status:" + status.name(), id.toString());
            }
        }
        client.close();
    }

    @Test
    void schedulesAtExplicitInstantAndPersistsExactJobTimestampAndTarget() {
        Job job = persist(Job.create("mail", Payload.empty()));
        Instant executeAt = Instant.parse("2027-02-03T04:05:06.123456789Z");

        scheduler.schedule(job.id(), executeAt, "emails");

        assertEquals((double) executeAt.toEpochMilli(), client.zscore(scheduledKey, job.id().toString()));
        assertEquals("emails", client.hget(targetsKey, job.id().toString()));
        Job stored = repository.findById(job.id()).orElseThrow();
        assertEquals(executeAt, stored.scheduledAt().orElseThrow());
        assertEquals(1, scheduler.scheduledCount());
    }

    @Test
    void schedulesRelativeDurationFromInjectedClock() {
        Job job = persist(Job.create("mail", Payload.empty()));
        Duration delay = Duration.ofSeconds(3, 250_000_000);
        Instant expected = FIXED_NOW.plus(delay);

        scheduler.scheduleDelayed(job.id(), delay, "delayed-mail");

        assertEquals((double) expected.toEpochMilli(), client.zscore(scheduledKey, job.id().toString()));
        assertEquals(expected, repository.findById(job.id()).orElseThrow().scheduledAt().orElseThrow());
    }

    @Test
    void zeroDelaySchedulesImmediatelyAtClockInstant() {
        Job job = persist(Job.create("mail", Payload.empty()));

        scheduler.scheduleDelayed(job.id(), Duration.ZERO, "mail");

        assertEquals(FIXED_NOW, repository.findById(job.id()).orElseThrow().scheduledAt().orElseThrow());
    }

    @Test
    void reschedulingUpdatesDueTimeAndDestinationWithoutDuplicate() {
        Job job = persist(Job.create("mail", Payload.empty()));
        Instant first = FIXED_NOW.plusSeconds(10);
        Instant second = FIXED_NOW.plusSeconds(20);
        scheduler.schedule(job.id(), first, "first-queue");

        scheduler.schedule(job.id(), second, "second-queue");

        assertEquals(1, scheduler.scheduledCount());
        assertEquals((double) second.toEpochMilli(), client.zscore(scheduledKey, job.id().toString()));
        assertEquals("second-queue", client.hget(targetsKey, job.id().toString()));
        assertEquals(second, repository.findById(job.id()).orElseThrow().scheduledAt().orElseThrow());
    }

    @Test
    void missingJobIdIsRejectedWithoutCreatingScheduleEntries() {
        JobId missing = JobId.generate();

        assertThrows(JobSchedulingException.class,
                () -> scheduler.schedule(missing, FIXED_NOW, "mail"));

        assertNull(client.zscore(scheduledKey, missing.toString()));
        assertNull(client.hget(targetsKey, missing.toString()));
        assertEquals(0, scheduler.scheduledCount());
    }

    @Test
    void onlyPendingAndRetryingJobsCanBeScheduled() {
        List<JobStatus> invalidStatuses = List.of(
                JobStatus.QUEUED,
                JobStatus.PROCESSING,
                JobStatus.COMPLETED,
                JobStatus.FAILED,
                JobStatus.DEAD
        );
        for (JobStatus status : invalidStatuses) {
            Job job = persist(jobWithStatus(status));
            assertThrows(JobSchedulingException.class,
                    () -> scheduler.schedule(job.id(), FIXED_NOW, "mail"), status.name());
            assertTrue(repository.findById(job.id()).orElseThrow().scheduledAt().isEmpty());
            assertNull(client.zscore(scheduledKey, job.id().toString()));
        }

        Job retrying = persist(jobWithStatus(JobStatus.RETRYING));
        scheduler.schedule(retrying.id(), FIXED_NOW, "retry-mail");
        assertEquals(JobStatus.RETRYING, repository.findById(retrying.id()).orElseThrow().status());
    }

    @Test
    void schedulingPreservesPriorityPayloadRetryMetadataAndOtherJobFields() {
        Job failed = Job.create("mail", Payload.of(Map.of("recipient", "a@example.com")))
                .withPriority(19)
                .withMetadata("tenant", "acme")
                .withStatus(JobStatus.QUEUED)
                .withStatus(JobStatus.PROCESSING)
                .withStatus(JobStatus.FAILED);
        Job retrying = failed.withRetryAttempt("temporary failure");
        persist(retrying);

        scheduler.schedule(retrying.id(), FIXED_NOW.plusSeconds(8), "retry-mail");

        Job restored = repository.findById(retrying.id()).orElseThrow();
        assertEquals(19, restored.priority());
        assertEquals(retrying.retryCount(), restored.retryCount());
        assertEquals(retrying.maxRetries(), restored.maxRetries());
        assertEquals(retrying.lastErrorReason(), restored.lastErrorReason());
        assertEquals(retrying.lastFailedAt(), restored.lastFailedAt());
        assertEquals(retrying.payload().asMap(), restored.payload().asMap());
        assertEquals(retrying.metadata(), restored.metadata());
        assertEquals(retrying.createdAt(), restored.createdAt());
        assertEquals(JobStatus.RETRYING, restored.status());
    }

    @Test
    void readsLegacyJobJsonWithoutPriorityOrScheduledAt() {
        JobId id = JobId.generate();
        jobIds.add(id);
        client.set("jobstream:job:" + id, """
                {"id":"%s","type":"legacy","status":"PENDING","payload":{},
                 "createdAt":"2026-01-01T00:00:00Z","updatedAt":"2026-01-01T00:00:00Z",
                 "metadata":{},"retryCount":1,"maxRetries":3,"lastErrorReason":"old error",
                 "lastFailedAt":"2025-12-31T23:59:00Z"}
                """.formatted(id));

        Instant executeAt = FIXED_NOW.plusSeconds(15);
        scheduler.schedule(id, executeAt, "legacy-queue");

        Job restored = repository.findById(id).orElseThrow();
        assertEquals(0, restored.priority());
        assertEquals(executeAt, restored.scheduledAt().orElseThrow());
        assertEquals(1, restored.retryCount());
        assertEquals("old error", restored.lastErrorReason().orElseThrow());
        assertEquals("legacy-queue", client.hget(targetsKey, id.toString()));
    }

    @Test
    void concurrentSchedulesForSameJobRemainUniqueAndConsistent() throws Exception {
        Job job = persist(Job.create("mail", Payload.empty()).withPriority(23));
        int taskCount = 24;
        ExecutorService executor = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < taskCount; i++) {
                int index = i;
                futures.add(executor.submit(() -> {
                    start.await();
                    scheduler.schedule(
                            job.id(),
                            FIXED_NOW.plusSeconds(index + 1L),
                            "queue-" + index
                    );
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> future : futures) {
                future.get();
            }
        } finally {
            executor.shutdownNow();
        }

        Job stored = repository.findById(job.id()).orElseThrow();
        Instant executeAt = stored.scheduledAt().orElseThrow();
        assertEquals(1, scheduler.scheduledCount());
        assertEquals((double) executeAt.toEpochMilli(), client.zscore(scheduledKey, job.id().toString()));
        assertTrue(client.hget(targetsKey, job.id().toString()).startsWith("queue-"));
        assertEquals(23, stored.priority());
    }

    @Test
    void incompatibleSchedulerKeyTypesDoNotPartiallyUpdateJob() {
        Job job = persist(Job.create("mail", Payload.empty()));
        client.set(scheduledKey, "wrong-type");

        assertThrows(JobSchedulingException.class,
                () -> scheduler.schedule(job.id(), FIXED_NOW.plusSeconds(1), "mail"));

        assertTrue(repository.findById(job.id()).orElseThrow().scheduledAt().isEmpty());
        assertNull(client.hget(targetsKey, job.id().toString()));
        assertEquals("wrong-type", client.get(scheduledKey));
    }

    @Test
    void staleRepositorySaveCannotRemoveSuccessfullyScheduledTimestamp() {
        Job stored = persist(Job.create("mail", Payload.empty()));
        Job staleCopy = stored.withMetadata("stale", "must-not-win");
        Instant executeAt = FIXED_NOW.plusSeconds(12);

        scheduler.schedule(stored.id(), executeAt, "mail");

        assertThrows(
                io.github.pandeyayushk.jobstream.persistence.ConcurrentJobUpdateException.class,
                () -> repository.save(staleCopy)
        );

        Job restored = repository.findById(stored.id()).orElseThrow();
        assertEquals(executeAt, restored.scheduledAt().orElseThrow());
        assertFalse(restored.metadata().containsKey("stale"));
        assertEquals("mail", client.hget(targetsKey, stored.id().toString()));
    }

    @Test
    void schedulerRetriesCasWhenRepositoryUpdateWinsBeforeItsAtomicWrite() {
        Job stored = persist(Job.create("mail", Payload.empty()));
        Job concurrentUpdate = stored.withMetadata("concurrent", "preserve-me");
        AtomicBoolean updateOnce = new AtomicBoolean(true);
        RedisJobScheduler racingScheduler = new RedisJobScheduler(
                client,
                serializer,
                Clock.fixed(FIXED_NOW, ZoneOffset.UTC),
                scheduledKey,
                targetsKey,
                () -> {
                    if (updateOnce.compareAndSet(true, false)) {
                        repository.save(concurrentUpdate);
                    }
                }
        );
        Instant executeAt = FIXED_NOW.plusSeconds(25);

        racingScheduler.schedule(stored.id(), executeAt, "mail-after-update");

        Job restored = repository.findById(stored.id()).orElseThrow();
        assertEquals("preserve-me", restored.metadata().get("concurrent"));
        assertEquals(executeAt, restored.scheduledAt().orElseThrow());
        assertEquals("mail-after-update", client.hget(targetsKey, stored.id().toString()));
        assertEquals(1, racingScheduler.scheduledCount());
    }

    @Test
    void validatesNullNegativeAndUnrepresentableTimeAndInvalidQueue() {
        Job job = persist(Job.create("mail", Payload.empty()));
        assertThrows(NullPointerException.class, () -> scheduler.schedule(job.id(), null, "mail"));
        assertThrows(IllegalArgumentException.class, () -> scheduler.schedule(job.id(), FIXED_NOW, " "));
        assertThrows(NullPointerException.class, () -> scheduler.schedule(null, FIXED_NOW, "mail"));
        assertThrows(NullPointerException.class, () -> scheduler.scheduleDelayed(job.id(), null, "mail"));
        assertThrows(IllegalArgumentException.class,
                () -> scheduler.scheduleDelayed(job.id(), Duration.ofMillis(-1), "mail"));
        assertThrows(IllegalArgumentException.class,
                () -> scheduler.scheduleDelayed(job.id(), Duration.ofSeconds(Long.MAX_VALUE), "mail"));
        assertThrows(IllegalArgumentException.class,
                () -> scheduler.schedule(job.id(), Instant.MAX, "mail"));
        assertThrows(IllegalArgumentException.class,
                () -> scheduler.schedule(job.id(), Instant.ofEpochMilli(1L << 53).plusMillis(1), "mail"));
        assertEquals(0, scheduler.scheduledCount());
    }

    private Job jobWithStatus(JobStatus status) {
        Job job = Job.create("mail", Payload.empty());
        return switch (status) {
            case PENDING -> job;
            case QUEUED -> job.withStatus(JobStatus.QUEUED);
            case PROCESSING -> job.withStatus(JobStatus.QUEUED).withStatus(JobStatus.PROCESSING);
            case COMPLETED -> job.withStatus(JobStatus.QUEUED)
                    .withStatus(JobStatus.PROCESSING).withStatus(JobStatus.COMPLETED);
            case FAILED -> job.withStatus(JobStatus.QUEUED)
                    .withStatus(JobStatus.PROCESSING).withStatus(JobStatus.FAILED);
            case RETRYING -> job.withStatus(JobStatus.QUEUED)
                    .withStatus(JobStatus.PROCESSING).withStatus(JobStatus.FAILED)
                    .withRetryAttempt("retryable");
            case DEAD -> job.withStatus(JobStatus.QUEUED)
                    .withStatus(JobStatus.PROCESSING).withStatus(JobStatus.FAILED)
                    .withStatus(JobStatus.DEAD);
        };
    }

    private Job persist(Job job) {
        jobIds.add(job.id());
        return repository.save(job);
    }
}
