package io.github.pandeyayushk.jobstream.queue;

import io.github.pandeyayushk.jobstream.job.Job;
import io.github.pandeyayushk.jobstream.job.JobStatus;
import io.github.pandeyayushk.jobstream.payload.Payload;
import io.github.pandeyayushk.jobstream.persistence.RedisJobRepository;
import io.github.pandeyayushk.jobstream.serialization.JacksonJobSerializer;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.RedisClient;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class RedisJobSubmissionStoreTest {
    @Test
    public void submitMovesPendingJobToQueued() {
        RedisClient client = RedisClient.builder()
                .hostAndPort("localhost", 6379)
                .build();

        JacksonJobSerializer serializer = new JacksonJobSerializer();
        RedisJobSubmissionStore store =new RedisJobSubmissionStore(client, serializer);

        RedisJobRepository repository =new RedisJobRepository(client, serializer);

        String queueName = "submission-pending-" + UUID.randomUUID();

        Job originalJob = Job.create("email",Payload.empty());

        Job queuedJob = originalJob.withStatus(JobStatus.QUEUED);

        store.submit(originalJob, queuedJob, queueName);

        Optional<Job> storedJob = repository.findById(originalJob.id());

        assertTrue(storedJob.isPresent());
        assertEquals(JobStatus.QUEUED, storedJob.get().status());

        assertFalse(
                client.sismember(
                        "jobstream:status:PENDING",
                        originalJob.id().toString()
                )
        );

        assertTrue(
                client.sismember(
                        "jobstream:status:QUEUED",
                        originalJob.id().toString()
                )
        );

        assertEquals(1, new RedisJobQueue(client).size(queueName));

        assertEquals(
                Optional.of(originalJob.id()),
                new RedisJobQueue(client).dequeueNonBlocking(queueName)
        );
    }

    @Test
    public void staleSubmissionCannotOverwriteNewerPersistedJob() {
        RedisClient client = RedisClient.create("redis://localhost:6379");
        JacksonJobSerializer serializer = new JacksonJobSerializer();
        RedisJobRepository repository = new RedisJobRepository(client, serializer);
        RedisJobSubmissionStore store = new RedisJobSubmissionStore(client, serializer);
        String queueName = "submission-stale-" + UUID.randomUUID();
        Job original = repository.save(Job.create("email", Payload.empty()));
        repository.save(original.withMetadata("newer", "preserve"));

        assertThrows(
                QueueException.class,
                () -> store.submit(original, original.withStatus(JobStatus.QUEUED), queueName)
        );

        Job restored = repository.findById(original.id()).orElseThrow();
        assertEquals(JobStatus.PENDING, restored.status());
        assertEquals("preserve", restored.metadata().get("newer"));
        assertEquals(0, new RedisJobQueue(client).size(queueName));
        client.del(
                "jobstream:job:" + original.id(),
                RedisPriorityQueueStore.legacyKey(queueName),
                RedisPriorityQueueStore.queueKey(queueName),
                RedisPriorityQueueStore.sequenceKey(queueName),
                RedisPriorityQueueStore.membersKey(queueName)
        );
        client.srem("jobstream:status:PENDING", original.id().toString());
        client.close();
    }

    @Test
    public void submissionReturnsCommittedSnapshotWithRevisionAndJobFields() {
        RedisClient client = RedisClient.create("redis://localhost:6379");
        JacksonJobSerializer serializer = new JacksonJobSerializer();
        RedisJobRepository repository = new RedisJobRepository(client, serializer);
        String queueName = "submission-result-" + UUID.randomUUID();
        Instant scheduledAt = Instant.parse("2027-03-04T05:06:07.123456789Z");

        Job retrying = Job.create("email", Payload.of(Map.of("recipient", "a@example.com")))
                .withPriority(14)
                .withScheduledAt(scheduledAt)
                .withMetadata("tenant", "acme")
                .withStatus(JobStatus.QUEUED)
                .withStatus(JobStatus.PROCESSING)
                .withStatus(JobStatus.FAILED)
                .withRetryAttempt("temporary failure");
        Job original = repository.save(retrying);
        QueueCoordinator coordinator = new QueueCoordinatorImp(
                new RedisJobSubmissionStore(client, serializer)
        );

        Job persisted = coordinator.submit(original, queueName);

        assertEquals(JobStatus.QUEUED, persisted.status());
        assertEquals(original.revision() + 1, persisted.revision());
        assertEquals(14, persisted.priority());
        assertEquals(scheduledAt, persisted.scheduledAt().orElseThrow());
        assertEquals(original.retryCount(), persisted.retryCount());
        assertEquals(original.lastErrorReason(), persisted.lastErrorReason());
        assertEquals("acme", persisted.metadata().get("tenant"));
        assertEquals("a@example.com", persisted.payload().asMap().get("recipient"));
        assertEquals(
                serializer.serialize(persisted),
                client.get("jobstream:job:" + original.id())
        );

        assertEquals(JobStatus.RETRYING, original.status());
        assertEquals(1, original.revision());
        assertFalse(original.metadata().containsKey("after-submit"));

        Job savedAgain = repository.save(persisted.withMetadata("after-submit", "yes"));
        assertEquals(persisted.revision() + 1, savedAgain.revision());
        assertEquals("yes", savedAgain.metadata().get("after-submit"));
        assertEquals(1, new RedisJobQueue(client).size(queueName));

        client.del(
                "jobstream:job:" + original.id(),
                RedisPriorityQueueStore.legacyKey(queueName),
                RedisPriorityQueueStore.queueKey(queueName),
                RedisPriorityQueueStore.sequenceKey(queueName),
                RedisPriorityQueueStore.membersKey(queueName)
        );
        client.srem("jobstream:status:QUEUED", original.id().toString());
        client.close();
    }

    @Test
    public void submitMovesDeadJobToQueued() {
        RedisClient client = RedisClient.builder()
                .hostAndPort("localhost", 6379)
                .build();

        JacksonJobSerializer serializer = new JacksonJobSerializer();
        RedisJobSubmissionStore store =
                new RedisJobSubmissionStore(client, serializer);

        String queueName = "submission-dead-" + UUID.randomUUID();

        Job pendingJob = Job.create(
                "email",
                Payload.empty()
        );
        Job deadJob = pendingJob.withStatus(JobStatus.QUEUED);
        deadJob = deadJob.withStatus(JobStatus.PROCESSING);
        deadJob = deadJob.withStatus(JobStatus.FAILED);
        deadJob = deadJob.withStatus(JobStatus.DEAD);

        client.set(
                "jobstream:job:" + deadJob.id(),
                serializer.serialize(deadJob)
        );

        client.sadd(
                "jobstream:status:DEAD",
                deadJob.id().toString()
        );

        Job queuedDeadJob = deadJob.withStatus(JobStatus.QUEUED);

        store.submit(deadJob, queuedDeadJob, queueName);

        assertFalse(
                client.sismember(
                        "jobstream:status:DEAD",
                        deadJob.id().toString()
                )
        );

        assertTrue(
                client.sismember(
                        "jobstream:status:QUEUED",
                        deadJob.id().toString()
                )
        );

        assertEquals(
                Optional.of(deadJob.id()),
                new RedisJobQueue(client).dequeueNonBlocking(queueName)
        );
    }

    @Test
    public void submitAddsExactlyOneJobIdToQueue() {
        RedisClient client = RedisClient.builder()
                .hostAndPort("localhost", 6379)
                .build();

        JacksonJobSerializer serializer = new JacksonJobSerializer();

        RedisJobSubmissionStore store =
                new RedisJobSubmissionStore(client, serializer);

        String queueName = "submission-queue-" + UUID.randomUUID();

        Job originalJob = Job.create(
                "email",
                Payload.empty()
        );

        Job queuedJob = originalJob.withStatus(JobStatus.QUEUED);

        store.submit(originalJob, queuedJob, queueName);

        RedisJobQueue queue = new RedisJobQueue(client);
        assertEquals(1, queue.size(queueName));
        assertEquals(Optional.of(originalJob.id()), queue.dequeueNonBlocking(queueName));
    }

    @Test
    public void submitRejectsNullOriginalJob() {
        RedisClient client = RedisClient.builder()
                .hostAndPort("localhost", 6379)
                .build();

        JacksonJobSerializer serializer = new JacksonJobSerializer();

        RedisJobSubmissionStore store =
                new RedisJobSubmissionStore(client, serializer);

        Job job = Job.create("email", Payload.empty());

        assertThrows(
                NullPointerException.class,
                () -> store.submit(
                        null,
                        job.withStatus(JobStatus.QUEUED),
                        "test"
                )
        );
    }

    @Test
    public void submitRejectsNullQueuedJob() {
        RedisClient client = RedisClient.builder()
                .hostAndPort("localhost", 6379)
                .build();

        JacksonJobSerializer serializer = new JacksonJobSerializer();

        RedisJobSubmissionStore store =
                new RedisJobSubmissionStore(client, serializer);

        Job job = Job.create("email", Payload.empty());

        assertThrows(
                NullPointerException.class,
                () -> store.submit(
                        job,
                        null,
                        "test"
                )
        );
    }

    @Test
    public void submitRejectsNullQueueName() {
        RedisClient client = RedisClient.builder()
                .hostAndPort("localhost", 6379)
                .build();

        JacksonJobSerializer serializer = new JacksonJobSerializer();

        RedisJobSubmissionStore store =
                new RedisJobSubmissionStore(client, serializer);

        Job job = Job.create("email", Payload.empty());
        Job queuedJob = job.withStatus(JobStatus.QUEUED);

        assertThrows(
                IllegalArgumentException.class,
                () -> store.submit(job, queuedJob, null)
        );
    }

    @Test
    public void submitRejectsBlankQueueName() {
        RedisClient client = RedisClient.builder()
                .hostAndPort("localhost", 6379)
                .build();

        JacksonJobSerializer serializer = new JacksonJobSerializer();

        RedisJobSubmissionStore store =
                new RedisJobSubmissionStore(client, serializer);

        Job job = Job.create("email", Payload.empty());
        Job queuedJob = job.withStatus(JobStatus.QUEUED);

        assertThrows(
                IllegalArgumentException.class,
                () -> store.submit(job, queuedJob, "   ")
        );
    }

    @Test
    public void submitThrowsQueueExceptionWhenRedisUnavailable() {
        RedisClient client = RedisClient.builder()
                .hostAndPort("localhost", 6390)
                .build();

        JacksonJobSerializer serializer = new JacksonJobSerializer();

        RedisJobSubmissionStore store =
                new RedisJobSubmissionStore(client, serializer);

        Job originalJob = Job.create(
                "email",
                Payload.empty()
        );

        Job queuedJob = originalJob.withStatus(JobStatus.QUEUED);

        assertThrows(
                QueueException.class,
                () -> store.submit(
                        originalJob,
                        queuedJob,
                        "redis-failure-" + UUID.randomUUID()
                )
        );
    }
}
