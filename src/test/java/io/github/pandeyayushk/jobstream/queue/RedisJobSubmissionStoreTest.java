package io.github.pandeyayushk.jobstream.queue;

import io.github.pandeyayushk.jobstream.job.Job;
import io.github.pandeyayushk.jobstream.job.JobStatus;
import io.github.pandeyayushk.jobstream.payload.Payload;
import io.github.pandeyayushk.jobstream.persistence.RedisJobRepository;
import io.github.pandeyayushk.jobstream.serialization.JacksonJobSerializer;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.RedisClient;

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

        assertEquals(1, client.llen("jobstream:queue:" + queueName));

        assertEquals(
                Optional.of(originalJob.id()),
                new RedisJobQueue(client).dequeueNonBlocking(queueName)
        );
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

        assertEquals(
                1,
                client.llen("jobstream:queue:" + queueName)
        );

        assertEquals(
                originalJob.id().toString(),
                client.rpop("jobstream:queue:" + queueName)
        );
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
