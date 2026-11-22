package io.github.pandeyayushk.jobstream.persistence;

import io.github.pandeyayushk.jobstream.job.Job;
import io.github.pandeyayushk.jobstream.job.JobId;
import io.github.pandeyayushk.jobstream.job.JobStatus;
import io.github.pandeyayushk.jobstream.payload.Payload;
import io.github.pandeyayushk.jobstream.serialization.JacksonJobSerializer;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.RedisClient;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class RedisJobRepositoryTest {

    @Test
    public void findByIdReturnsJobWhenPresent() {
        RedisClient client = RedisClient.builder()
                .hostAndPort("localhost", 6379)
                .build();

        JacksonJobSerializer serializer = new JacksonJobSerializer();
        RedisJobRepository repository =
                new RedisJobRepository(client, serializer);

        Job job = Job.create(
                "Email",
                Payload.of(Map.of(
                        "email", "test@example.com",
                        "attempts", 3,
                        "urgent", true
                ))
        );

        repository.save(job);

        var result = repository.findById(job.id());

        assertTrue(result.isPresent());

        Job restored = result.get();

        assertEquals(job.id(), restored.id());
        assertEquals(job.type(), restored.type());
        assertEquals(job.status(), restored.status());
        assertEquals(job.payload().asMap(), restored.payload().asMap());
        assertEquals(job.createdAt(), restored.createdAt());
        assertEquals(job.updatedAt(), restored.updatedAt());
        assertEquals(job.metadata(), restored.metadata());

        repository.delete(job.id());
    }

    @Test
    public void findByIdReturnsEmptyWhenJobDoesNotExist() {
        RedisClient client = RedisClient.builder()
                .hostAndPort("localhost", 6379)
                .build();

        JacksonJobSerializer serializer = new JacksonJobSerializer();
        RedisJobRepository repository =
                new RedisJobRepository(client, serializer);

        JobId id = JobId.generate();

        assertTrue(repository.findById(id).isEmpty());
    }

    @Test
    public void existsReturnsTrueForSavedJobAndFalseForUnknownJobId() {
        RedisClient client = RedisClient.builder()
                .hostAndPort("localhost", 6379)
                .build();

        JacksonJobSerializer serializer = new JacksonJobSerializer();
        RedisJobRepository repository =
                new RedisJobRepository(client, serializer);

        Job job = Job.create(
                "Email",
                Payload.of(Map.of("email", "test@example.com"))
        );

        repository.save(job);

        assertTrue(repository.exists(job.id()));

        JobId unknownId = JobId.generate();

        assertFalse(repository.exists(unknownId));

        repository.delete(job.id());
    }

    @Test
    public void findByStatusReturnsAllSavedPendingJobs() {
        RedisClient client = RedisClient.builder()
                .hostAndPort("localhost", 6379)
                .build();

        JacksonJobSerializer serializer = new JacksonJobSerializer();
        RedisJobRepository repository =
                new RedisJobRepository(client, serializer);

        Job job1 = Job.create(
                "Email",
                Payload.of(Map.of("email", "one@example.com"))
        );

        Job job2 = Job.create(
                "Email",
                Payload.of(Map.of("email", "two@example.com"))
        );

        repository.save(job1);
        repository.save(job2);

        List<Job> pendingJobs =
                repository.findByStatus(JobStatus.PENDING);

        assertTrue(
                pendingJobs.stream()
                        .anyMatch(job -> job.id().equals(job1.id()))
        );

        assertTrue(
                pendingJobs.stream()
                        .anyMatch(job -> job.id().equals(job2.id()))
        );

        repository.delete(job1.id());
        repository.delete(job2.id());
    }

    @Test
    public void saveWithUpdatedStatusUpdatesStatusIndexCleanly() {
        RedisClient client = RedisClient.builder()
                .hostAndPort("localhost", 6379)
                .build();

        JacksonJobSerializer serializer = new JacksonJobSerializer();
        RedisJobRepository repository =
                new RedisJobRepository(client, serializer);

        Job job = Job.create(
                "Email",
                Payload.of(Map.of("email", "test@example.com"))
        );

        Job persistedJob = repository.save(job);

        Job queuedJob = persistedJob.withStatus(JobStatus.QUEUED);

        queuedJob = repository.save(queuedJob);

        List<Job> pendingJobs =
                repository.findByStatus(JobStatus.PENDING);

        List<Job> queuedJobs =
                repository.findByStatus(JobStatus.QUEUED);

        assertTrue(
                pendingJobs.stream()
                        .noneMatch(savedJob ->
                                savedJob.id().equals(job.id()))
        );

        assertTrue(
                queuedJobs.stream()
                        .anyMatch(savedJob ->
                                savedJob.id().equals(job.id()))
        );
        assertEquals(2, queuedJob.revision());

        repository.delete(job.id());
    }

    @Test
    public void deleteRemovesBothJobRecordAndStatusIndexEntry() {
        RedisClient client = RedisClient.builder()
                .hostAndPort("localhost", 6379)
                .build();

        JacksonJobSerializer serializer = new JacksonJobSerializer();
        RedisJobRepository repository =
                new RedisJobRepository(client, serializer);

        Job job = Job.create(
                "Email",
                Payload.of(Map.of("email", "test@example.com"))
        );

        repository.save(job);

        assertTrue(repository.exists(job.id()));

        boolean deleted = repository.delete(job.id());

        assertTrue(deleted);
        assertFalse(repository.exists(job.id()));
        assertTrue(repository.findById(job.id()).isEmpty());

        List<Job> pendingJobs =
                repository.findByStatus(JobStatus.PENDING);

        assertTrue(
                pendingJobs.stream()
                        .noneMatch(savedJob ->
                                savedJob.id().equals(job.id()))
        );
    }

    @Test
    public void staleSnapshotCannotOverwriteNewerJobRevision() {
        RedisClient client = RedisClient.create("redis://localhost:6379");
        JacksonJobSerializer serializer = new JacksonJobSerializer();
        RedisJobRepository repository = new RedisJobRepository(client, serializer);
        Job saved = repository.save(Job.create("Email", Payload.empty()));
        Job stale = saved.withMetadata("stale", "value");
        Job current = repository.save(saved.withMetadata("current", "value"));

        assertThrows(ConcurrentJobUpdateException.class, () -> repository.save(stale));

        Job restored = repository.findById(saved.id()).orElseThrow();
        assertEquals(current.revision(), restored.revision());
        assertEquals("value", restored.metadata().get("current"));
        assertFalse(restored.metadata().containsKey("stale"));
        repository.delete(saved.id());
        client.close();
    }

    @Test
    public void payloadRevisionFieldDoesNotConfuseOptimisticVersionCheck() {
        RedisClient client = RedisClient.create("redis://localhost:6379");
        JacksonJobSerializer serializer = new JacksonJobSerializer();
        RedisJobRepository repository = new RedisJobRepository(client, serializer);
        Job saved = repository.save(
                Job.create("Email", Payload.of(Map.of("revision", 99)))
        );

        Job updated = repository.save(saved.withMetadata("updated", "yes"));

        assertEquals(2, updated.revision());
        assertEquals(99, repository.findById(saved.id()).orElseThrow().payload().asMap().get("revision"));
        repository.delete(saved.id());
        client.close();
    }
}
