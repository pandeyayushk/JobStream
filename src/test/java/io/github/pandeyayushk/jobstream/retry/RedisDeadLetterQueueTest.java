package io.github.pandeyayushk.jobstream.retry;

import io.github.pandeyayushk.jobstream.job.Job;
import io.github.pandeyayushk.jobstream.job.JobStatus;
import io.github.pandeyayushk.jobstream.payload.Payload;
import io.github.pandeyayushk.jobstream.persistence.RedisJobRepository;
import io.github.pandeyayushk.jobstream.serialization.JacksonJobSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.RedisClient;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class RedisDeadLetterQueueTest {

    private RedisClient client;
    private RedisJobRepository repository;
    private RedisDeadLetterQueue deadLetterQueue;

    @BeforeEach
    void setUp() {
        client = RedisClient.builder()
                .hostAndPort("localhost", 6379)
                .build();

        client.flushDB();

        JacksonJobSerializer serializer = new JacksonJobSerializer();

        repository = new RedisJobRepository(
                client,
                serializer
        );

        deadLetterQueue = new RedisDeadLetterQueue(
                client,
                serializer
        );
    }

    @AfterEach
    void tearDown() {
        client.flushDB();
    }

    @Test
    void moveToDeadLetterChangesJobStatusToDead() {
        Job job = Job.create(
                "test-job", Payload.empty());

        Job failed = job
                .withStatus(JobStatus.QUEUED)
                .withStatus(JobStatus.PROCESSING)
                .withStatus(JobStatus.FAILED);

        repository.save(failed);

        deadLetterQueue.moveToDeadLetter(
                failed,
                "permanent failure"
        );

        Optional<Job> restored = repository.findById(failed.id());

        assertTrue(restored.isPresent());
        assertEquals(
                JobStatus.DEAD,
                restored.get().status()
        );
    }

    @Test
    void moveToDeadLetterAddsJobIdToDeadLetterQueue() {
        Job job = Job.create(
                "test-job",Payload.empty()
        );

        Job failed = job
                .withStatus(JobStatus.QUEUED)
                .withStatus(JobStatus.PROCESSING)
                .withStatus(JobStatus.FAILED);

        repository.save(failed);

        deadLetterQueue.moveToDeadLetter(
                failed,
                "permanent failure"
        );

        assertEquals(
                1,
                deadLetterQueue.size()
        );
    }

    @Test
    void moveToDeadLetterPreservesFailureReason() {
        Job job = Job.create(
                "test-job",Payload.empty()
        );

        Job failed = job
                .withStatus(JobStatus.QUEUED)
                .withStatus(JobStatus.PROCESSING)
                .withStatus(JobStatus.FAILED);

        repository.save(failed);

        deadLetterQueue.moveToDeadLetter(
                failed,
                "permanent failure"
        );

        Job restored = repository
                .findById(failed.id())
                .orElseThrow();

        assertEquals(
                "permanent failure",
                restored.metadata().get("failure.reason")
        );
    }

    @Test
    void moveToDeadLetterRejectsNullJob() {
        assertThrows(
                IllegalArgumentException.class,
                () -> deadLetterQueue.moveToDeadLetter(
                        null,
                        "failure"
                )
        );
    }

    @Test
    void moveToDeadLetterRejectsNullReason() {
        Job job = Job.create(
                "test-job",Payload.empty()
        );

        Job failed = job
                .withStatus(JobStatus.QUEUED)
                .withStatus(JobStatus.PROCESSING)
                .withStatus(JobStatus.FAILED);

        assertThrows(
                IllegalArgumentException.class,
                () -> deadLetterQueue.moveToDeadLetter(
                        failed,
                        null
                )
        );
    }

    @Test
    void moveToDeadLetterRejectsBlankReason() {
        Job job = Job.create(
                "test-job",Payload.empty()
        );

        Job failed = job
                .withStatus(JobStatus.QUEUED)
                .withStatus(JobStatus.PROCESSING)
                .withStatus(JobStatus.FAILED);

        assertThrows(
                IllegalArgumentException.class,
                () -> deadLetterQueue.moveToDeadLetter(
                        failed,
                        "   "
                )
        );
    }

    @Test
    void moveToDeadLetterRejectsNonFailedJob() {
        Job job = Job.create(
                "test-job",Payload.empty()
        );

        assertThrows(
                IllegalStateException.class,
                () -> deadLetterQueue.moveToDeadLetter(
                        job,
                        "failure"
                )
        );
    }

    @Test
    void listDeadJobsRejectsNegativeOffset() {
        assertThrows(
                IllegalArgumentException.class,
                () -> deadLetterQueue.listDeadJobs(-1, 10)
        );
    }

    @Test
    void listDeadJobsRejectsNegativeLimit() {
        assertThrows(
                IllegalArgumentException.class,
                () -> deadLetterQueue.listDeadJobs(0, -1)
        );
    }

    @Test
    void listDeadJobsReturnsEmptyForZeroLimit() {
        assertTrue(
                deadLetterQueue.listDeadJobs(0, 0).isEmpty()
        );
    }

    @Test
    void listDeadJobsReturnsDeadJobsWithoutRemovingThem() {
        Job first = Job.create("test-job", Payload.empty())
                .withStatus(JobStatus.QUEUED)
                .withStatus(JobStatus.PROCESSING)
                .withStatus(JobStatus.FAILED);

        Job second = Job.create("test-job", Payload.empty())
                .withStatus(JobStatus.QUEUED)
                .withStatus(JobStatus.PROCESSING)
                .withStatus(JobStatus.FAILED);

        repository.save(first);
        repository.save(second);

        deadLetterQueue.moveToDeadLetter(first, "first failure");
        deadLetterQueue.moveToDeadLetter(second, "second failure");

        List<Job> jobs = deadLetterQueue.listDeadJobs(0, 10);

        assertEquals(2, jobs.size());
        assertEquals(2, deadLetterQueue.size());
    }

    @Test
    void listDeadJobsSupportsOffsetAndLimit() {
        Job first = Job.create("test-job", Payload.empty())
                .withStatus(JobStatus.QUEUED)
                .withStatus(JobStatus.PROCESSING)
                .withStatus(JobStatus.FAILED);

        Job second = Job.create("test-job", Payload.empty())
                .withStatus(JobStatus.QUEUED)
                .withStatus(JobStatus.PROCESSING)
                .withStatus(JobStatus.FAILED);

        Job third = Job.create("test-job", Payload.empty())
                .withStatus(JobStatus.QUEUED)
                .withStatus(JobStatus.PROCESSING)
                .withStatus(JobStatus.FAILED);

        repository.save(first);
        repository.save(second);
        repository.save(third);

        deadLetterQueue.moveToDeadLetter(first, "first");
        deadLetterQueue.moveToDeadLetter(second, "second");
        deadLetterQueue.moveToDeadLetter(third, "third");

        List<Job> jobs = deadLetterQueue.listDeadJobs(1, 1);

        assertEquals(1, jobs.size());
    }

    @Test
    void requeueMovesDeadJobBackToQueuedAndResetsRetryCount() {
        Job job = Job.create("test-job", Payload.empty())
                .withStatus(JobStatus.QUEUED)
                .withStatus(JobStatus.PROCESSING)
                .withStatus(JobStatus.FAILED);

        repository.save(job);

        deadLetterQueue.moveToDeadLetter(
                job,
                "permanent failure"
        );

        Job deadJob = repository
                .findById(job.id())
                .orElseThrow();

        assertEquals(JobStatus.DEAD, deadJob.status());

        Optional<Job> result = deadLetterQueue.requeue(
                job.id(),
                "default"
        );

        assertTrue(result.isPresent());

        Job requeued = result.get();

        assertEquals(JobStatus.QUEUED, requeued.status());
        assertEquals(0, requeued.retryCount());
        assertEquals(deadJob.maxRetries(), requeued.maxRetries());
    }


    @Test
    void requeueRemovesJobFromDeadLetterQueue() {
        Job job = Job.create("test-job", Payload.empty())
                .withStatus(JobStatus.QUEUED)
                .withStatus(JobStatus.PROCESSING)
                .withStatus(JobStatus.FAILED);

        repository.save(job);

        deadLetterQueue.moveToDeadLetter(
                job,
                "permanent failure"
        );

        assertEquals(1, deadLetterQueue.size());

        Optional<Job> result = deadLetterQueue.requeue(
                job.id(),
                "default"
        );

        assertTrue(result.isPresent());
        assertEquals(0, deadLetterQueue.size());
    }


    @Test
    void requeueAddsJobToTargetQueue() {
        Job job = Job.create("test-job", Payload.empty())
                .withStatus(JobStatus.QUEUED)
                .withStatus(JobStatus.PROCESSING)
                .withStatus(JobStatus.FAILED);

        repository.save(job);

        deadLetterQueue.moveToDeadLetter(
                job,
                "permanent failure"
        );

        Optional<Job> result = deadLetterQueue.requeue(
                job.id(),
                "default"
        );

        assertTrue(result.isPresent());

        assertEquals(
                1,
                client.llen("jobstream:queue:default")
        );

        assertEquals(
                job.id().toString(),
                client.rpop("jobstream:queue:default")
        );
    }


    @Test
    void requeuePersistsQueuedStatus() {
        Job job = Job.create("test-job", Payload.empty())
                .withStatus(JobStatus.QUEUED)
                .withStatus(JobStatus.PROCESSING)
                .withStatus(JobStatus.FAILED);

        repository.save(job);

        deadLetterQueue.moveToDeadLetter(
                job,
                "permanent failure"
        );

        deadLetterQueue.requeue(
                job.id(),
                "default"
        );

        Job restored = repository
                .findById(job.id())
                .orElseThrow();

        assertEquals(
                JobStatus.QUEUED,
                restored.status()
        );
    }


    @Test
    void requeuePreservesFailureDiagnostics() {
        Job job = Job.create("test-job", Payload.empty())
                .withStatus(JobStatus.QUEUED)
                .withStatus(JobStatus.PROCESSING)
                .withStatus(JobStatus.FAILED)
                .withMetadata("failure.type", "test");

        repository.save(job);

        deadLetterQueue.moveToDeadLetter(
                job,
                "permanent failure"
        );

        Job deadJob = repository
                .findById(job.id())
                .orElseThrow();

        Optional<Job> result = deadLetterQueue.requeue(
                job.id(),
                "default"
        );

        assertTrue(result.isPresent());

        Job requeued = result.get();

        assertEquals(
                deadJob.lastErrorReason(),
                requeued.lastErrorReason()
        );

        assertEquals(
                deadJob.lastFailedAt(),
                requeued.lastFailedAt()
        );

        assertEquals(
                deadJob.metadata(),
                requeued.metadata()
        );
    }


    @Test
    void requeueReturnsEmptyWhenJobDoesNotExist() {
        Job missingJob = Job.create(
                "test-job",
                Payload.empty()
        );

        Optional<Job> result = deadLetterQueue.requeue(
                missingJob.id(),
                "default"
        );

        assertTrue(result.isEmpty());
    }


    @Test
    void requeueReturnsEmptyWhenJobIsNotDead() {
        Job job = Job.create(
                "test-job",
                Payload.empty()
        );

        repository.save(job);

        Optional<Job> result = deadLetterQueue.requeue(
                job.id(),
                "default"
        );

        assertTrue(result.isEmpty());
    }


    @Test
    void requeueRejectsNullJobId() {
        assertThrows(
                IllegalArgumentException.class,
                () -> deadLetterQueue.requeue(
                        null,
                        "default"
                )
        );
    }


    @Test
    void requeueRejectsNullTargetQueue() {
        Job job = Job.create(
                "test-job",
                Payload.empty()
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> deadLetterQueue.requeue(
                        job.id(),
                        null
                )
        );
    }


    @Test
    void requeueRejectsBlankTargetQueue() {
        Job job = Job.create(
                "test-job",
                Payload.empty()
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> deadLetterQueue.requeue(
                        job.id(),
                        "   "
                )
        );
    }

    @Test
    void purgeRemovesAllDeadLetterJobs() {
        Job first = Job.create("test-job", Payload.empty())
                .withStatus(JobStatus.QUEUED)
                .withStatus(JobStatus.PROCESSING)
                .withStatus(JobStatus.FAILED);

        Job second = Job.create("test-job", Payload.empty())
                .withStatus(JobStatus.QUEUED)
                .withStatus(JobStatus.PROCESSING)
                .withStatus(JobStatus.FAILED);

        repository.save(first);
        repository.save(second);

        deadLetterQueue.moveToDeadLetter(first, "first failure");
        deadLetterQueue.moveToDeadLetter(second, "second failure");

        assertEquals(2, deadLetterQueue.size());

        deadLetterQueue.purge();

        assertEquals(0, deadLetterQueue.size());
    }
}