package io.github.pandeyayushk.jobstream.cli.command;

import io.github.pandeyayushk.jobstream.cli.JobStreamCli;
import io.github.pandeyayushk.jobstream.job.Job;
import io.github.pandeyayushk.jobstream.job.JobId;
import io.github.pandeyayushk.jobstream.job.JobStatus;
import io.github.pandeyayushk.jobstream.payload.Payload;
import io.github.pandeyayushk.jobstream.persistence.RedisJobRepository;
import io.github.pandeyayushk.jobstream.queue.RedisJobQueue;
import io.github.pandeyayushk.jobstream.retry.RedisDeadLetterQueue;
import io.github.pandeyayushk.jobstream.serialization.JacksonJobSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.RedisClient;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class DlqCommandIntegrationTest {

    private static final String REDIS_URI = "redis://localhost:6379";

    private RedisClient client;
    private RedisJobRepository repository;
    private RedisDeadLetterQueue deadLetterQueue;
    private RedisJobQueue queue;

    private PrintStream originalOut;
    private PrintStream originalErr;
    private InputStream originalIn;
    private ByteArrayOutputStream output;
    private ByteArrayOutputStream errorOutput;

    @BeforeEach
    void setUp() {
        client = RedisClient.create(REDIS_URI);
        JacksonJobSerializer serializer = new JacksonJobSerializer();
        repository = new RedisJobRepository(client, serializer);
        deadLetterQueue = new RedisDeadLetterQueue(client, serializer);
        queue = new RedisJobQueue(client);
        client.flushDB();

        originalOut = System.out;
        originalErr = System.err;
        originalIn = System.in;
        output = new ByteArrayOutputStream();
        errorOutput = new ByteArrayOutputStream();
        System.setOut(new PrintStream(output));
        System.setErr(new PrintStream(errorOutput));
    }

    @AfterEach
    void tearDown() {
        System.setOut(originalOut);
        System.setErr(originalErr);
        System.setIn(originalIn);
        client.flushDB();
        client.close();
    }

    @Test
    void listsEmptyDeadLetterQueue() {
        JobStreamCli cli = new JobStreamCli();

        int exitCode = JobStreamCli.createCommandLine(cli).execute("dlq", "list");

        assertEquals(0, exitCode);
        assertTrue(output.toString().contains("Dead letter queue is empty."));

        cli.close();
    }

    @Test
    void listsDeadJob() {
        Job dead = moveFailedJobToDlq("permanent failure");

        JobStreamCli cli = new JobStreamCli();
        int exitCode = JobStreamCli.createCommandLine(cli).execute("dlq", "list");

        assertEquals(0, exitCode);
        String result = output.toString();
        assertTrue(result.contains("JOB ID"));
        assertTrue(result.contains(dead.id().toString()));
        assertTrue(result.contains("DEAD"));
        assertTrue(result.contains("permanent failure"));
        assertTrue(result.contains("RETRIES"));

        cli.close();
    }

    @Test
    void requeuesDeadJobOntoTargetQueue() {
        Job dead = moveFailedJobToDlq("permanent failure");

        JobStreamCli cli = new JobStreamCli();
        int exitCode = JobStreamCli.createCommandLine(cli).execute(
                "dlq",
                "requeue",
                dead.id().toString(),
                "--target-queue",
                "recovery"
        );

        assertEquals(0, exitCode);
        String result = output.toString();
        assertTrue(result.contains("Job requeued successfully."));
        assertTrue(result.contains("Job ID: " + dead.id()));
        assertTrue(result.contains("Queue: recovery"));
        assertTrue(result.contains("Status: QUEUED"));

        assertEquals(0L, deadLetterQueue.size());
        assertEquals(1L, queue.size("recovery"));
        assertEquals(dead.id(), queue.peek("recovery", 1).getFirst());

        Job restored = repository.findById(dead.id()).orElseThrow();
        assertEquals(JobStatus.QUEUED, restored.status());

        cli.close();
    }

    @Test
    void requeueResetsRetryCountUsingExistingDlqBehavior() {
        Job job = Job.create("email:send", Payload.empty())
                .withStatus(JobStatus.QUEUED)
                .withStatus(JobStatus.PROCESSING)
                .withStatus(JobStatus.FAILED)
                .withRetryAttempt("temporary failure")
                .withStatus(JobStatus.QUEUED)
                .withStatus(JobStatus.PROCESSING)
                .withStatus(JobStatus.FAILED);

        repository.save(job);
        deadLetterQueue.moveToDeadLetter(job, "permanent failure");

        Job dead = repository.findById(job.id()).orElseThrow();
        assertEquals(1, dead.retryCount());

        JobStreamCli cli = new JobStreamCli();
        int exitCode = JobStreamCli.createCommandLine(cli).execute(
                "dlq",
                "requeue",
                job.id().toString(),
                "--target-queue",
                "default"
        );

        assertEquals(0, exitCode);
        assertTrue(output.toString().contains("Retry count: 0"));

        Job restored = repository.findById(job.id()).orElseThrow();
        assertEquals(0, restored.retryCount());
        assertEquals(JobStatus.QUEUED, restored.status());
        assertEquals("permanent failure", restored.metadata().get("failure.reason"));

        cli.close();
    }

    @Test
    void requeueRejectsJobMissingFromDlq() {
        String missingId = JobId.generate().toString();
        JobStreamCli cli = new JobStreamCli();

        int exitCode = JobStreamCli.createCommandLine(cli).execute(
                "dlq",
                "requeue",
                missingId,
                "--target-queue",
                "default"
        );

        assertNotEquals(0, exitCode);
        String combined = output.toString() + errorOutput.toString();
        assertTrue(combined.contains("Job not found in dead letter queue"));

        cli.close();
    }

    @Test
    void requeueRejectsInvalidJobId() {
        JobStreamCli cli = new JobStreamCli();

        int exitCode = JobStreamCli.createCommandLine(cli).execute(
                "dlq",
                "requeue",
                "not-a-valid-job-id",
                "--target-queue",
                "default"
        );

        assertNotEquals(0, exitCode);
        String combined = output.toString() + errorOutput.toString();
        assertTrue(combined.contains("Invalid job ID"));

        cli.close();
    }

    @Test
    void requeueRequiresTargetQueue() {
        JobStreamCli cli = new JobStreamCli();

        int exitCode = JobStreamCli.createCommandLine(cli).execute(
                "dlq",
                "requeue",
                JobId.generate().toString()
        );

        assertNotEquals(0, exitCode);
        String combined = output.toString() + errorOutput.toString();
        assertTrue(combined.contains("Error:"));

        cli.close();
    }

    @Test
    void purgesPopulatedDeadLetterQueue() {
        moveFailedJobToDlq("first failure");
        moveFailedJobToDlq("second failure");
        assertEquals(2L, deadLetterQueue.size());

        JobStreamCli cli = new JobStreamCli();
        int exitCode = JobStreamCli.createCommandLine(cli).execute("dlq", "purge");

        assertEquals(0, exitCode);
        assertTrue(output.toString().contains("Purged 2 job(s) from the dead letter queue."));
        assertEquals(0L, deadLetterQueue.size());

        cli.close();
    }

    @Test
    void purgesEmptyDeadLetterQueue() {
        JobStreamCli cli = new JobStreamCli();
        int exitCode = JobStreamCli.createCommandLine(cli).execute("dlq", "purge");

        assertEquals(0, exitCode);
        assertTrue(output.toString().contains("Purged 0 job(s) from the dead letter queue."));

        cli.close();
    }

    @Test
    void rejectsInvalidIntegerLimit() {
        JobStreamCli cli = new JobStreamCli();

        int exitCode = JobStreamCli.createCommandLine(cli).execute(
                "dlq",
                "list",
                "--limit",
                "abc"
        );

        assertNotEquals(0, exitCode);
        String combined = output.toString() + errorOutput.toString();
        assertTrue(combined.contains("Error:"));

        cli.close();
    }

    private Job moveFailedJobToDlq(String reason) {
        Job job = Job.create("email:send", Payload.empty())
                .withStatus(JobStatus.QUEUED)
                .withStatus(JobStatus.PROCESSING)
                .withStatus(JobStatus.FAILED);
        repository.save(job);
        deadLetterQueue.moveToDeadLetter(job, reason);
        return repository.findById(job.id()).orElseThrow();
    }
}
