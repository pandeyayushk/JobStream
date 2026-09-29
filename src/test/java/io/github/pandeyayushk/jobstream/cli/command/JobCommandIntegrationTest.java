package io.github.pandeyayushk.jobstream.cli.command;

import io.github.pandeyayushk.jobstream.cli.JobStreamCli;
import io.github.pandeyayushk.jobstream.job.Job;
import io.github.pandeyayushk.jobstream.persistence.JobRepository;
import io.github.pandeyayushk.jobstream.queue.RedisJobQueue;
import io.github.pandeyayushk.jobstream.queue.RedisJobSubmissionStore;
import io.github.pandeyayushk.jobstream.serialization.JacksonJobSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;
import redis.clients.jedis.RedisClient;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

class JobCommandIntegrationTest {

    private static final String REDIS_URI = "redis://localhost:6379";

    private RedisClient client;
    private JobRepository jobRepository;
    private RedisJobQueue queue;

    private PrintStream originalOut;
    private PrintStream originalErr;

    private ByteArrayOutputStream output;
    private ByteArrayOutputStream errorOutput;

    @BeforeEach
    void setUp() {
        client = RedisClient.create(REDIS_URI);

        JacksonJobSerializer serializer = new JacksonJobSerializer();

        jobRepository = new io.github.pandeyayushk.jobstream.persistence.RedisJobRepository(
                client,
                serializer
        );

        queue = new RedisJobQueue(client);

        client.flushDB();

        originalOut = System.out;
        originalErr = System.err;

        output = new ByteArrayOutputStream();
        errorOutput = new ByteArrayOutputStream();

        System.setOut(new PrintStream(output));
        System.setErr(new PrintStream(errorOutput));
    }

    @AfterEach
    void tearDown() {
        System.setOut(originalOut);
        System.setErr(originalErr);

        client.flushDB();
        client.close();
    }

    @Test
    void submitsJobSuccessfully() {
        JobStreamCli cli = new JobStreamCli();

        int exitCode = new CommandLine(cli).execute(
                "job",
                "submit",
                "--type", "email:send",
                "--payload", "{\"to\":\"dev@example.com\"}"
        );

        assertEquals(0, exitCode);

        String result = output.toString();

        assertTrue(result.contains("Job submitted successfully."));
        assertTrue(result.contains("Queue: default"));
        assertTrue(result.contains("Job ID:"));

        String jobId = extractJobId(result);

        Optional<Job> storedJob =
                jobRepository.findById(
                        io.github.pandeyayushk.jobstream.job.JobId.fromString(jobId)
                );

        assertTrue(storedJob.isPresent());

        Job job = storedJob.orElseThrow();

        assertEquals("email:send", job.type());
        assertEquals("QUEUED", job.status().name());

        assertEquals(1L, queue.size("default"));

        cli.context().close();
    }

    @Test
    void submitsJobToSpecifiedQueue() {
        JobStreamCli cli = new JobStreamCli();

        int exitCode = new CommandLine(cli).execute(
                "job",
                "submit",
                "--type", "email:send",
                "--payload", "{\"to\":\"dev@example.com\"}",
                "--queue", "emails"
        );

        assertEquals(0, exitCode);

        String result = output.toString();

        assertTrue(result.contains("Job submitted successfully."));
        assertTrue(result.contains("Queue: emails"));

        assertEquals(1L, queue.size("emails"));
        assertEquals(0L, queue.size("default"));

        cli.context().close();
    }

    @Test
    void statusDisplaysStoredJob() {
        JobStreamCli cli = new JobStreamCli();

        int submitExitCode = new CommandLine(cli).execute(
                "job",
                "submit",
                "--type", "email:send",
                "--payload", "{\"to\":\"dev@example.com\"}"
        );

        assertEquals(0, submitExitCode);

        String submitOutput = output.toString();
        String jobId = extractJobId(submitOutput);

        output.reset();
        errorOutput.reset();

        int statusExitCode = new CommandLine(cli).execute(
                "job",
                "status",
                jobId
        );

        assertEquals(0, statusExitCode);

        String statusOutput = output.toString();

        assertTrue(statusOutput.contains("Job ID: " + jobId));
        assertTrue(statusOutput.contains("Type: email:send"));
        assertTrue(statusOutput.contains("Status: QUEUED"));
        assertTrue(statusOutput.contains("Retry count:"));
        assertTrue(statusOutput.contains("Max retries:"));

        cli.context().close();
    }

    @Test
    void rejectsInvalidJsonPayload() {
        JobStreamCli cli = new JobStreamCli();

        int exitCode = new CommandLine(cli).execute(
                "job",
                "submit",
                "--type", "email:send",
                "--payload", "{invalid-json"
        );

        assertNotEquals(0, exitCode);

        String combinedOutput =
                output.toString() + errorOutput.toString();

        assertTrue(
                combinedOutput.contains("Invalid JSON payload")
                        || combinedOutput.contains("Invalid")
        );

        cli.context().close();
    }

    @Test
    void rejectsMissingJobType() {
        JobStreamCli cli = new JobStreamCli();

        int exitCode = new CommandLine(cli).execute(
                "job",
                "submit",
                "--payload", "{\"to\":\"dev@example.com\"}"
        );

        assertNotEquals(0, exitCode);

        cli.context().close();
    }

    @Test
    void rejectsMissingPayload() {
        JobStreamCli cli = new JobStreamCli();

        int exitCode = new CommandLine(cli).execute(
                "job",
                "submit",
                "--type", "email:send"
        );

        assertNotEquals(0, exitCode);

        cli.context().close();
    }

    @Test
    void rejectsUnknownJobId() {
        JobStreamCli cli = new JobStreamCli();

        String unknownJobId =
                "00000000-0000-0000-0000-000000000001";

        int exitCode = new CommandLine(cli).execute(
                "job",
                "status",
                unknownJobId
        );

        assertNotEquals(0, exitCode);

        String combinedOutput =
                output.toString() + errorOutput.toString();

        assertTrue(
                combinedOutput.contains("Job not found")
                        || combinedOutput.contains("not found")
        );

        cli.context().close();
    }

    @Test
    void rejectsInvalidJobId() {
        JobStreamCli cli = new JobStreamCli();

        int exitCode = new CommandLine(cli).execute(
                "job",
                "status",
                "not-a-valid-job-id"
        );

        assertNotEquals(0, exitCode);

        cli.context().close();
    }

    private String extractJobId(String output) {
        Pattern pattern =
                Pattern.compile(
                        "Job ID:\\s+([0-9a-fA-F-]{36})"
                );

        Matcher matcher = pattern.matcher(output);

        assertTrue(
                matcher.find(),
                "Expected output to contain a valid Job ID: " + output
        );

        return matcher.group(1);
    }
}