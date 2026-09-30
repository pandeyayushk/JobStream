package io.github.pandeyayushk.jobstream.cli.command;

import io.github.pandeyayushk.jobstream.cli.JobStreamCli;
import io.github.pandeyayushk.jobstream.job.JobId;
import io.github.pandeyayushk.jobstream.queue.RedisJobQueue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.RedisClient;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

class QueueCommandIntegrationTest {

    private static final String REDIS_URI = "redis://localhost:6379";

    private RedisClient client;
    private RedisJobQueue queue;

    private PrintStream originalOut;
    private PrintStream originalErr;
    private ByteArrayOutputStream output;
    private ByteArrayOutputStream errorOutput;

    @BeforeEach
    void setUp() {
        client = RedisClient.create(REDIS_URI);
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
    void reportsEmptyQueueSize() {
        JobStreamCli cli = new JobStreamCli();

        int exitCode = JobStreamCli.createCommandLine(cli).execute(
                "queue",
                "size",
                "default"
        );

        assertEquals(0, exitCode);
        String result = output.toString();
        assertTrue(result.contains("Queue: default"));
        assertTrue(result.contains("Size: 0"));

        cli.close();
    }

    @Test
    void reportsQueueSizeAfterSubmissions() {
        JobStreamCli cli = new JobStreamCli();

        submitJob(cli, "emails");
        submitJob(cli, "emails");
        output.reset();
        errorOutput.reset();

        int exitCode = JobStreamCli.createCommandLine(cli).execute(
                "queue",
                "size",
                "emails"
        );

        assertEquals(0, exitCode);
        assertTrue(output.toString().contains("Queue: emails"));
        assertTrue(output.toString().contains("Size: 2"));
        assertEquals(2L, queue.size("emails"));

        cli.close();
    }

    @Test
    void peeksEmptyQueueWithoutError() {
        JobStreamCli cli = new JobStreamCli();

        int exitCode = JobStreamCli.createCommandLine(cli).execute(
                "queue",
                "peek",
                "default"
        );

        assertEquals(0, exitCode);
        String result = output.toString();
        assertTrue(result.contains("QUEUE: default"));
        assertTrue(result.contains("Queue is empty."));

        cli.close();
    }

    @Test
    void peeksPopulatedQueueInFifoOrder() {
        JobStreamCli cli = new JobStreamCli();

        String first = submitJob(cli, "default");
        String second = submitJob(cli, "default");
        String third = submitJob(cli, "default");
        output.reset();
        errorOutput.reset();

        int exitCode = JobStreamCli.createCommandLine(cli).execute(
                "queue",
                "peek",
                "default"
        );

        assertEquals(0, exitCode);
        String result = output.toString();
        assertTrue(result.contains("QUEUE: default"));
        assertTrue(result.contains("JOB ID"));
        assertTrue(result.contains(first));
        assertTrue(result.contains(second));
        assertTrue(result.contains(third));

        int firstIndex = result.indexOf(first);
        int secondIndex = result.indexOf(second);
        int thirdIndex = result.indexOf(third);
        assertTrue(firstIndex < secondIndex);
        assertTrue(secondIndex < thirdIndex);

        cli.close();
    }

    @Test
    void peekDoesNotRemoveJobs() {
        JobStreamCli cli = new JobStreamCli();

        String jobId = submitJob(cli, "default");
        output.reset();
        errorOutput.reset();

        int exitCode = JobStreamCli.createCommandLine(cli).execute(
                "queue",
                "peek",
                "default"
        );

        assertEquals(0, exitCode);
        assertEquals(1L, queue.size("default"));
        assertEquals(
                JobId.fromString(jobId),
                queue.peek("default", 1).getFirst()
        );

        cli.close();
    }

    @Test
    void rejectsMissingQueueNameForSize() {
        JobStreamCli cli = new JobStreamCli();

        int exitCode = JobStreamCli.createCommandLine(cli).execute(
                "queue",
                "size"
        );

        assertNotEquals(0, exitCode);
        String combined = output.toString() + errorOutput.toString();
        assertTrue(combined.contains("Error:"));

        cli.close();
    }

    @Test
    void rejectsMissingQueueNameForPeek() {
        JobStreamCli cli = new JobStreamCli();

        int exitCode = JobStreamCli.createCommandLine(cli).execute(
                "queue",
                "peek"
        );

        assertNotEquals(0, exitCode);

        cli.close();
    }

    @Test
    void rejectsNegativePeekLimit() {
        JobStreamCli cli = new JobStreamCli();

        int exitCode = JobStreamCli.createCommandLine(cli).execute(
                "queue",
                "peek",
                "default",
                "--limit",
                "-1"
        );

        assertNotEquals(0, exitCode);
        String combined = output.toString() + errorOutput.toString();
        assertTrue(combined.contains("Count can not be negative"));

        cli.close();
    }

    private String submitJob(JobStreamCli cli, String queueName) {
        output.reset();
        errorOutput.reset();

        int exitCode = JobStreamCli.createCommandLine(cli).execute(
                "job",
                "submit",
                "--type", "email:send",
                "--payload", "{\"to\":\"dev@example.com\"}",
                "--queue", queueName
        );
        assertEquals(0, exitCode);
        return extractJobId(output.toString());
    }

    private String extractJobId(String text) {
        Pattern pattern = Pattern.compile("Job ID:\\s+([0-9a-fA-F-]{36})");
        Matcher matcher = pattern.matcher(text);
        assertTrue(matcher.find(), "Expected a Job ID in: " + text);
        return matcher.group(1);
    }
}
