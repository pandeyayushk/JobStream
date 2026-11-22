package io.github.pandeyayushk.jobstream.cli;

import io.github.pandeyayushk.jobstream.job.Job;
import io.github.pandeyayushk.jobstream.job.JobStatus;
import io.github.pandeyayushk.jobstream.payload.Payload;
import io.github.pandeyayushk.jobstream.queue.RedisJobQueue;
import io.github.pandeyayushk.jobstream.retry.RedisDeadLetterQueue;
import io.github.pandeyayushk.jobstream.serialization.JacksonJobSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.RedisClient;

import java.io.ByteArrayOutputStream;
import java.io.BufferedReader;
import java.io.PrintStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class InteractiveShellTest {

    private RedisClient redis;
    private RedisJobQueue queue;
    private RedisDeadLetterQueue deadLetterQueue;
    private PrintStream originalOut;
    private ByteArrayOutputStream output;

    @BeforeEach
    void setUp() {
        redis = RedisClient.create("redis://localhost:6379");
        redis.flushDB();
        queue = new RedisJobQueue(redis);
        deadLetterQueue = new RedisDeadLetterQueue(redis, new JacksonJobSerializer());
        originalOut = System.out;
        output = new ByteArrayOutputStream();
        System.setOut(new PrintStream(output, true, StandardCharsets.UTF_8));
    }

    @AfterEach
    void tearDown() {
        System.setOut(originalOut);
        redis.flushDB();
        redis.close();
    }

    @Test
    void startupHelpAndEofCloseTheSession() {
        JobStreamCli cli = new JobStreamCli();
        run(cli, "help\n");

        String text = output.toString(StandardCharsets.UTF_8);
        assertTrue(text.contains("JOBSTREAM"));
        assertTrue(text.contains("Connected to Redis at localhost:6379"));
        assertTrue(text.contains("submit <type> <json>"));
        assertTrue(text.contains("Type 'help'"));
    }

    @Test
    void submitsNestedJsonAndRunsSeveralCommandsInOneSession() {
        JobStreamCli cli = new JobStreamCli();
        run(cli,
                "submit email {\"recipient\":\"test@example.com\",\"metadata\":{\"source\":\"manual test\",\"labels\":[\"a\",\"b\"]},\"quote\":\"say \\\"hello\\\"!\"}\n"
                        + "queue\nworkers\ndlq\nnot-a-command\nsubmit email\nquit\n");

        String text = output.toString(StandardCharsets.UTF_8);
        assertTrue(text.contains("Job submitted successfully."), text);
        assertTrue(text.contains("Queue: default"), text);
        assertTrue(text.contains("Size: 1"), text);
        assertTrue(text.contains("No registered workers."), text);
        assertTrue(text.contains("Dead letter queue is empty."), text);
        assertTrue(text.contains("Unknown command: not-a-command"), text);
        assertTrue(text.contains("Usage: submit <type> <json>"), text);
        assertTrue(text.contains("Goodbye."), text);
        assertEquals(1L, queue.size("default"));
        assertEquals(0L, queue.size("other"));
    }

    @Test
    void statusAndNamedQueueShowDetailsAndIds() {
        JobStreamCli cli = new JobStreamCli();
        InteractiveShell shell = new InteractiveShell(cli,
                new BufferedReader(new StringReader("exit\n")), System.out);
        assertTrue(shell.executeLine("submit task {\"text\":\"with spaces\"}"));
        String submitted = output.toString(StandardCharsets.UTF_8);
        String id = submitted.substring(submitted.indexOf("Job ID: ") + 8).split("\\R")[0];
        assertTrue(shell.executeLine("status " + id));
        assertTrue(shell.executeLine("queue named"));

        Job named = Job.create("task", Payload.empty()).withStatus(JobStatus.QUEUED);
        new io.github.pandeyayushk.jobstream.persistence.RedisJobRepository(redis, new JacksonJobSerializer()).save(named);
        queue.enqueue(named.id(), "named");
        assertTrue(shell.executeLine("queue named"));

        String text = output.toString(StandardCharsets.UTF_8);
        assertTrue(text.contains("Status: QUEUED"), text);
        assertTrue(text.contains("JOB ID"), text);
        assertTrue(text.contains(named.id().toString()), text);
        run(cli, "");
    }

    @Test
    void requeuesToDefaultAndNamedQueuesAndHandlesPurgeConfirmation() {
        Job defaultJob = dead("default requeue");
        Job namedJob = dead("named requeue");
        Job purgeJob = dead("purge this");
        JobStreamCli cli = new JobStreamCli();
        InteractiveShell shell = new InteractiveShell(cli,
                new BufferedReader(new StringReader("yes\n")), System.out);

        assertTrue(shell.executeLine("requeue " + defaultJob.id()));
        assertTrue(shell.executeLine("requeue " + namedJob.id() + " recovery"));
        assertTrue(shell.executeLine("dlq"));
        assertTrue(shell.executeLine("purge"));

        String text = output.toString(StandardCharsets.UTF_8);
        assertTrue(text.contains("Queue: default"), text);
        assertTrue(text.contains("Queue: recovery"), text);
        assertTrue(text.contains(purgeJob.id().toString()), text);
        assertEquals(1L, queue.size("default"));
        assertEquals(1L, queue.size("recovery"));
        assertEquals(0L, deadLetterQueue.size());
        run(cli, "");
    }

    @Test
    void declinedPurgeAndMalformedInputReturnToPrompt() {
        dead("keep this");
        JobStreamCli cli = new JobStreamCli();
        run(cli, "purge\nno\nstatus\nsubmit task {\"broken\nhelp\nexit\n");

        String text = output.toString(StandardCharsets.UTF_8);
        assertTrue(text.contains("Purge cancelled."), text);
        assertTrue(text.contains("Usage: status <id>"), text);
        assertTrue(text.contains("Unclosed quotation mark"), text);
        assertTrue(text.contains("Available commands:"), text);
        assertEquals(1L, deadLetterQueue.size());
    }

    @Test
    void redisFailureDoesNotEndSession() {
        JobStreamCli failingCli = new JobStreamCli("127.0.0.1", 1);
        // A closed local port causes a connection failure without depending on Redis data.
        new InteractiveShell(failingCli, new BufferedReader(new StringReader("queue\nhelp\nexit\n")), System.out).run();

        String text = output.toString(StandardCharsets.UTF_8);
        assertTrue(text.contains("Redis connection error:"), text);
        assertTrue(text.contains("Available commands:"), text);
        assertTrue(text.contains("Goodbye."), text);
    }

    @Test
    void tokenizerPreservesQuotedWhitespaceAndEscapedQuotes() {
        List<String> tokens = InteractiveShell.tokenize(
                "submit email {\"message\":\"two words and \\\"quoted\\\"\",\"items\":[1, 2]}"
        );
        assertEquals(3, tokens.size());
        assertTrue(tokens.get(2).contains("two words"));
        assertTrue(tokens.get(2).contains("\\\"quoted\\\""));
        assertTrue(tokens.get(2).contains("[1, 2]"));
    }

    private void run(JobStreamCli cli, String input) {
        new InteractiveShell(cli, new BufferedReader(new StringReader(input)), System.out).run();
    }

    private Job dead(String reason) {
        Job job = Job.create("task", Payload.empty())
                .withStatus(JobStatus.QUEUED)
                .withStatus(JobStatus.PROCESSING)
                .withStatus(JobStatus.FAILED);
        deadLetterQueue.moveToDeadLetter(job, reason);
        return job;
    }

}
