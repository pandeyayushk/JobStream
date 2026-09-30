package io.github.pandeyayushk.jobstream.cli.command;

import io.github.pandeyayushk.jobstream.cli.JobStreamCli;
import io.github.pandeyayushk.jobstream.worker.RedisWorkerRegistry;
import io.github.pandeyayushk.jobstream.worker.WorkerConfig;
import io.github.pandeyayushk.jobstream.worker.WorkerId;
import io.github.pandeyayushk.jobstream.worker.WorkerInfo;
import io.github.pandeyayushk.jobstream.worker.WorkerStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.RedisClient;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkerCommandIntegrationTest {

    private static final String REDIS_URI = "redis://localhost:6379";

    private RedisClient client;
    private RedisWorkerRegistry registry;

    private PrintStream originalOut;
    private PrintStream originalErr;
    private ByteArrayOutputStream output;
    private ByteArrayOutputStream errorOutput;

    @BeforeEach
    void setUp() {
        client = RedisClient.create(REDIS_URI);
        registry = new RedisWorkerRegistry(
                client,
                WorkerConfig.defaults().heartbeatTtl()
        );
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
    void listsEmptyWorkerRegistry() {
        JobStreamCli cli = new JobStreamCli();

        int exitCode = JobStreamCli.createCommandLine(cli).execute(
                "worker",
                "list"
        );

        assertEquals(0, exitCode);
        assertTrue(output.toString().contains("No registered workers."));

        cli.close();
    }

    @Test
    void listsOneRegisteredWorker() {
        WorkerId workerId = WorkerId.generate();
        Instant startedAt = Instant.parse("2026-01-01T00:00:00Z");
        registry.register(new WorkerInfo(workerId, WorkerStatus.RUNNING, startedAt));

        JobStreamCli cli = new JobStreamCli();

        int exitCode = JobStreamCli.createCommandLine(cli).execute(
                "worker",
                "list"
        );

        assertEquals(0, exitCode);
        String result = output.toString();
        assertTrue(result.contains("WORKER ID"));
        assertTrue(result.contains(workerId.toString()));
        assertTrue(result.contains("RUNNING"));
        assertTrue(result.contains(startedAt.toString()));

        cli.close();
    }

    @Test
    void listsMultipleRegisteredWorkers() {
        WorkerId first = WorkerId.generate();
        WorkerId second = WorkerId.generate();
        registry.register(new WorkerInfo(first, WorkerStatus.RUNNING, Instant.now()));
        registry.register(new WorkerInfo(second, WorkerStatus.STARTING, Instant.now()));

        JobStreamCli cli = new JobStreamCli();

        int exitCode = JobStreamCli.createCommandLine(cli).execute(
                "worker",
                "list"
        );

        assertEquals(0, exitCode);
        String result = output.toString();
        assertTrue(result.contains(first.toString()));
        assertTrue(result.contains(second.toString()));
        assertTrue(result.contains("RUNNING"));
        assertTrue(result.contains("STARTING"));
        assertTrue(result.contains("STARTED AT"));

        cli.close();
    }

    @Test
    void listDoesNotStartOrStopWorkers() {
        WorkerId workerId = WorkerId.generate();
        registry.register(new WorkerInfo(workerId, WorkerStatus.RUNNING, Instant.now()));

        JobStreamCli cli = new JobStreamCli();
        int exitCode = JobStreamCli.createCommandLine(cli).execute(
                "worker",
                "list"
        );

        assertEquals(0, exitCode);
        assertTrue(registry.getWorker(workerId).isPresent());
        assertEquals(
                WorkerStatus.RUNNING,
                registry.getWorker(workerId).orElseThrow().status()
        );

        cli.close();
    }

    @Test
    void closesCliContextAfterListingWorkers() {
        JobStreamCli cli = new JobStreamCli();
        assertEquals(0, JobStreamCli.createCommandLine(cli).execute("worker", "list"));
        cli.close();

        JobStreamCli second = new JobStreamCli();
        assertEquals(0, JobStreamCli.createCommandLine(second).execute("worker", "list"));
        second.close();

        assertFalse(output.toString().contains("Exception"));
    }
}
