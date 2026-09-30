package io.github.pandeyayushk.jobstream.cli.command;

import io.github.pandeyayushk.jobstream.cli.JobStreamCli;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CliErrorHandlingIntegrationTest {

    private PrintStream originalOut;
    private PrintStream originalErr;
    private ByteArrayOutputStream output;
    private ByteArrayOutputStream errorOutput;

    @BeforeEach
    void setUp() {
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
    }

    @Test
    void reportsRedisUnavailableWithoutStackTrace() {
        JobStreamCli cli = new JobStreamCli();

        int exitCode = JobStreamCli.createCommandLine(cli).execute(
                "--host", "127.0.0.1",
                "--port", "1",
                "queue",
                "size",
                "default"
        );

        assertNotEquals(0, exitCode);
        String combined = output.toString() + errorOutput.toString();
        assertTrue(combined.contains("Unable to connect to Redis") || combined.contains("Error:"));
        assertFalse(combined.contains("at io.github.pandeyayushk.jobstream"));

        cli.close();
    }

    @Test
    void rejectsPortOutsideTcpRange() {
        JobStreamCli cli = new JobStreamCli();

        int exitCode = JobStreamCli.createCommandLine(cli).execute(
                "--port", "70000",
                "queue",
                "size",
                "default"
        );

        assertNotEquals(0, exitCode);
        String combined = output.toString() + errorOutput.toString();
        assertTrue(combined.contains("Redis port is outside TCP range"));

        cli.close();
    }
}
