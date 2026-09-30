package io.github.pandeyayushk.jobstream.cli;

import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JobStreamCliTest {

    @Test
    void usesDefaultRedisConfiguration() {
        JobStreamCli cli = new JobStreamCli();

        int exitCode = JobStreamCli.createCommandLine(cli).execute();

        assertEquals(0, exitCode);
        assertEquals("localhost", cli.getHost());
        assertEquals(6379, cli.getPort());
    }

    @Test
    void parsesCustomRedisConfiguration() {
        JobStreamCli cli = new JobStreamCli();

        int exitCode = JobStreamCli.createCommandLine(cli).execute(
                "--host", "redis.example.com",
                "--port", "6380"
        );

        assertEquals(0, exitCode);
        assertEquals("redis.example.com", cli.getHost());
        assertEquals(6380, cli.getPort());
    }

    @Test
    void supportsHelpOptionWithoutConnectingToRedis() {
        JobStreamCli cli = new JobStreamCli();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommandLine commandLine = JobStreamCli.createCommandLine(cli);
        commandLine.setOut(new PrintWriter(out, true));

        int exitCode = commandLine.execute("--help");

        assertEquals(0, exitCode);
        String help = out.toString();
        assertTrue(help.contains("jobstream"));
        assertTrue(help.contains("job"));
        assertTrue(help.contains("queue"));
        assertTrue(help.contains("worker"));
        assertTrue(help.contains("dlq"));
    }

    @Test
    void supportsVersionOptionWithoutConnectingToRedis() {
        JobStreamCli cli = new JobStreamCli();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommandLine commandLine = JobStreamCli.createCommandLine(cli);
        commandLine.setOut(new PrintWriter(out, true));

        int exitCode = commandLine.execute("--version");

        assertEquals(0, exitCode);
        assertTrue(out.toString().contains("JobStream 1.0-SNAPSHOT"));
    }

    @Test
    void showsJobHelpWithoutConnectingToRedis() {
        assertSubcommandHelp("job", "submit", "status");
    }

    @Test
    void showsQueueHelpWithoutConnectingToRedis() {
        assertSubcommandHelp("queue", "size", "peek");
    }

    @Test
    void showsWorkerHelpWithoutConnectingToRedis() {
        assertSubcommandHelp("worker", "list");
    }

    @Test
    void showsDlqHelpWithoutConnectingToRedis() {
        assertSubcommandHelp("dlq", "list", "requeue", "purge");
    }

    @Test
    void rejectsUnknownOption() {
        JobStreamCli cli = new JobStreamCli();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        CommandLine commandLine = JobStreamCli.createCommandLine(cli);
        commandLine.setErr(new PrintWriter(err, true));

        int exitCode = commandLine.execute("--unknown");

        assertNotEquals(0, exitCode);
        assertTrue(err.toString().contains("Error:"));
    }

    @Test
    void rejectsUnknownCommand() {
        JobStreamCli cli = new JobStreamCli();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        CommandLine commandLine = JobStreamCli.createCommandLine(cli);
        commandLine.setErr(new PrintWriter(err, true));

        int exitCode = commandLine.execute("not-a-command");

        assertNotEquals(0, exitCode);
        assertTrue(err.toString().contains("Error:"));
    }

    @Test
    void rejectsInvalidPortValue() {
        JobStreamCli cli = new JobStreamCli();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        CommandLine commandLine = JobStreamCli.createCommandLine(cli);
        commandLine.setErr(new PrintWriter(err, true));

        int exitCode = commandLine.execute("--port", "not-a-number");

        assertNotEquals(0, exitCode);
        assertTrue(err.toString().contains("Error:"));
    }

    @Test
    void exposesCliContext() {
        JobStreamCli cli = new JobStreamCli();

        JobStreamCli.createCommandLine(cli).execute();

        assertNotNull(cli.context());
        cli.close();
    }

    private static void assertSubcommandHelp(String command, String... expected) {
        JobStreamCli cli = new JobStreamCli();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommandLine commandLine = JobStreamCli.createCommandLine(cli);
        commandLine.setOut(new PrintWriter(out, true));

        int exitCode = commandLine.execute(command, "--help");

        assertEquals(0, exitCode);
        String help = out.toString();
        for (String token : expected) {
            assertTrue(help.contains(token), () -> help + " missing " + token);
        }
    }
}
