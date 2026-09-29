package io.github.pandeyayushk.jobstream.cli;

import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JobStreamCliTest {

    @Test
    void usesDefaultRedisConfiguration() {
        JobStreamCli cli = new JobStreamCli();

        int exitCode = new CommandLine(cli).execute();

        assertEquals(0, exitCode);
        assertEquals("localhost", cli.getHost());
        assertEquals(6379, cli.getPort());
    }

    @Test
    void parsesCustomRedisConfiguration() {
        JobStreamCli cli = new JobStreamCli();

        int exitCode = new CommandLine(cli).execute(
                "--host", "redis.example.com",
                "--port", "6380"
        );

        assertEquals(0, exitCode);
        assertEquals("redis.example.com", cli.getHost());
        assertEquals(6380, cli.getPort());
    }

    @Test
    void supportsHelpOption() {
        JobStreamCli cli = new JobStreamCli();

        int exitCode = new CommandLine(cli).execute("--help");

        assertEquals(0, exitCode);
    }

    @Test
    void supportsVersionOption() {
        JobStreamCli cli = new JobStreamCli();

        int exitCode = new CommandLine(cli).execute("--version");

        assertEquals(0, exitCode);
    }

    @Test
    void rejectsUnknownOption() {
        JobStreamCli cli = new JobStreamCli();

        int exitCode = new CommandLine(cli).execute("--unknown");

        assertTrue(exitCode != 0);
    }

    @Test
    void exposesCliContext() {
        JobStreamCli cli = new JobStreamCli();

        new CommandLine(cli).execute();

        assertNotNull(cli.context());

        cli.context().close();
    }
}