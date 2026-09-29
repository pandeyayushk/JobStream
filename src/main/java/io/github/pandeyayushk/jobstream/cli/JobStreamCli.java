package io.github.pandeyayushk.jobstream.cli;

import io.github.pandeyayushk.jobstream.cli.command.JobCommand;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.concurrent.Callable;

@Command(
        name = "jobstream",
        description = "Command-line interface for JobStream.",
        mixinStandardHelpOptions = true,
        version = "JobStream 1.0-SNAPSHOT",
        subcommands = {
                JobCommand.class
        }
)
public final class JobStreamCli implements Callable<Integer> {

    @Option(
            names = "--host",
            description = "Redis host.",
            defaultValue = "localhost"
    )
    private String host = "localhost";

    @Option(
            names = "--port",
            description = "Redis port.",
            defaultValue = "6379"
    )
    private int port = 6379;

    private CliContext context;

    @Override
    public Integer call() {
        return 0;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public CliContext context() {
        if (context == null) {
            context = new CliContext(host, port);
        }

        return context;
    }

    public static void main(String[] args) {
        JobStreamCli cli = new JobStreamCli();

        try {
            int exitCode =
                    new CommandLine(cli).execute(args);

            System.exit(exitCode);
        } finally {
            cli.close();
        }
    }

    private void close() {
        if (context != null) {
            context.close();
        }
    }
}