package io.github.pandeyayushk.jobstream.cli;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.concurrent.Callable;

@Command(
        name = "jobstream",
        description = "Command-line interface for JobStream.",
        mixinStandardHelpOptions = true,
        version = "JobStream 1.0-SNAPSHOT"
)
public final class JobStreamCli implements Callable<Integer> {

    @Option(
            names = "--host",
            description = "Redis host.",
            defaultValue = "localhost"
    )
    private String host;

    @Option(
            names = "--port",
            description = "Redis port.",
            defaultValue = "6379"
    )
    private int port;

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

    public static void main(String[] args) {
        int exitCode = new CommandLine(new JobStreamCli()).execute(args);
        System.exit(exitCode);
    }
}