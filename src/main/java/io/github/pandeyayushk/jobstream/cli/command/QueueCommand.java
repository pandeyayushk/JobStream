package io.github.pandeyayushk.jobstream.cli.command;

import io.github.pandeyayushk.jobstream.cli.CliContext;
import io.github.pandeyayushk.jobstream.cli.JobStreamCli;
import io.github.pandeyayushk.jobstream.cli.format.TableFormatter;
import io.github.pandeyayushk.jobstream.job.JobId;
import io.github.pandeyayushk.jobstream.queue.JobQueue;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

@Command(
        name = "queue",
        description = "Inspect JobStream queues.",
        mixinStandardHelpOptions = true,
        subcommands = {
                QueueCommand.SizeCommand.class,
                QueueCommand.PeekCommand.class
        }
)
public final class QueueCommand implements Runnable {

    @ParentCommand
    private JobStreamCli root;

    CliContext context() {
        return root.context();
    }

    @Override
    public void run() {
        System.out.println(
                "Use 'jobstream queue --help' for available commands."
        );
    }

    @Command(
            name = "size",
            description = "Show the number of jobs waiting in a queue.",
            mixinStandardHelpOptions = true
    )
    public static final class SizeCommand implements Callable<Integer> {

        @ParentCommand
        private QueueCommand parent;

        @Parameters(
                index = "0",
                description = "Queue name."
        )
        private String queueName;

        @Override
        public Integer call() {
            JobQueue queue = parent.context().jobQueue();
            long size = queue.size(queueName);

            System.out.println("Queue: " + queueName);
            System.out.println("Size: " + size);

            return 0;
        }
    }

    @Command(
            name = "peek",
            description = "Show queued job IDs without removing them.",
            mixinStandardHelpOptions = true
    )
    public static final class PeekCommand implements Callable<Integer> {

        @ParentCommand
        private QueueCommand parent;

        @Parameters(
                index = "0",
                description = "Queue name."
        )
        private String queueName;

        @Option(
                names = "--limit",
                description = "Maximum number of job IDs to show. Defaults to the full queue."
        )
        private Integer limit;

        @Override
        public Integer call() {
            JobQueue queue = parent.context().jobQueue();
            long size = queue.size(queueName);
            int peekCount = peekCount(size);

            List<JobId> jobIds = queue.peek(queueName, peekCount);

            System.out.println("QUEUE: " + queueName);

            if (jobIds.isEmpty()) {
                System.out.println("Queue is empty.");
                return 0;
            }

            List<List<String>> rows = new ArrayList<>();
            for (JobId jobId : jobIds) {
                rows.add(List.of(jobId.toString()));
            }

            System.out.print(TableFormatter.format(List.of("JOB ID"), rows));
            return 0;
        }

        private int peekCount(long size) {
            if (limit != null) {
                return limit;
            }
            if (size > Integer.MAX_VALUE) {
                return Integer.MAX_VALUE;
            }
            return (int) size;
        }
    }
}
