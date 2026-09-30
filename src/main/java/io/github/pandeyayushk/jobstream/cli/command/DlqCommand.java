package io.github.pandeyayushk.jobstream.cli.command;

import io.github.pandeyayushk.jobstream.cli.CliContext;
import io.github.pandeyayushk.jobstream.cli.JobStreamCli;
import io.github.pandeyayushk.jobstream.cli.format.TableFormatter;
import io.github.pandeyayushk.jobstream.job.Job;
import io.github.pandeyayushk.jobstream.job.JobId;
import io.github.pandeyayushk.jobstream.retry.DeadLetterQueue;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;

@Command(
        name = "dlq",
        description = "Inspect and manage the dead letter queue.",
        mixinStandardHelpOptions = true,
        subcommands = {
                DlqCommand.ListCommand.class,
                DlqCommand.RequeueCommand.class,
                DlqCommand.PurgeCommand.class
        }
)
public final class DlqCommand implements Runnable {

    @ParentCommand
    private JobStreamCli root;

    CliContext context() {
        return root.context();
    }

    @Override
    public void run() {
        System.out.println(
                "Use 'jobstream dlq --help' for available commands."
        );
    }

    @Command(
            name = "list",
            description = "List jobs in the dead letter queue.",
            mixinStandardHelpOptions = true
    )
    public static final class ListCommand implements Callable<Integer> {

        @ParentCommand
        private DlqCommand parent;

        @Option(
                names = "--offset",
                defaultValue = "0",
                description = "Number of dead jobs to skip."
        )
        private int offset;

        @Option(
                names = "--limit",
                defaultValue = "100",
                description = "Maximum number of dead jobs to show."
        )
        private int limit;

        @Override
        public Integer call() {
            DeadLetterQueue deadLetterQueue = parent.context().deadLetterQueue();
            List<Job> jobs = deadLetterQueue.listDeadJobs(offset, limit);

            if (jobs.isEmpty()) {
                System.out.println("Dead letter queue is empty.");
                return 0;
            }

            List<List<String>> rows = new ArrayList<>();
            for (Job job : jobs) {
                rows.add(List.of(
                        job.id().toString(),
                        job.status().name(),
                        Integer.toString(job.retryCount()),
                        failureReason(job),
                        job.updatedAt().toString()
                ));
            }

            System.out.print(
                    TableFormatter.format(
                            List.of("JOB ID", "STATUS", "RETRIES", "REASON", "UPDATED"),
                            rows
                    )
            );
            return 0;
        }

        private static String failureReason(Job job) {
            return job.lastErrorReason()
                    .or(() -> Optional.ofNullable(job.metadata().get("failure.reason")))
                    .orElse("");
        }
    }

    @Command(
            name = "requeue",
            description = "Move a dead job back onto a target queue.",
            mixinStandardHelpOptions = true
    )
    public static final class RequeueCommand implements Callable<Integer> {

        @ParentCommand
        private DlqCommand parent;

        @Parameters(
                index = "0",
                description = "Job ID."
        )
        private String jobId;

        @Option(
                names = "--target-queue",
                required = true,
                description = "Queue that should receive the requeued job."
        )
        private String targetQueue;

        @Override
        public Integer call() {
            JobId id;
            try {
                id = JobId.fromString(jobId);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                        "Invalid job ID: " + jobId,
                        e
                );
            }

            Optional<Job> requeued = parent.context()
                    .deadLetterQueue()
                    .requeue(id, targetQueue);

            if (requeued.isEmpty()) {
                throw new IllegalArgumentException(
                        "Job not found in dead letter queue: " + jobId
                );
            }

            Job job = requeued.get();
            System.out.println("Job requeued successfully.");
            System.out.println("Job ID: " + job.id());
            System.out.println("Queue: " + targetQueue);
            System.out.println("Status: " + job.status());
            System.out.println("Retry count: " + job.retryCount());
            return 0;
        }
    }

    @Command(
            name = "purge",
            description = "Remove all jobs from the dead letter queue.",
            mixinStandardHelpOptions = true
    )
    public static final class PurgeCommand implements Callable<Integer> {

        @ParentCommand
        private DlqCommand parent;

        @Override
        public Integer call() {
            DeadLetterQueue deadLetterQueue = parent.context().deadLetterQueue();
            long size = deadLetterQueue.size();
            if (size == 0) {
                System.out.println("Dead letter queue is empty.");
                return 0;
            }

            System.out.print(
                    "Are you sure you want to purge " + size + " job(s) from the dead letter queue? (y/N): "
            );
            System.out.flush();

            String response;
            try {
                BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
                response = reader.readLine();
            } catch (Exception e) {
                System.out.println();
                System.out.println("Purge cancelled.");
                return 0;
            }

            if (response == null || (!response.trim().equalsIgnoreCase("y") && !response.trim().equalsIgnoreCase("yes"))) {
                System.out.println("Purge cancelled.");
                return 0;
            }

            deadLetterQueue.purge();

            System.out.println(
                    "Purged " + size + " job(s) from the dead letter queue."
            );
            return 0;
        }
    }
}
