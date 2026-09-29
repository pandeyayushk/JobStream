package io.github.pandeyayushk.jobstream.cli.command;

import io.github.pandeyayushk.jobstream.cli.CliContext;
import io.github.pandeyayushk.jobstream.cli.JobStreamCli;
import io.github.pandeyayushk.jobstream.job.Job;
import io.github.pandeyayushk.jobstream.job.JobId;
import io.github.pandeyayushk.jobstream.payload.Payload;
import io.github.pandeyayushk.jobstream.persistence.JobRepository;
import io.github.pandeyayushk.jobstream.queue.QueueCoordinator;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;

import java.util.Map;
import java.util.concurrent.Callable;

@Command(
        name = "job",
        description = "Manage JobStream jobs.",
        subcommands = {
                JobCommand.SubmitCommand.class,
                JobCommand.StatusCommand.class
        }
)
public final class JobCommand implements Runnable {

    @ParentCommand
    private JobStreamCli root;

    CliContext context() {
        return root.context();
    }

    @Override
    public void run() {
        System.out.println(
                "Use 'jobstream job --help' for available commands."
        );
    }

    @Command(
            name = "submit",
            description = "Submit a new job."
    )
    public static final class SubmitCommand
            implements Callable<Integer> {

        @ParentCommand
        private JobCommand parent;

        @Option(
                names = "--type",
                required = true,
                description = "Job type."
        )
        private String type;

        @Option(
                names = "--payload",
                required = true,
                description = "Job payload as JSON object."
        )
        private String payloadJson;

        @Option(
                names = "--queue",
                defaultValue = "default",
                description = "Target queue."
        )
        private String queueName;

        @Override
        public Integer call() {
            Map<String, Object> payloadMap =
                    parsePayload(payloadJson);

            Payload payload = Payload.of(payloadMap);
            Job job = Job.create(type, payload);

            QueueCoordinator queueCoordinator =
                    parent.context().queueCoordinator();

            queueCoordinator.submit(job, queueName);

            System.out.println("Job submitted successfully.");
            System.out.println("Job ID: " + job.id());
            System.out.println("Queue: " + queueName);

            return 0;
        }

        private Map<String, Object> parsePayload(String json) {
            try {
                tools.jackson.databind.ObjectMapper mapper =
                        new tools.jackson.databind.ObjectMapper();

                return mapper.readValue(json, Map.class);
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        "Invalid JSON payload: " + e.getMessage(),
                        e
                );
            }
        }
    }

    @Command(
            name = "status",
            description = "Show the status of a job."
    )
    public static final class StatusCommand
            implements Callable<Integer> {

        @ParentCommand
        private JobCommand parent;

        @Parameters(
                index = "0",
                description = "Job ID."
        )
        private String jobId;

        @Override
        public Integer call() {
            JobRepository jobRepository =
                    parent.context().jobRepository();

            JobId id;

            try {
                id = JobId.fromString(jobId);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                        "Invalid job ID: " + jobId,
                        e
                );
            }

            Job job = jobRepository.findById(id)
                    .orElseThrow(() ->
                            new IllegalArgumentException(
                                    "Job not found: " + jobId
                            )
                    );

            System.out.println("Job ID: " + job.id());
            System.out.println("Type: " + job.type());
            System.out.println("Status: " + job.status());
            System.out.println("Retry count: " + job.retryCount());
            System.out.println("Max retries: " + job.maxRetries());
            System.out.println("Created: " + job.createdAt());
            System.out.println("Updated: " + job.updatedAt());

            job.lastErrorReason().ifPresent(
                    reason ->
                            System.out.println(
                                    "Last error: " + reason
                            )
            );

            job.lastFailedAt().ifPresent(
                    failedAt ->
                            System.out.println(
                                    "Last failed: " + failedAt
                            )
            );

            return 0;
        }
    }
}