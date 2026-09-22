package io.github.pandeyayushk.jobstream.queue;

import io.github.pandeyayushk.jobstream.job.Job;
import io.github.pandeyayushk.jobstream.job.JobStatus;

import java.util.Objects;

public class QueueCoordinatorImp implements QueueCoordinator {

    private final JobSubmissionStore submissionStore;

    public QueueCoordinatorImp(JobSubmissionStore submissionStore) {
        this.submissionStore = Objects.requireNonNull(
                submissionStore,
                "JobSubmissionStore cannot be null"
        );
    }

    @Override
    public void submit(Job job, String queueName) {
        Objects.requireNonNull(job, "Job cannot be null");

        if (queueName == null || queueName.isBlank()) {
            throw new IllegalArgumentException(
                    "Queue name cannot be null or blank"
            );
        }

        Job queuedJob = job.withStatus(JobStatus.QUEUED);

        submissionStore.submit(job,queuedJob, queueName);
    }
}