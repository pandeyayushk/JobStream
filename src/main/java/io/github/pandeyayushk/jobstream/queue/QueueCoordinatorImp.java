package io.github.pandeyayushk.jobstream.queue;

import io.github.pandeyayushk.jobstream.job.Job;
import io.github.pandeyayushk.jobstream.job.JobStatus;
import io.github.pandeyayushk.jobstream.metrics.JobMetrics;

import java.util.Objects;

public class QueueCoordinatorImp implements QueueCoordinator {

    private final JobSubmissionStore submissionStore;
    private final JobMetrics metrics;

    public QueueCoordinatorImp(JobSubmissionStore submissionStore) {
        this(submissionStore, JobMetrics.noop());
    }

    public QueueCoordinatorImp(JobSubmissionStore submissionStore, JobMetrics metrics) {
        this.submissionStore = Objects.requireNonNull(
                submissionStore,
                "JobSubmissionStore cannot be null"
        );
        this.metrics = Objects.requireNonNull(metrics, "JobMetrics cannot be null");
    }

    @Override
    public Job submit(Job job, String queueName) {
        Objects.requireNonNull(job, "Job cannot be null");

        if (queueName == null || queueName.isBlank()) {
            throw new IllegalArgumentException(
                    "Queue name cannot be null or blank"
            );
        }

        Job queuedJob = job.withStatus(JobStatus.QUEUED);

        Job persisted = submissionStore.submit(job, queuedJob, queueName);
        JobMetrics.recordSafely(metrics::recordJobSubmitted);
        return persisted;
    }
}
