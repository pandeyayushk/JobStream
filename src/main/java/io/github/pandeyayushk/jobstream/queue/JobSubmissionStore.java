package io.github.pandeyayushk.jobstream.queue;

import io.github.pandeyayushk.jobstream.job.Job;

public interface JobSubmissionStore {
    void submit(Job job, Job queuedJob, String queueName);
}
