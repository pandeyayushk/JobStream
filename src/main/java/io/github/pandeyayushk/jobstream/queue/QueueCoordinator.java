package io.github.pandeyayushk.jobstream.queue;

import io.github.pandeyayushk.jobstream.job.Job;

public interface QueueCoordinator {

    Job submit(Job job, String queueName);
}
