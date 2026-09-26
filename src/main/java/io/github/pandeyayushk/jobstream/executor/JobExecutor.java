package io.github.pandeyayushk.jobstream.executor;

import io.github.pandeyayushk.jobstream.job.Job;

public interface JobExecutor {
    ExecutionResult execute(Job job);
}
