package io.github.pandeyayushk.jobstream.executor;

import io.github.pandeyayushk.jobstream.job.Job;

public class NoOpExecutor implements JobExecutor {

    @Override
    public ExecutionResult execute(Job job) {
        return ExecutionResult.success();
    }
}