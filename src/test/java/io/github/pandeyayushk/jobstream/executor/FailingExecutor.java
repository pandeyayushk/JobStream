package io.github.pandeyayushk.jobstream.executor;

import io.github.pandeyayushk.jobstream.job.Job;

import java.util.Objects;

public class FailingExecutor implements JobExecutor {

    private final String failureMessage;

    public FailingExecutor(String failureMessage) {
        this.failureMessage = Objects.requireNonNull(
                failureMessage,
                "Failure message cannot be null"
        );
    }

    @Override
    public ExecutionResult execute(Job job) {
        return ExecutionResult.failure(failureMessage);
    }
}