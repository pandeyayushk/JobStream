package io.github.pandeyayushk.jobstream.retry;

import io.github.pandeyayushk.jobstream.job.Job;

import java.time.Duration;

public final class NoRetryPolicy implements RetryPolicy {

    @Override
    public boolean shouldRetry(Job job, Throwable cause) {
        return false;
    }

    @Override
    public Duration computeBackoff(Job job) {
        return Duration.ZERO;
    }
}