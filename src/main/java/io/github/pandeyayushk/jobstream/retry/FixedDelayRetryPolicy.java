package io.github.pandeyayushk.jobstream.retry;

import io.github.pandeyayushk.jobstream.job.Job;

import java.time.Duration;
import java.util.Objects;

public final class FixedDelayRetryPolicy implements RetryPolicy {

    private final Duration delay;

    public FixedDelayRetryPolicy(Duration delay) {
        this.delay = Objects.requireNonNull(
                delay,
                "Delay cannot be null"
        );

        if (delay.isNegative()) {
            throw new IllegalArgumentException(
                    "Delay cannot be negative"
            );
        }
    }

    @Override
    public boolean shouldRetry(Job job, Throwable cause) {
        Objects.requireNonNull(job, "Job cannot be null");
        Objects.requireNonNull(cause, "Failure cause cannot be null");

        return job.retryCount() < job.maxRetries();
    }

    @Override
    public Duration computeBackoff(Job job) {
        Objects.requireNonNull(job, "Job cannot be null");

        return delay;
    }
}