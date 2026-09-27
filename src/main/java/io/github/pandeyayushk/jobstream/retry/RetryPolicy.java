package io.github.pandeyayushk.jobstream.retry;

import io.github.pandeyayushk.jobstream.job.Job;

import java.time.Duration;

public interface RetryPolicy {
    boolean shouldRetry(Job job, Throwable cause);
    Duration computeBackoff(Job job);
}
