package io.github.pandeyayushk.jobstream.retry;

import io.github.pandeyayushk.jobstream.job.Job;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

public final class ExponentialBackoffRetryPolicy implements RetryPolicy {

    private final Duration baseBackoff;
    private final Duration maxBackoff;

    public ExponentialBackoffRetryPolicy(
            Duration baseBackoff,
            Duration maxBackoff
    ) {
        this.baseBackoff = Objects.requireNonNull(
                baseBackoff,
                "Base backoff cannot be null"
        );

        this.maxBackoff = Objects.requireNonNull(
                maxBackoff,
                "Max backoff cannot be null"
        );

        if (baseBackoff.isNegative() || baseBackoff.isZero()) {
            throw new IllegalArgumentException(
                    "Base backoff must be greater than zero"
            );
        }

        if (maxBackoff.isNegative() || maxBackoff.isZero()) {
            throw new IllegalArgumentException(
                    "Max backoff must be greater than zero"
            );
        }

        if (baseBackoff.compareTo(maxBackoff) > 0) {
            throw new IllegalArgumentException(
                    "Base backoff cannot exceed max backoff"
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

        long attempt = job.retryCount();

        double exponentialMultiplier = Math.pow(2.0, attempt);

        double exponentialDelay =
                baseBackoff.toMillis() * exponentialMultiplier;

        double cappedDelay =
                Math.min(
                        exponentialDelay,
                        maxBackoff.toMillis()
                );

        double jitterMultiplier =
                ThreadLocalRandom.current().nextDouble(0.5, 1.5);

        long jitteredDelay =
                Math.round(cappedDelay * jitterMultiplier);

        return Duration.ofMillis(jitteredDelay);
    }
}