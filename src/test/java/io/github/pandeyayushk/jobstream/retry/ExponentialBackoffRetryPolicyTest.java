package io.github.pandeyayushk.jobstream.retry;

import io.github.pandeyayushk.jobstream.job.Job;
import io.github.pandeyayushk.jobstream.payload.Payload;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExponentialBackoffRetryPolicyTest {

    @Test
    void retriesWhileRetryCountIsBelowMaximum() {
        RetryPolicy policy =
                new ExponentialBackoffRetryPolicy(
                        Duration.ofSeconds(1),
                        Duration.ofSeconds(30)
                );

        Job job = Job.create(
                "test-job",
                Payload.empty()
        );

        assertTrue(
                policy.shouldRetry(
                        job,
                        new RuntimeException("failure")
                )
        );
    }

    @Test
    void stopsRetryingWhenMaximumRetriesAreReached() {
        RetryPolicy policy =
                new ExponentialBackoffRetryPolicy(
                        Duration.ofSeconds(1),
                        Duration.ofSeconds(30)
                );

        Job job = Job.create(
                "test-job",
                Payload.empty()
        );

        for (int i = 0; i < job.maxRetries(); i++) {
            job = job
                    .withStatus(io.github.pandeyayushk.jobstream.job.JobStatus.QUEUED)
                    .withStatus(io.github.pandeyayushk.jobstream.job.JobStatus.PROCESSING)
                    .withStatus(io.github.pandeyayushk.jobstream.job.JobStatus.FAILED);

            job = job.withRetryAttempt("failure");
        }

        job = job
                .withStatus(io.github.pandeyayushk.jobstream.job.JobStatus.QUEUED)
                .withStatus(io.github.pandeyayushk.jobstream.job.JobStatus.PROCESSING)
                .withStatus(io.github.pandeyayushk.jobstream.job.JobStatus.FAILED);

        assertFalse(
                policy.shouldRetry(
                        job,
                        new RuntimeException("failure")
                )
        );
    }

    @Test
    void computesPositiveBackoff() {
        RetryPolicy policy =
                new ExponentialBackoffRetryPolicy(
                        Duration.ofSeconds(1),
                        Duration.ofSeconds(30)
                );

        Job job = Job.create(
                "test-job",
                Payload.empty()
        );

        Duration backoff = policy.computeBackoff(job);

        assertTrue(backoff.compareTo(Duration.ZERO) > 0);
    }

    @Test
    void rejectsNullBaseBackoff() {
        assertThrows(
                NullPointerException.class,
                () -> new ExponentialBackoffRetryPolicy(
                        null,
                        Duration.ofSeconds(30)
                )
        );
    }

    @Test
    void rejectsNullMaxBackoff() {
        assertThrows(
                NullPointerException.class,
                () -> new ExponentialBackoffRetryPolicy(
                        Duration.ofSeconds(1),
                        null
                )
        );
    }

    @Test
    void rejectsZeroBaseBackoff() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new ExponentialBackoffRetryPolicy(
                        Duration.ZERO,
                        Duration.ofSeconds(30)
                )
        );
    }

    @Test
    void rejectsZeroMaxBackoff() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new ExponentialBackoffRetryPolicy(
                        Duration.ofSeconds(1),
                        Duration.ZERO
                )
        );
    }

    @Test
    void rejectsBaseBackoffGreaterThanMaximum() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new ExponentialBackoffRetryPolicy(
                        Duration.ofSeconds(30),
                        Duration.ofSeconds(10)
                )
        );
    }

    @Test
    void backoffFallsWithinJitterRangeForInitialAttempt() {
        Duration base = Duration.ofSeconds(10);

        RetryPolicy policy =
                new ExponentialBackoffRetryPolicy(
                        base,
                        Duration.ofMinutes(1)
                );

        Job job = Job.create(
                "test-job",
                Payload.empty()
        );

        Duration backoff = policy.computeBackoff(job);

        Duration minimum =
                Duration.ofMillis(5_000);

        Duration maximum =
                Duration.ofMillis(15_000);

        assertTrue(
                !backoff.minus(minimum).isNegative(),
                "Backoff should be at least 0.5x base"
        );

        assertTrue(
                !backoff.minus(maximum).isPositive(),
                "Backoff should be at most 1.5x base"
        );
    }

    @Test
    void backoffRangeGrowsExponentiallyWithRetryAttempt() {
        Duration base = Duration.ofSeconds(10);

        RetryPolicy policy =
                new ExponentialBackoffRetryPolicy(
                        base,
                        Duration.ofMinutes(10)
                );

        Job job = Job.create(
                "test-job",
                Payload.empty()
        );

        Duration firstBackoff =
                policy.computeBackoff(job);

        Job failedJob = job
                .withStatus(io.github.pandeyayushk.jobstream.job.JobStatus.QUEUED)
                .withStatus(io.github.pandeyayushk.jobstream.job.JobStatus.PROCESSING)
                .withStatus(io.github.pandeyayushk.jobstream.job.JobStatus.FAILED);

        Job retryingJob =
                failedJob.withRetryAttempt("failure");

        Duration secondBackoff =
                policy.computeBackoff(retryingJob);

        assertTrue(
                firstBackoff.compareTo(Duration.ofSeconds(5)) >= 0
        );

        assertTrue(
                firstBackoff.compareTo(Duration.ofSeconds(15)) <= 0
        );

        assertTrue(
                secondBackoff.compareTo(Duration.ofSeconds(10)) >= 0
        );

        assertTrue(
                secondBackoff.compareTo(Duration.ofSeconds(30)) <= 0
        );
    }

    @Test
    void exponentialBackoffUsesMaximumCapBeforeJitter() {
        Duration base = Duration.ofSeconds(10);
        Duration max = Duration.ofSeconds(20);

        RetryPolicy policy =
                new ExponentialBackoffRetryPolicy(
                        base,
                        max
                );

        Job job = Job.create(
                "test-job",
                Payload.empty()
        );

        for (int i = 0; i < 3; i++) {
            job = job
                    .withStatus(io.github.pandeyayushk.jobstream.job.JobStatus.QUEUED)
                    .withStatus(io.github.pandeyayushk.jobstream.job.JobStatus.PROCESSING)
                    .withStatus(io.github.pandeyayushk.jobstream.job.JobStatus.FAILED);

            job = job.withRetryAttempt("failure");
        }

        Duration backoff =
                policy.computeBackoff(job);

        assertTrue(
                backoff.compareTo(Duration.ofSeconds(10)) >= 0
        );

        assertTrue(
                backoff.compareTo(Duration.ofSeconds(30)) <= 0
        );
    }
}