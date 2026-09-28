package io.github.pandeyayushk.jobstream.retry;

import io.github.pandeyayushk.jobstream.job.Job;
import io.github.pandeyayushk.jobstream.payload.Payload;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FixedDelayRetryPolicyTest {

    @Test
    void retriesWhileRetryCountIsBelowMaximum() {
        RetryPolicy policy =
                new FixedDelayRetryPolicy(Duration.ofSeconds(5));

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
                new FixedDelayRetryPolicy(Duration.ofSeconds(5));

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

        assertEquals(
                job.maxRetries(),
                job.retryCount()
        );

        assertFalse(
                policy.shouldRetry(
                        job,
                        new RuntimeException("failure")
                )
        );
    }

    @Test
    void returnsConfiguredDelay() {
        Duration delay = Duration.ofSeconds(10);

        RetryPolicy policy =
                new FixedDelayRetryPolicy(delay);

        Job job = Job.create(
                "test-job",
                Payload.empty()
        );

        assertEquals(
                delay,
                policy.computeBackoff(job)
        );
    }

    @Test
    void rejectsNullDelay() {
        assertThrows(
                NullPointerException.class,
                () -> new FixedDelayRetryPolicy(null)
        );
    }

    @Test
    void rejectsNegativeDelay() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new FixedDelayRetryPolicy(
                        Duration.ofSeconds(-1)
                )
        );
    }
}