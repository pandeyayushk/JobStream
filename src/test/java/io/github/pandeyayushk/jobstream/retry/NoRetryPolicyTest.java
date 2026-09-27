package io.github.pandeyayushk.jobstream.retry;

import io.github.pandeyayushk.jobstream.job.Job;
import io.github.pandeyayushk.jobstream.payload.Payload;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;

class NoRetryPolicyTest {

    @Test
    void neverRetries() {
        RetryPolicy policy = new NoRetryPolicy();
        Job job = Job.create("test-job", Payload.empty());

        assertFalse(
                policy.shouldRetry(
                        job,
                        new RuntimeException("temporary failure")
                )
        );
    }

    @Test
    void hasZeroBackoff() {
        RetryPolicy policy = new NoRetryPolicy();
        Job job = Job.create("test-job", Payload.empty());

        assertEquals(
                Duration.ZERO,
                policy.computeBackoff(job)
        );
    }
}