package io.github.pandeyayushk.jobstream.metrics;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProcessLocalJobMetricsTest {
    @Test
    void countersStartAtZeroAndSnapshotHasStableImmutableMetricMap() {
        ProcessLocalJobMetrics metrics = new ProcessLocalJobMetrics();

        assertEquals(0, metrics.snapshot().jobsSubmittedTotal());
        assertEquals(List.of(
                        "jobs_submitted_total",
                        "jobs_completed_total",
                        "jobs_failed_total",
                        "jobs_retried_total",
                        "jobs_moved_to_dlq_total"
                ), new ArrayList<>(metrics.snapshot().asMap().keySet()));

        metrics.recordJobSubmitted();
        metrics.recordJobCompleted();
        metrics.recordJobFailed();
        metrics.recordJobRetried();
        metrics.recordJobMovedToDlq();
        JobMetricsSnapshot snapshot = metrics.snapshot();

        assertEquals(new JobMetricsSnapshot(1, 1, 1, 1, 1), snapshot);
        assertThrows(UnsupportedOperationException.class,
                () -> snapshot.asMap().put("jobs_submitted_total", 8L));
    }

    @Test
    void concurrentIncrementsAreNotLost() throws Exception {
        ProcessLocalJobMetrics metrics = new ProcessLocalJobMetrics();
        int threads = 8;
        int incrementsPerThread = 12_500;
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch start = new CountDownLatch(1);
        try (var executor = Executors.newFixedThreadPool(threads)) {
            for (int thread = 0; thread < threads; thread++) {
                executor.submit(() -> {
                    ready.countDown();
                    start.await();
                    for (int i = 0; i < incrementsPerThread; i++) {
                        metrics.recordJobFailed();
                    }
                    return null;
                });
            }
            ready.await();
            start.countDown();
        }

        assertEquals((long) threads * incrementsPerThread,
                metrics.snapshot().jobsFailedTotal());
    }
}
