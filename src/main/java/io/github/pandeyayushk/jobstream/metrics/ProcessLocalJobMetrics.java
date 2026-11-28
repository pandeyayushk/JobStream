package io.github.pandeyayushk.jobstream.metrics;

import java.util.concurrent.atomic.LongAdder;

/** Thread-safe, in-memory counters owned by one JobStream process. */
public final class ProcessLocalJobMetrics implements JobMetrics {
    private final LongAdder jobsSubmittedTotal = new LongAdder();
    private final LongAdder jobsCompletedTotal = new LongAdder();
    private final LongAdder jobsFailedTotal = new LongAdder();
    private final LongAdder jobsRetriedTotal = new LongAdder();
    private final LongAdder jobsMovedToDlqTotal = new LongAdder();

    @Override
    public void recordJobSubmitted() {
        jobsSubmittedTotal.increment();
    }

    @Override
    public void recordJobCompleted() {
        jobsCompletedTotal.increment();
    }

    @Override
    public void recordJobFailed() {
        jobsFailedTotal.increment();
    }

    @Override
    public void recordJobRetried() {
        jobsRetriedTotal.increment();
    }

    @Override
    public void recordJobMovedToDlq() {
        jobsMovedToDlqTotal.increment();
    }

    @Override
    public JobMetricsSnapshot snapshot() {
        return new JobMetricsSnapshot(
                jobsSubmittedTotal.sum(),
                jobsCompletedTotal.sum(),
                jobsFailedTotal.sum(),
                jobsRetriedTotal.sum(),
                jobsMovedToDlqTotal.sum()
        );
    }
}
