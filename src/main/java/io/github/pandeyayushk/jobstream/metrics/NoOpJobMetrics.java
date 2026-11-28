package io.github.pandeyayushk.jobstream.metrics;

/** Immutable no-op recorder for components created without metrics wiring. */
final class NoOpJobMetrics implements JobMetrics {
    static final NoOpJobMetrics INSTANCE = new NoOpJobMetrics();
    private static final JobMetricsSnapshot ZERO = new JobMetricsSnapshot(0, 0, 0, 0, 0);

    private NoOpJobMetrics() {
    }

    @Override public void recordJobSubmitted() { }
    @Override public void recordJobCompleted() { }
    @Override public void recordJobFailed() { }
    @Override public void recordJobRetried() { }
    @Override public void recordJobMovedToDlq() { }
    @Override public JobMetricsSnapshot snapshot() { return ZERO; }
}
