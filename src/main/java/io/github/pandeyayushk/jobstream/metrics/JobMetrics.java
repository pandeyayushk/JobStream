package io.github.pandeyayushk.jobstream.metrics;

/** Process-scoped job counter events and an immutable counter snapshot. */
public interface JobMetrics {
    void recordJobSubmitted();

    void recordJobCompleted();

    void recordJobFailed();

    void recordJobRetried();

    void recordJobMovedToDlq();

    JobMetricsSnapshot snapshot();

    /** Metrics are best effort; a faulty recorder must not change job processing outcomes. */
    static void recordSafely(Runnable recording) {
        try {
            recording.run();
        } catch (RuntimeException ignored) {
            // The durable job operation has already succeeded; telemetry cannot reverse it.
        }
    }

    static JobMetrics noop() {
        return NoOpJobMetrics.INSTANCE;
    }
}
