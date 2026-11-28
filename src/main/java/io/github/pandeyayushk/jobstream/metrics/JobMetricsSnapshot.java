package io.github.pandeyayushk.jobstream.metrics;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Immutable snapshot of one process's operational job counters. */
public record JobMetricsSnapshot(
        long jobsSubmittedTotal,
        long jobsCompletedTotal,
        long jobsFailedTotal,
        long jobsRetriedTotal,
        long jobsMovedToDlqTotal
) {
    /** Returns the counters using their stable metric names and declaration order. */
    public Map<String, Long> asMap() {
        Map<String, Long> counters = new LinkedHashMap<>();
        counters.put("jobs_submitted_total", jobsSubmittedTotal);
        counters.put("jobs_completed_total", jobsCompletedTotal);
        counters.put("jobs_failed_total", jobsFailedTotal);
        counters.put("jobs_retried_total", jobsRetriedTotal);
        counters.put("jobs_moved_to_dlq_total", jobsMovedToDlqTotal);
        return Collections.unmodifiableMap(counters);
    }
}
