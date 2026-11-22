package io.github.pandeyayushk.jobstream.schedule;

/** Summary of one bounded scheduler poll. */
public record SchedulerPollResult(int examined, int released, int cleaned, int deferred) {
}
