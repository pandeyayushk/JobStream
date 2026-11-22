package io.github.pandeyayushk.jobstream.schedule;

/** Moves due scheduled jobs into their ready queues without executing them. */
public interface SchedulerPoller {

    SchedulerPollResult pollOnce(int batchSize);
}
