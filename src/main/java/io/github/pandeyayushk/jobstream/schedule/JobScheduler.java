package io.github.pandeyayushk.jobstream.schedule;

import io.github.pandeyayushk.jobstream.job.JobId;

import java.time.Duration;
import java.time.Instant;

public interface JobScheduler {

    void schedule(JobId jobId, Instant executeAt, String targetQueue);

    void scheduleDelayed(JobId jobId, Duration delay, String targetQueue);

    long scheduledCount();
}
