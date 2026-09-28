package io.github.pandeyayushk.jobstream.retry;

import io.github.pandeyayushk.jobstream.job.Job;
import io.github.pandeyayushk.jobstream.job.JobId;

import java.util.List;
import java.util.Optional;

public class NoOpDeadLetterQueue implements DeadLetterQueue {

    @Override
    public void moveToDeadLetter(Job job, String reason) {
        // Intentionally does nothing.
    }

    @Override
    public List<Job> listDeadJobs(int offset, int limit) {
        return List.of();
    }

    @Override
    public Optional<Job> requeue(JobId jobId, String targetQueue) {
        return Optional.empty();
    }

    @Override
    public void purge() {
        // Intentionally does nothing.
    }

    @Override
    public long size() {
        return 0;
    }
}