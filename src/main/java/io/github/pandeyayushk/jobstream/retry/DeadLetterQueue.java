package io.github.pandeyayushk.jobstream.retry;

import io.github.pandeyayushk.jobstream.job.Job;
import io.github.pandeyayushk.jobstream.job.JobId;

import java.util.List;
import java.util.Optional;

public interface DeadLetterQueue {

    void moveToDeadLetter(Job job, String reason);

    List<Job> listDeadJobs(int offset, int limit);

    Optional<Job> requeue(JobId jobId, String targetQueue);

    void purge();

    long size();
}