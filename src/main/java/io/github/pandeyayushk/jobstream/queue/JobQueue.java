package io.github.pandeyayushk.jobstream.queue;

import io.github.pandeyayushk.jobstream.job.JobId;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

public interface JobQueue {
    void enqueue(JobId jobId, String queueName);
    Optional<JobId> dequeue(String queueName, Duration timeout);
    Optional<JobId> dequeueNonBlocking(String queueName);
    long size(String queueName);
    List<JobId> peek(String queueName, int count);
}
