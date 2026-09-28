package io.github.pandeyayushk.jobstream.retry;

import io.github.pandeyayushk.jobstream.job.Job;
import io.github.pandeyayushk.jobstream.job.JobId;
import io.github.pandeyayushk.jobstream.job.JobStatus;
import io.github.pandeyayushk.jobstream.serialization.JobSerializer;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.exceptions.JedisException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class RedisDeadLetterQueue implements DeadLetterQueue {

    private static final String DLQ_KEY =
            "jobstream:queue:dead-letter";

    private static final String JOB_KEY_PREFIX =
            "jobstream:job:";

    private static final String STATUS_KEY_PREFIX =
            "jobstream:status:";

    private final RedisClient client;
    private final JobSerializer serializer;

    public RedisDeadLetterQueue(
            RedisClient client,
            JobSerializer serializer
    ) {
        if (client == null) {
            throw new IllegalArgumentException("Redis client cannot be null");
        }

        if (serializer == null) {
            throw new IllegalArgumentException("Job serializer cannot be null");
        }

        this.client = client;
        this.serializer = serializer;
    }


    @Override
    public void moveToDeadLetter(Job job, String reason) {
        if (job == null) {
            throw new IllegalArgumentException("Job cannot be null");
        }

        if (reason == null) {
            throw new IllegalArgumentException("Failure reason cannot be null");
        }

        if (reason.isBlank()) {
            throw new IllegalArgumentException("Failure reason cannot be blank");
        }

        if (job.status() != JobStatus.FAILED) {
            throw new IllegalStateException(
                    "Only failed jobs can be moved to the dead letter queue"
            );
        }

        Job deadJob = job
                .withMetadata("failure.reason", reason)
                .withStatus(JobStatus.DEAD);

        String jobKey = JOB_KEY_PREFIX + deadJob.id();
        String statusKey = STATUS_KEY_PREFIX + JobStatus.DEAD;
        String oldStatusKey = STATUS_KEY_PREFIX + JobStatus.FAILED;

        String json = serializer.serialize(deadJob);

        try {
            var transaction = client.multi();

            transaction.set(jobKey, json);
            transaction.srem(oldStatusKey, deadJob.id().toString());
            transaction.sadd(statusKey, deadJob.id().toString());
            transaction.lpush(DLQ_KEY, deadJob.id().toString());

            transaction.exec();
        } catch (JedisException e) {
            throw new RetryException("Failed to move job to dead letter queue",e);
        }
    }

    @Override
    public List<Job> listDeadJobs(int offset, int limit) {
        if (offset < 0) {
            throw new IllegalArgumentException("Offset cannot be negative");
        }

        if (limit < 0) {
            throw new IllegalArgumentException("Limit cannot be negative");
        }

        if (limit == 0) {
            return List.of();
        }

        try {
            long end = (long) offset + limit - 1;

            List<String> jobIds = client.lrange(
                    DLQ_KEY,
                    offset,
                    end
            );

            List<Job> jobs = new ArrayList<>();

            for (String jobId : jobIds) {
                String jobKey = JOB_KEY_PREFIX + jobId;
                String json = client.get(jobKey);

                if (json != null) {
                    jobs.add(serializer.deserialize(json));
                }
            }

            return jobs;

        } catch (JedisException e) {
            throw new RetryException(
                    "Failed to list dead letter jobs",
                    e
            );
        }
    }

    @Override
    public Optional<Job> requeue(JobId jobId, String targetQueue) {
        if (jobId == null) {
            throw new IllegalArgumentException("Job id cannot be null");
        }

        if (targetQueue == null || targetQueue.isBlank()) {
            throw new IllegalArgumentException(
                    "Target queue cannot be null or blank"
            );
        }

        String jobIdValue = jobId.toString();
        String jobKey = JOB_KEY_PREFIX + jobIdValue;
        String deadStatusKey = STATUS_KEY_PREFIX + JobStatus.DEAD;
        String queuedStatusKey = STATUS_KEY_PREFIX + JobStatus.QUEUED;
        String targetQueueKey = "jobstream:queue:" + targetQueue;

        try {
            String json = client.get(jobKey);

            if (json == null) {
                return Optional.empty();
            }

            Job job = serializer.deserialize(json);

            if (job.status() != JobStatus.DEAD) {
                return Optional.empty();
            }

            Job requeuedJob = job.resetRetryForRequeue();
            String requeuedJson = serializer.serialize(requeuedJob);

            var transaction = client.multi();

            transaction.lrem(DLQ_KEY, 1, jobIdValue);
            transaction.set(jobKey, requeuedJson);
            transaction.srem(deadStatusKey, jobIdValue);
            transaction.sadd(queuedStatusKey, jobIdValue);
            transaction.lpush(targetQueueKey, jobIdValue);

            transaction.exec();

            return Optional.of(requeuedJob);

        } catch (JedisException e) {
            throw new RetryException(
                    "Failed to requeue dead letter job",
                    e
            );
        }
    }

    @Override
    public void purge() {
        try {
            client.del(DLQ_KEY);
        } catch (JedisException e) {
            throw new RetryException(
                    "Failed to purge dead letter queue",
                    e
            );
        }
    }

    @Override
    public long size() {
        try {
            return client.llen(DLQ_KEY);
        } catch (JedisException e) {
            throw new RetryException(
                    "Failed to get dead letter queue size",
                    e
            );
        }
    }
}