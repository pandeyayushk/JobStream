package io.github.pandeyayushk.jobstream.queue;

import io.github.pandeyayushk.jobstream.job.Job;
import io.github.pandeyayushk.jobstream.serialization.JobSerializer;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.exceptions.JedisException;

import java.util.Objects;

public class RedisJobSubmissionStore implements JobSubmissionStore {

    private final RedisClient client;
    private final JobSerializer serializer;

    public RedisJobSubmissionStore(RedisClient client,JobSerializer serializer){
        this.client = Objects.requireNonNull(
                client,
                "RedisClient cannot be null"
        );
        this.serializer = Objects.requireNonNull(
                serializer,
                "JobSerializer cannot be null"
        );
    }

    @Override
    public void submit(Job originalJob, Job queuedJob, String queueName) {
        Objects.requireNonNull(originalJob, "Original job cannot be null");
        Objects.requireNonNull(queuedJob, "Queued job cannot be null");


        String jobKey = "jobstream:job:" + queuedJob.id();
        String oldStatusKey =
                "jobstream:status:" + originalJob.status();
        String queuedStatusKey =
                "jobstream:status:" + queuedJob.status();
        String queueKey =
                "jobstream:queue:" + queueName;

        String json = serializer.serialize(queuedJob);

        try {
            var transaction = client.multi();

            transaction.set(jobKey, json);
            transaction.srem(oldStatusKey, queuedJob.id().toString());
            transaction.sadd(queuedStatusKey, queuedJob.id().toString());
            transaction.lpush(queueKey, queuedJob.id().toString());

            transaction.exec();

        } catch (JedisException e) {
            throw new QueueException(
                    "Failed to atomically submit job",
                    e
            );
        }
    }
}