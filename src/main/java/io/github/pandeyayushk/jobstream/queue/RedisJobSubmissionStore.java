package io.github.pandeyayushk.jobstream.queue;

import io.github.pandeyayushk.jobstream.job.Job;
import io.github.pandeyayushk.jobstream.serialization.JobSerializer;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.exceptions.JedisException;

import java.util.List;
import java.util.Objects;

public class RedisJobSubmissionStore implements JobSubmissionStore {

    private static final String SUBMIT_SCRIPT = """
            redis.call('SET', KEYS[1], ARGV[1])
            redis.call('SREM', KEYS[2], ARGV[2])
            redis.call('SADD', KEYS[3], ARGV[2])

            local jobId = ARGV[2]
            if redis.call('HEXISTS', KEYS[6], jobId) == 0 then
                local streamId = redis.call('XADD', KEYS[4], '*', 'job', jobId)
                redis.call('XDEL', KEYS[4], streamId)
                local millis, tick = string.match(streamId, '^(%d+)%-(%d+)$')
                local function pad(value)
                    return string.rep('0', 20 - string.len(value)) .. value
                end
                local member = pad(millis) .. '-' .. pad(tick) .. ':' .. jobId
                redis.call('ZADD', KEYS[5], -tonumber(ARGV[3]), member)
                redis.call('HSET', KEYS[6], jobId, member)
            end
            return 1
            """;

    private final RedisClient client;
    private final JobSerializer serializer;

    public RedisJobSubmissionStore(RedisClient client, JobSerializer serializer) {
        this.client = Objects.requireNonNull(client, "RedisClient cannot be null");
        this.serializer = Objects.requireNonNull(serializer, "JobSerializer cannot be null");
    }

    @Override
    public void submit(Job originalJob, Job queuedJob, String queueName) {
        Objects.requireNonNull(originalJob, "Original job cannot be null");
        Objects.requireNonNull(queuedJob, "Queued job cannot be null");
        if (queueName == null || queueName.isBlank()) {
            throw new IllegalArgumentException("Queue name cannot be null or blank");
        }

        String jobKey = "jobstream:job:" + queuedJob.id();
        String oldStatusKey = "jobstream:status:" + originalJob.status();
        String queuedStatusKey = "jobstream:status:" + queuedJob.status();
        String json = serializer.serialize(queuedJob);

        try {
            RedisPriorityQueueStore.migrateLegacyQueue(client, queueName);
            client.eval(
                    SUBMIT_SCRIPT,
                    List.of(
                            jobKey,
                            oldStatusKey,
                            queuedStatusKey,
                            RedisPriorityQueueStore.sequenceKey(queueName),
                            RedisPriorityQueueStore.queueKey(queueName),
                            RedisPriorityQueueStore.membersKey(queueName)
                    ),
                    List.of(
                            json,
                            queuedJob.id().toString(),
                            Integer.toString(queuedJob.priority())
                    )
            );
        } catch (JedisException e) {
            throw new QueueException("Failed to atomically submit job", e);
        }
    }
}
