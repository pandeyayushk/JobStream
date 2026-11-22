package io.github.pandeyayushk.jobstream.queue;

import io.github.pandeyayushk.jobstream.job.Job;
import io.github.pandeyayushk.jobstream.serialization.JobSerializer;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.exceptions.JedisException;

import java.util.List;
import java.util.Objects;

public class RedisJobSubmissionStore implements JobSubmissionStore {

    private static final String SUBMIT_SCRIPT = """
            local current = redis.call('GET', KEYS[1])
            local oldStatus = ARGV[5]
            if current then
                local ok, data = pcall(cjson.decode, current)
                if not ok or type(data) ~= 'table' then return -2 end
                local revision = string.match(current, '.*"revision"%s*:%s*(%d+)%s*}%s*$') or '0'
                if revision ~= ARGV[3] then return -1 end
                oldStatus = data.status
            elseif ARGV[3] ~= '0' then
                return -1
            end

            local oldStatusKey = 'jobstream:status:' .. oldStatus
            local oldStatusType = redis.call('TYPE', oldStatusKey).ok
            local newStatusType = redis.call('TYPE', KEYS[2]).ok
            local sequenceType = redis.call('TYPE', KEYS[3]).ok
            local queueType = redis.call('TYPE', KEYS[4]).ok
            local membersType = redis.call('TYPE', KEYS[5]).ok
            if (oldStatusType ~= 'none' and oldStatusType ~= 'set') or
               (newStatusType ~= 'none' and newStatusType ~= 'set') or
               (sequenceType ~= 'none' and sequenceType ~= 'stream') or
               (queueType ~= 'none' and queueType ~= 'zset') or
               (membersType ~= 'none' and membersType ~= 'hash') then return -2 end

            redis.call('SET', KEYS[1], ARGV[1])
            redis.call('SREM', oldStatusKey, ARGV[2])
            redis.call('SADD', KEYS[2], ARGV[2])

            local jobId = ARGV[2]
            if redis.call('HEXISTS', KEYS[5], jobId) == 0 then
                local streamId = redis.call('XADD', KEYS[3], '*', 'job', jobId)
                redis.call('XDEL', KEYS[3], streamId)
                local millis, tick = string.match(streamId, '^(%d+)%-(%d+)$')
                local function pad(value)
                    return string.rep('0', 20 - string.len(value)) .. value
                end
                local member = pad(millis) .. '-' .. pad(tick) .. ':' .. jobId
                redis.call('ZADD', KEYS[4], -tonumber(ARGV[4]), member)
                redis.call('HSET', KEYS[5], jobId, member)
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
        String queuedStatusKey = "jobstream:status:" + queuedJob.status();
        long nextRevision;
        try {
            nextRevision = Math.addExact(originalJob.revision(), 1);
        } catch (ArithmeticException e) {
            throw new QueueException("Job revision exhausted", e);
        }
        String json = serializer.serialize(queuedJob.withRevision(nextRevision));

        try {
            RedisPriorityQueueStore.migrateLegacyQueue(client, queueName);
            Object result = client.eval(
                    SUBMIT_SCRIPT,
                    List.of(
                            jobKey,
                            queuedStatusKey,
                            RedisPriorityQueueStore.sequenceKey(queueName),
                            RedisPriorityQueueStore.queueKey(queueName),
                            RedisPriorityQueueStore.membersKey(queueName)
                    ),
                    List.of(
                            json,
                            queuedJob.id().toString(),
                            Long.toString(originalJob.revision()),
                            Integer.toString(queuedJob.priority()),
                            originalJob.status().name()
                    )
            );
            // The script checks the revision and all types before mutation.
            // A conflict must be visible to the caller, never a silent overwrite.
            long status = ((Number) result).longValue();
            if (status == -1) {
                throw new QueueException("Job changed concurrently while submitting " + queuedJob.id());
            }
            if (status != 1) {
                throw new QueueException("Incompatible Redis key types while submitting " + queuedJob.id());
            }
        } catch (JedisException e) {
            throw new QueueException("Failed to atomically submit job", e);
        }
    }
}
