package io.github.pandeyayushk.jobstream.retry;

import io.github.pandeyayushk.jobstream.job.Job;
import io.github.pandeyayushk.jobstream.job.JobId;
import io.github.pandeyayushk.jobstream.job.JobStatus;
import io.github.pandeyayushk.jobstream.queue.RedisPriorityQueueStore;
import io.github.pandeyayushk.jobstream.queue.RedisQueueRegistry;
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

    private static final String MOVE_TO_DLQ_SCRIPT = """
            local current = redis.call('GET', KEYS[1])
            local oldStatus = ARGV[4]
            if current then
                local ok, data = pcall(cjson.decode, current)
                if not ok or type(data) ~= 'table' or type(data.status) ~= 'string' then return -2 end
                local revision = string.match(current, '.*"revision"%s*:%s*(%d+)%s*}%s*$') or '0'
                if revision ~= ARGV[1] then return -1 end
                oldStatus = data.status
            elseif ARGV[1] ~= '0' then
                return -1
            end
            local oldStatusKey = 'jobstream:status:' .. oldStatus
            local oldType = redis.call('TYPE', oldStatusKey).ok
            local deadType = redis.call('TYPE', KEYS[2]).ok
            local listType = redis.call('TYPE', KEYS[3]).ok
            if (oldType ~= 'none' and oldType ~= 'set') or
               (deadType ~= 'none' and deadType ~= 'set') or
               (listType ~= 'none' and listType ~= 'list') then return -2 end
            redis.call('SET', KEYS[1], ARGV[2])
            redis.call('SREM', oldStatusKey, ARGV[3])
            redis.call('SADD', KEYS[2], ARGV[3])
            redis.call('LPUSH', KEYS[3], ARGV[3])
            return 1
            """;

    private static final String REQUEUE_SCRIPT = RedisPriorityQueueStore.enqueueFunctionLua() + """
            local current = redis.call('GET', KEYS[2])
            if not current then return -1 end
            local ok, data = pcall(cjson.decode, current)
            if not ok or type(data) ~= 'table' then return -2 end
            local revision = string.match(current, '.*"revision"%s*:%s*(%d+)%s*}%s*$') or '0'
            if revision ~= ARGV[1] then return -1 end
            if data.status ~= 'DEAD' then return -1 end

            local listType = redis.call('TYPE', KEYS[1]).ok
            local deadType = redis.call('TYPE', KEYS[3]).ok
            local queuedType = redis.call('TYPE', KEYS[4]).ok
            local sequenceType = redis.call('TYPE', KEYS[5]).ok
            local queueType = redis.call('TYPE', KEYS[6]).ok
            local membersType = redis.call('TYPE', KEYS[7]).ok
            local registryType = redis.call('TYPE', KEYS[8]).ok
            if (listType ~= 'none' and listType ~= 'list') or
               (deadType ~= 'none' and deadType ~= 'set') or
               (queuedType ~= 'none' and queuedType ~= 'set') or
               (sequenceType ~= 'none' and sequenceType ~= 'stream') or
               (queueType ~= 'none' and queueType ~= 'zset') or
               (membersType ~= 'none' and membersType ~= 'hash') or
               (registryType ~= 'none' and registryType ~= 'set') then return -2 end

            -- Register first so a failed requeue cannot leave an undiscoverable queue.
            redis.call('SADD', KEYS[8], ARGV[5])
            redis.call('LREM', KEYS[1], 1, ARGV[3])
            redis.call('SET', KEYS[2], ARGV[2])
            redis.call('SREM', KEYS[3], ARGV[3])
            redis.call('SADD', KEYS[4], ARGV[3])

            enqueuePriority(KEYS[5], KEYS[6], KEYS[7], KEYS[8], ARGV[5], ARGV[3], tonumber(ARGV[4]))
            return 1
            """;

    private final RedisClient client;
    private final JobSerializer serializer;
    private final String queueRegistryKey;

    public RedisDeadLetterQueue(
            RedisClient client,
            JobSerializer serializer
    ) {
        this(client, serializer, RedisPriorityQueueStore.queueRegistryKey());
    }

    RedisDeadLetterQueue(
            RedisClient client,
            JobSerializer serializer,
            String queueRegistryKey
    ) {
        if (client == null) {
            throw new IllegalArgumentException("Redis client cannot be null");
        }

        if (serializer == null) {
            throw new IllegalArgumentException("Job serializer cannot be null");
        }

        this.client = client;
        this.serializer = serializer;
        this.queueRegistryKey = java.util.Objects.requireNonNull(
                queueRegistryKey, "Queue registry key cannot be null"
        );
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
        final long nextRevision;
        try {
            nextRevision = Math.addExact(job.revision(), 1);
        } catch (ArithmeticException e) {
            throw new RetryException("Job revision exhausted", e);
        }
        String jobKey = JOB_KEY_PREFIX + deadJob.id();
        String json = serializer.serialize(deadJob.withRevision(nextRevision));

        try {
            Object result = client.eval(
                    MOVE_TO_DLQ_SCRIPT,
                    List.of(jobKey, STATUS_KEY_PREFIX + JobStatus.DEAD, DLQ_KEY),
                    List.of(
                            Long.toString(job.revision()),
                            json,
                            deadJob.id().toString(),
                            job.status().name()
                    )
            );
            long status = ((Number) result).longValue();
            if (status != 1) {
                throw new RetryException(
                        status == -1
                                ? "Job changed concurrently while moving to dead letter queue"
                                : "Incompatible Redis key types while moving job to dead letter queue"
                );
            }
        } catch (JedisException e) {
            throw new RetryException("Failed to move job to dead letter queue", e);
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
            final long nextRevision;
            try {
                nextRevision = Math.addExact(job.revision(), 1);
            } catch (ArithmeticException e) {
                throw new RetryException("Job revision exhausted", e);
            }
            Job persistedRequeuedJob = requeuedJob.withRevision(nextRevision);
            String requeuedJson = serializer.serialize(persistedRequeuedJob);

            io.github.pandeyayushk.jobstream.queue.RedisPriorityQueueStore
                    .migrateLegacyQueue(client, targetQueue);
            Object result = client.eval(
                    REQUEUE_SCRIPT,
                    List.of(
                            DLQ_KEY,
                            jobKey,
                            deadStatusKey,
                            queuedStatusKey,
                            io.github.pandeyayushk.jobstream.queue.RedisPriorityQueueStore
                                    .sequenceKey(targetQueue),
                            io.github.pandeyayushk.jobstream.queue.RedisPriorityQueueStore
                                    .queueKey(targetQueue),
                            io.github.pandeyayushk.jobstream.queue.RedisPriorityQueueStore
                                    .membersKey(targetQueue),
                            queueRegistryKey
                    ),
                    List.of(
                            Long.toString(job.revision()),
                            requeuedJson,
                            jobIdValue,
                            Integer.toString(requeuedJob.priority()),
                            targetQueue
                    )
            );

            long resultCode = ((Number) result).longValue();
            if (resultCode != 1) {
                throw new RetryException(
                        resultCode == -1
                                ? "Job changed concurrently while requeueing from dead letter queue"
                                : "Incompatible Redis key types while requeueing job"
                );
            }

            return Optional.of(persistedRequeuedJob);

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
