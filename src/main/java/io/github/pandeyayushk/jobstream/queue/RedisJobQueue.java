package io.github.pandeyayushk.jobstream.queue;

import io.github.pandeyayushk.jobstream.job.JobId;
import io.github.pandeyayushk.jobstream.serialization.JacksonJobSerializer;
import io.github.pandeyayushk.jobstream.serialization.SerializationException;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.exceptions.JedisException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class RedisJobQueue implements JobQueue {

    private static final String JOB_KEY_PREFIX = "jobstream:job:";
    private static final String POP_SCRIPT = """
            local first = redis.call('ZRANGE', KEYS[1], 0, 0)
            if #first == 0 then return nil end
            local member = first[1]
            redis.call('ZREM', KEYS[1], member)
            local jobId = string.match(member, ':(.+)$')
            if jobId then redis.call('HDEL', KEYS[2], jobId) end
            return member
            """;

    private final RedisClient client;
    private final JacksonJobSerializer serializer = new JacksonJobSerializer();

    public RedisJobQueue(RedisClient client) {
        this.client = client;
    }

    @Override
    public void enqueue(JobId jobId, String queueName) {
        validateQueueName(queueName);
        validateJobId(jobId);
        try {
            RedisPriorityQueueStore.migrateLegacyQueue(client, queueName);
            String json = client.get(JOB_KEY_PREFIX + jobId);
            int priority = json == null ? 0 : serializer.deserialize(json).priority();
            RedisPriorityQueueStore.enqueue(
                    client,
                    queueName,
                    jobId.toString(),
                    priority
            );
        } catch (JedisException e) {
            throw new QueueException("Job cannot be added to queue", e);
        } catch (SerializationException e) {
            throw new QueueException("Unable to read job priority", e);
        }
    }

    @Override
    public Optional<JobId> dequeue(String queueName, Duration timeout) {
        if (timeout == null || timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("Duration cannot be zero, negative, or null");
        }
        validateQueueName(queueName);

        long timeoutNanos;
        try {
            timeoutNanos = timeout.toNanos();
        } catch (ArithmeticException overflow) {
            timeoutNanos = Long.MAX_VALUE;
        }
        long startedAt = System.nanoTime();

        while (true) {
            Optional<JobId> jobId = dequeueNonBlocking(queueName);
            if (jobId.isPresent()) {
                return jobId;
            }

            long elapsed = System.nanoTime() - startedAt;
            long remaining = timeoutNanos - elapsed;
            if (remaining <= 0) {
                return Optional.empty();
            }

            long sleepMillis = Math.min(10, Math.max(1, Duration.ofNanos(remaining).toMillis()));
            try {
                Thread.sleep(sleepMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return Optional.empty();
            }
        }
    }

    @Override
    public Optional<JobId> dequeueNonBlocking(String queueName) {
        validateQueueName(queueName);
        try {
            RedisPriorityQueueStore.migrateLegacyQueue(client, queueName);
            Object result = client.eval(
                    POP_SCRIPT,
                    List.of(
                            RedisPriorityQueueStore.queueKey(queueName),
                            RedisPriorityQueueStore.membersKey(queueName)
                    ),
                    List.of()
            );
            if (result == null) {
                return Optional.empty();
            }
            return Optional.of(parseJobId(extractJobId((String) result)));
        } catch (JedisException e) {
            throw new QueueException("Job cannot be removed from queue", e);
        }
    }

    @Override
    public long size(String queueName) {
        validateQueueName(queueName);
        try {
            RedisPriorityQueueStore.migrateLegacyQueue(client, queueName);
            return client.zcard(RedisPriorityQueueStore.queueKey(queueName));
        } catch (JedisException e) {
            throw new QueueException("Failed to get queue size", e);
        }
    }

    @Override
    public List<JobId> peek(String queueName, int count) {
        validateQueueName(queueName);
        if (count < 0) {
            throw new IllegalArgumentException("Count can not be negative");
        }
        if (count == 0) {
            return List.of();
        }
        try {
            RedisPriorityQueueStore.migrateLegacyQueue(client, queueName);
            List<String> members = client.zrange(
                    RedisPriorityQueueStore.queueKey(queueName),
                    0,
                    count - 1L
            );
            List<JobId> ids = new ArrayList<>(members.size());
            for (String member : members) {
                ids.add(parseJobId(extractJobId(member)));
            }
            return ids;
        } catch (JedisException e) {
            throw new QueueException("Failed to peek queue", e);
        }
    }

    private String extractJobId(String member) {
        int separator = member.lastIndexOf(':');
        if (separator < 0 || separator == member.length() - 1) {
            throw new QueueException("Redis returned an invalid priority queue entry");
        }
        return member.substring(separator + 1);
    }

    private JobId parseJobId(String value) {
        try {
            return JobId.fromString(value);
        } catch (IllegalArgumentException e) {
            throw new QueueException("Redis returned an invalid JobId", e);
        }
    }

    private void validateQueueName(String queueName) {
        if (queueName == null) {
            throw new IllegalArgumentException("Queue name cannot be null");
        }
        if (queueName.isBlank()) {
            throw new IllegalArgumentException("Queue name cannot be empty");
        }
    }

    private void validateJobId(JobId jobId) {
        if (jobId == null) {
            throw new IllegalArgumentException("Job id cannot be null");
        }
    }
}
