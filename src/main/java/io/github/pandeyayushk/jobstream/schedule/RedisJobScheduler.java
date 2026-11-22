package io.github.pandeyayushk.jobstream.schedule;

import io.github.pandeyayushk.jobstream.job.Job;
import io.github.pandeyayushk.jobstream.job.JobId;
import io.github.pandeyayushk.jobstream.job.JobStatus;
import io.github.pandeyayushk.jobstream.serialization.JobSerializer;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.exceptions.JedisException;

import java.time.Clock;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

public final class RedisJobScheduler implements JobScheduler {

    public static final String SCHEDULED_KEY = "jobstream:schedule:delayed";
    public static final String TARGETS_KEY = "jobstream:schedule:targets";
    private static final String JOB_KEY_PREFIX = "jobstream:job:";
    private static final int MAX_COMPARE_AND_SET_ATTEMPTS = 32;
    private static final long MAX_EXACT_REDIS_INTEGER = 1L << 53;

    private static final String SCHEDULE_SCRIPT = """
            local queueType = redis.call('TYPE', KEYS[2]).ok
            local targetsType = redis.call('TYPE', KEYS[3]).ok
            if (queueType ~= 'none' and queueType ~= 'zset') or
               (targetsType ~= 'none' and targetsType ~= 'hash') then
                return -2
            end
            if redis.call('GET', KEYS[1]) ~= ARGV[1] then
                return -1
            end
            redis.call('SET', KEYS[1], ARGV[2])
            redis.call('ZADD', KEYS[2], ARGV[3], ARGV[4])
            redis.call('HSET', KEYS[3], ARGV[4], ARGV[5])
            return 1
            """;

    private final RedisClient client;
    private final JobSerializer serializer;
    private final Clock clock;
    private final String scheduledKey;
    private final String targetsKey;

    public RedisJobScheduler(RedisClient client, JobSerializer serializer) {
        this(client, serializer, Clock.systemUTC());
    }

    public RedisJobScheduler(RedisClient client, JobSerializer serializer, Clock clock) {
        this(client, serializer, clock, SCHEDULED_KEY, TARGETS_KEY);
    }

    RedisJobScheduler(
            RedisClient client,
            JobSerializer serializer,
            Clock clock,
            String scheduledKey,
            String targetsKey
    ) {
        this.client = Objects.requireNonNull(client, "RedisClient cannot be null");
        this.serializer = Objects.requireNonNull(serializer, "JobSerializer cannot be null");
        this.clock = Objects.requireNonNull(clock, "Clock cannot be null");
        this.scheduledKey = Objects.requireNonNull(scheduledKey, "Scheduled key cannot be null");
        this.targetsKey = Objects.requireNonNull(targetsKey, "Targets key cannot be null");
    }

    @Override
    public void schedule(JobId jobId, Instant executeAt, String targetQueue) {
        validate(jobId, targetQueue);
        Objects.requireNonNull(executeAt, "Execution timestamp cannot be null");
        long score = toRedisScore(executeAt);
        String jobKey = JOB_KEY_PREFIX + jobId;

        try {
            for (int attempt = 0; attempt < MAX_COMPARE_AND_SET_ATTEMPTS; attempt++) {
                String currentJson = client.get(jobKey);
                if (currentJson == null) {
                    throw new JobSchedulingException("Cannot schedule missing job " + jobId);
                }

                Job current = serializer.deserialize(currentJson);
                validateSchedulable(current);
                String scheduledJson = serializer.serialize(current.withScheduledAt(executeAt));
                Object result = client.eval(
                        SCHEDULE_SCRIPT,
                        List.of(jobKey, scheduledKey, targetsKey),
                        List.of(
                                currentJson,
                                scheduledJson,
                                Long.toString(score),
                                jobId.toString(),
                                targetQueue
                        )
                );
                long status = ((Number) result).longValue();
                if (status == 1) {
                    return;
                }
                if (status == -2) {
                    throw new JobSchedulingException(
                            "Scheduler Redis keys have incompatible data types"
                    );
                }
            }
            throw new JobSchedulingException(
                    "Job changed too frequently to schedule: " + jobId
            );
        } catch (JedisException e) {
            throw new JobSchedulingException("Redis failure while scheduling job " + jobId, e);
        }
    }

    @Override
    public void scheduleDelayed(JobId jobId, Duration delay, String targetQueue) {
        if (delay == null) {
            throw new NullPointerException("Delay cannot be null");
        }
        if (delay.isNegative()) {
            throw new IllegalArgumentException("Delay cannot be negative");
        }

        final Instant executeAt;
        try {
            executeAt = clock.instant().plus(delay);
        } catch (DateTimeException | ArithmeticException e) {
            throw new IllegalArgumentException("Delay produces an out-of-range timestamp", e);
        }
        schedule(jobId, executeAt, targetQueue);
    }

    @Override
    public long scheduledCount() {
        try {
            return client.zcard(scheduledKey);
        } catch (JedisException e) {
            throw new JobSchedulingException("Redis failure while counting scheduled jobs", e);
        }
    }

    private static void validate(JobId jobId, String targetQueue) {
        Objects.requireNonNull(jobId, "JobId cannot be null");
        if (targetQueue == null || targetQueue.isBlank()) {
            throw new IllegalArgumentException("Target queue cannot be null or blank");
        }
    }

    private static void validateSchedulable(Job job) {
        if (job.status() != JobStatus.PENDING && job.status() != JobStatus.RETRYING) {
            throw new JobSchedulingException(
                    "Only PENDING or RETRYING jobs can be scheduled; job is " + job.status()
            );
        }
    }

    private static long toRedisScore(Instant instant) {
        final long epochMillis;
        try {
            epochMillis = instant.toEpochMilli();
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("Execution timestamp is outside epoch-millisecond range", e);
        }
        if (epochMillis < -MAX_EXACT_REDIS_INTEGER || epochMillis > MAX_EXACT_REDIS_INTEGER) {
            throw new IllegalArgumentException(
                    "Execution timestamp cannot be represented exactly as a Redis Sorted Set score"
            );
        }
        return epochMillis;
    }
}
