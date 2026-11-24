package io.github.pandeyayushk.jobstream.schedule;

import io.github.pandeyayushk.jobstream.job.Job;
import io.github.pandeyayushk.jobstream.job.JobId;
import io.github.pandeyayushk.jobstream.job.JobStatus;
import io.github.pandeyayushk.jobstream.queue.RedisPriorityQueueStore;
import io.github.pandeyayushk.jobstream.serialization.JobSerializer;
import io.github.pandeyayushk.jobstream.serialization.SerializationException;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.exceptions.JedisException;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Atomically releases due Jobs from scheduler indexes into the existing priority queues. */
public final class RedisSchedulerPoller implements SchedulerPoller {

    public static final String POLL_CURSOR_KEY = "jobstream:schedule:poll-cursor";
    private static final String JOB_KEY_PREFIX = "jobstream:job:";
    private static final String STATUS_KEY_PREFIX = "jobstream:status:";
    private static final int MAX_CAS_ATTEMPTS = 8;
    private static final long MAX_EXACT_REDIS_INTEGER = 1L << 53;

    /* Atomically advances a persistent rank through the currently due sorted-set entries. */
    private static final String SELECT_CANDIDATES_SCRIPT = """
            local scheduleType = redis.call('TYPE', KEYS[1]).ok
            local cursorType = redis.call('TYPE', KEYS[2]).ok
            if (scheduleType ~= 'none' and scheduleType ~= 'zset') or
               (cursorType ~= 'none' and cursorType ~= 'string') then return {-1} end

            local dueCount = redis.call('ZCOUNT', KEYS[1], '-inf', ARGV[1])
            if dueCount == 0 then return {0, 0} end
            local storedCursor = redis.call('GET', KEYS[2]) or '0'
            if not string.match(storedCursor, '^%d+$') then return {-1} end
            local offset = tonumber(storedCursor) % dueCount
            local candidates = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1],
                    'LIMIT', offset, ARGV[2])
            local nextOffset = offset + #candidates
            if nextOffset >= dueCount then nextOffset = 0 end
            redis.call('SET', KEYS[2], tostring(nextOffset))
            local result = {dueCount, #candidates}
            for _, candidate in ipairs(candidates) do
                table.insert(result, candidate)
            end
            return result
            """;

    /* Return codes: 1 released, 2 cleaned, 0 stale/raced, -1 incompatible type. */
    private static final String RELEASE_SCRIPT = RedisPriorityQueueStore.enqueueFunctionLua() + """
            local function keyType(key)
                return redis.call('TYPE', key).ok
            end
            local function compatible(key, expected)
                local actual = keyType(key)
                return actual == 'none' or actual == expected
            end

            local id = ARGV[1]
            local target = ARGV[2]
            local now = tonumber(ARGV[3])
            if not compatible(KEYS[1], 'zset') or not compatible(KEYS[2], 'hash') or
               not compatible(KEYS[3], 'string') then return -1 end
            local current = redis.call('GET', KEYS[3])
            local dueScore = redis.call('ZSCORE', KEYS[1], id)
            if not dueScore or tonumber(dueScore) > now or
               redis.call('HGET', KEYS[2], id) ~= target then return 0 end
            if not current then
                redis.call('ZREM', KEYS[1], id)
                redis.call('HDEL', KEYS[2], id)
                return 2
            end
            if current ~= ARGV[4] then return 0 end

            local action = ARGV[5]
            if action == 'CLEAN' then
                redis.call('ZREM', KEYS[1], id)
                redis.call('HDEL', KEYS[2], id)
                return 2
            end

            if not compatible(KEYS[3], 'string') or
               not compatible(KEYS[4], 'set') or
               not compatible(KEYS[5], 'set') or
               not compatible(KEYS[6], 'stream') or
               not compatible(KEYS[7], 'zset') or
               not compatible(KEYS[8], 'hash') or
               not compatible(KEYS[9], 'set') then return -1 end

            -- Register first so a failed release cannot leave an undiscoverable queue.
            redis.call('SADD', KEYS[9], target)
            redis.call('SET', KEYS[3], ARGV[6])
            redis.call('SREM', KEYS[4], id)
            redis.call('SADD', KEYS[5], id)
            redis.call('ZREM', KEYS[1], id)
            redis.call('HDEL', KEYS[2], id)
            enqueuePriority(KEYS[6], KEYS[7], KEYS[8], KEYS[9], target, id, tonumber(ARGV[7]))
            return 1
            """;

    private static final String CLEAN_MISSING_SCRIPT = """
            local ztype = redis.call('TYPE', KEYS[1]).ok
            local htype = redis.call('TYPE', KEYS[2]).ok
            local jtype = redis.call('TYPE', KEYS[3]).ok
            if (ztype ~= 'none' and ztype ~= 'zset') or
               (htype ~= 'none' and htype ~= 'hash') or
               (jtype ~= 'none' and jtype ~= 'string') then return -1 end
            local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
            if not score or tonumber(score) > tonumber(ARGV[2]) or
               redis.call('HGET', KEYS[2], ARGV[1]) ~= ARGV[3] or
               redis.call('EXISTS', KEYS[3]) == 1 then return 0 end
            redis.call('ZREM', KEYS[1], ARGV[1])
            redis.call('HDEL', KEYS[2], ARGV[1])
            return 2
            """;

    private final RedisClient client;
    private final JobSerializer serializer;
    private final Clock clock;
    private final String scheduledKey;
    private final String targetsKey;
    private final String cursorKey;
    private final Runnable beforeRelease;
    private final String queueRegistryKey;

    public RedisSchedulerPoller(RedisClient client, JobSerializer serializer) {
        this(client, serializer, Clock.systemUTC());
    }

    public RedisSchedulerPoller(RedisClient client, JobSerializer serializer, Clock clock) {
        this(client, serializer, clock, RedisJobScheduler.SCHEDULED_KEY,
                RedisJobScheduler.TARGETS_KEY, POLL_CURSOR_KEY, () -> { });
    }

    RedisSchedulerPoller(
            RedisClient client,
            JobSerializer serializer,
            Clock clock,
            String scheduledKey,
            String targetsKey,
            String cursorKey,
            Runnable beforeRelease
    ) {
        this(client, serializer, clock, scheduledKey, targetsKey, cursorKey,
                beforeRelease, RedisPriorityQueueStore.queueRegistryKey());
    }

    RedisSchedulerPoller(
            RedisClient client,
            JobSerializer serializer,
            Clock clock,
            String scheduledKey,
            String targetsKey,
            String cursorKey,
            Runnable beforeRelease,
            String queueRegistryKey
    ) {
        this.client = Objects.requireNonNull(client, "RedisClient cannot be null");
        this.serializer = Objects.requireNonNull(serializer, "JobSerializer cannot be null");
        this.clock = Objects.requireNonNull(clock, "Clock cannot be null");
        this.scheduledKey = Objects.requireNonNull(scheduledKey, "Scheduled key cannot be null");
        this.targetsKey = Objects.requireNonNull(targetsKey, "Targets key cannot be null");
        this.cursorKey = Objects.requireNonNull(cursorKey, "Cursor key cannot be null");
        this.beforeRelease = Objects.requireNonNull(beforeRelease, "Hook cannot be null");
        this.queueRegistryKey = Objects.requireNonNull(queueRegistryKey, "Queue registry key cannot be null");
    }

    @Override
    public SchedulerPollResult pollOnce(int batchSize) {
        if (batchSize <= 0) throw new IllegalArgumentException("Batch size must be positive");
        long now = redisMillis(clock.instant());
        try {
            List<String> due = selectDueCandidates(now, batchSize);
            int released = 0;
            int cleaned = 0;
            int deferred = 0;
            for (String rawId : due) {
                JobId id;
                try {
                    id = JobId.fromString(rawId);
                } catch (IllegalArgumentException invalidId) {
                    deferred++;
                    continue;
                }
                Outcome outcome = releaseOne(id, rawId, now);
                switch (outcome) {
                    case RELEASED -> released++;
                    case CLEANED -> cleaned++;
                    case DEFERRED -> deferred++;
                    case RACED -> { }
                }
            }
            return new SchedulerPollResult(due.size(), released, cleaned, deferred);
        } catch (JedisException e) {
            throw new JobSchedulingException("Redis failure while polling due Jobs", e);
        }
    }

    private List<String> selectDueCandidates(long now, int batchSize) {
        Object response = client.eval(SELECT_CANDIDATES_SCRIPT,
                List.of(scheduledKey, cursorKey),
                List.of(Long.toString(now), Integer.toString(batchSize)));
        if (!(response instanceof List<?> values) || values.isEmpty()) {
            throw new JobSchedulingException("Redis returned an invalid scheduler scan response");
        }
        if (((Number) values.get(0)).longValue() < 0) {
            throw new JobSchedulingException("Scheduler scan keys have incompatible types or cursor data");
        }
        if (values.size() < 2) {
            throw new JobSchedulingException("Redis returned an incomplete scheduler scan response");
        }
        int candidateCount = Math.toIntExact(((Number) values.get(1)).longValue());
        if (candidateCount != values.size() - 2 || candidateCount > batchSize) {
            throw new JobSchedulingException("Redis returned an invalid scheduler candidate batch");
        }
        List<String> candidates = new ArrayList<>(candidateCount);
        for (int index = 2; index < values.size(); index++) {
            candidates.add((String) values.get(index));
        }
        return candidates;
    }

    private Outcome releaseOne(JobId id, String rawId, long now) {
        String jobKey = JOB_KEY_PREFIX + rawId;
        String target = client.hget(targetsKey, rawId);
        if (target == null || target.isBlank()) return Outcome.DEFERRED;

        RedisPriorityQueueStore.migrateLegacyQueue(client, target);
        for (int attempt = 0; attempt < MAX_CAS_ATTEMPTS; attempt++) {
            String currentJson = client.get(jobKey);
            if (currentJson == null) {
                Object result = client.eval(CLEAN_MISSING_SCRIPT,
                        List.of(scheduledKey, targetsKey, jobKey),
                        List.of(rawId, Long.toString(now), target));
                return resultCode(result) == 2 ? Outcome.CLEANED : Outcome.RACED;
            }

            final Job current;
            try {
                current = serializer.deserialize(currentJson);
            } catch (SerializationException malformed) {
                return Outcome.DEFERRED;
            }
            if (!current.id().equals(id)) return Outcome.DEFERRED;
            if (current.status() != JobStatus.PENDING && current.status() != JobStatus.RETRYING) {
                beforeRelease.run();
                long result = evalRelease(current, currentJson, null, rawId, target, now, "CLEAN");
                if (result == 2) return Outcome.CLEANED;
                if (result == -1) throw incompatibleKeys(id);
                continue;
            }

            final Job queued;
            try {
                queued = current.withStatus(JobStatus.QUEUED)
                        .withRevision(Math.addExact(current.revision(), 1));
            } catch (ArithmeticException | IllegalStateException e) {
                return Outcome.DEFERRED;
            }
            beforeRelease.run();
            long result = evalRelease(current, currentJson, serializer.serialize(queued),
                    rawId, target, now, "RELEASE");
            if (result == 1) return Outcome.RELEASED;
            if (result == -1) throw incompatibleKeys(id);
        }
        return Outcome.DEFERRED;
    }

    private long evalRelease(Job current, String expectedJson, String replacementJson,
                             String rawId, String target, long now, String action) {
        String oldStatus = current.status().name();
        List<String> keys = List.of(
                scheduledKey,
                targetsKey,
                JOB_KEY_PREFIX + rawId,
                STATUS_KEY_PREFIX + oldStatus,
                STATUS_KEY_PREFIX + JobStatus.QUEUED.name(),
                RedisPriorityQueueStore.sequenceKey(target),
                RedisPriorityQueueStore.queueKey(target),
                RedisPriorityQueueStore.membersKey(target),
                queueRegistryKey
        );
        return resultCode(client.eval(RELEASE_SCRIPT, keys, List.of(
                rawId,
                target,
                Long.toString(now),
                expectedJson,
                action,
                replacementJson == null ? "" : replacementJson,
                Integer.toString(current.priority())
        )));
    }

    private static long resultCode(Object result) {
        return ((Number) result).longValue();
    }

    private static JobSchedulingException incompatibleKeys(JobId id) {
        return new JobSchedulingException("Incompatible Redis key types while releasing Job " + id);
    }

    private static long redisMillis(Instant instant) {
        final long millis;
        try {
            millis = instant.toEpochMilli();
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("Clock instant is outside epoch-millisecond range", e);
        }
        if (millis < -MAX_EXACT_REDIS_INTEGER || millis > MAX_EXACT_REDIS_INTEGER) {
            throw new IllegalArgumentException("Clock instant cannot be represented exactly as a Redis score");
        }
        return millis;
    }

    private enum Outcome { RELEASED, CLEANED, DEFERRED, RACED }
}
