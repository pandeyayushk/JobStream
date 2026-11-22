package io.github.pandeyayushk.jobstream.queue;

import redis.clients.jedis.RedisClient;

import java.util.List;

/** Redis key layout and atomic scripts shared by queue and submission adapters. */
public final class RedisPriorityQueueStore {

    private static final String LEGACY_PREFIX = "jobstream:queue:";
    private static final String QUEUE_PREFIX = "jobstream:priority-queue:";
    private static final String SEQUENCE_PREFIX = "jobstream:priority-sequence:";
    private static final String MEMBERS_PREFIX = "jobstream:priority-members:";

    private static final String MIGRATE_SCRIPT = """
            local legacy = KEYS[1]
            local queue = KEYS[2]
            local sequence = KEYS[3]
            local members = KEYS[4]
            local keyType = redis.call('TYPE', legacy).ok
            if keyType == 'none' then return 0 end
            if keyType ~= 'list' then return redis.error_reply('Legacy queue key is not a list') end
            local queueType = redis.call('TYPE', queue).ok
            local sequenceType = redis.call('TYPE', sequence).ok
            local membersType = redis.call('TYPE', members).ok
            if (queueType ~= 'none' and queueType ~= 'zset') or
               (sequenceType ~= 'none' and sequenceType ~= 'stream') or
               (membersType ~= 'none' and membersType ~= 'hash') then
                return redis.error_reply('Priority queue keys have incompatible data types')
            end

            local function nextMember(jobId)
                local streamId = redis.call('XADD', sequence, '*', 'job', jobId)
                redis.call('XDEL', sequence, streamId)
                local millis, tick = string.match(streamId, '^(%d+)%-(%d+)$')
                local function pad(value)
                    return string.rep('0', 20 - string.len(value)) .. value
                end
                return pad(millis) .. '-' .. pad(tick) .. ':' .. jobId
            end

            local oldIds = redis.call('LRANGE', legacy, 0, -1)
            for _, jobId in ipairs(oldIds) do
                local jobType = redis.call('TYPE', 'jobstream:job:' .. jobId).ok
                if jobType ~= 'none' and jobType ~= 'string' then
                    return redis.error_reply('Job record has an incompatible data type')
                end
            end
            for index = #oldIds, 1, -1 do
                local jobId = oldIds[index]
                if redis.call('HEXISTS', members, jobId) == 0 then
                    local priority = 0
                    local jobJson = redis.call('GET', 'jobstream:job:' .. jobId)
                    if jobJson then
                        local ok, jobData = pcall(cjson.decode, jobJson)
                        if ok and type(jobData) == 'table' and type(jobData.priority) == 'number' then
                            priority = jobData.priority
                        end
                    end
                    local member = nextMember(jobId)
                    redis.call('ZADD', queue, -priority, member)
                    redis.call('HSET', members, jobId, member)
                end
            end
            redis.call('DEL', legacy)
            return #oldIds
            """;

    private static final String ENQUEUE_FUNCTION = """
            local function enqueuePriority(sequence, queue, members, jobId, priority)
                local existingMember = redis.call('HGET', members, jobId)
                if existingMember then
                    if redis.call('ZSCORE', queue, existingMember) then
                        redis.call('ZADD', queue, -priority, existingMember)
                        return 0
                    end
                    redis.call('HDEL', members, jobId)
                end

                local streamId = redis.call('XADD', sequence, '*', 'job', jobId)
                redis.call('XDEL', sequence, streamId)
                local millis, tick = string.match(streamId, '^(%d+)%-(%d+)$')
                local function pad(value)
                    return string.rep('0', 20 - string.len(value)) .. value
                end
                local member = pad(millis) .. '-' .. pad(tick) .. ':' .. jobId
                redis.call('ZADD', queue, -priority, member)
                redis.call('HSET', members, jobId, member)
                return 1
            end
            """;
    private static final String ENQUEUE_SCRIPT = ENQUEUE_FUNCTION + """
            return enqueuePriority(KEYS[1], KEYS[2], KEYS[3], ARGV[1], tonumber(ARGV[2]))
            """;

    private RedisPriorityQueueStore() {
    }

    public static String legacyKey(String queueName) {
        return LEGACY_PREFIX + queueName;
    }

    public static String queueKey(String queueName) {
        return QUEUE_PREFIX + queueName;
    }

    public static String sequenceKey(String queueName) {
        return SEQUENCE_PREFIX + queueName;
    }

    public static String membersKey(String queueName) {
        return MEMBERS_PREFIX + queueName;
    }

    /** Shared Lua implementation for the queue's FIFO tie-break and duplicate handling. */
    public static String enqueueFunctionLua() {
        return ENQUEUE_FUNCTION;
    }

    public static void migrateLegacyQueue(RedisClient client, String queueName) {
        client.eval(
                MIGRATE_SCRIPT,
                List.of(
                        legacyKey(queueName),
                        queueKey(queueName),
                        sequenceKey(queueName),
                        membersKey(queueName)
                ),
                List.of()
        );
    }

    static void enqueue(
            RedisClient client,
            String queueName,
            String jobId,
            int priority
    ) {
        client.eval(
                ENQUEUE_SCRIPT,
                List.of(
                        sequenceKey(queueName),
                        queueKey(queueName),
                        membersKey(queueName)
                ),
                List.of(jobId, Integer.toString(priority))
        );
    }

}
