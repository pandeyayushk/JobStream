package io.github.pandeyayushk.jobstream.queue;

import redis.clients.jedis.RedisClient;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;

/** Redis Set implementation of {@link QueueRegistry}, including legacy key backfill. */
public final class RedisQueueRegistry implements QueueRegistry {
    static final String REDIS_KEY = "jobstream:queues";
    static final String PRIORITY_QUEUE_PATTERN = "jobstream:priority-queue:*";
    static final String LEGACY_QUEUE_PATTERN = "jobstream:queue:*";
    private static final String DLQ_KEY = "jobstream:queue:dead-letter";
    private static final int SCAN_COUNT = 100;

    private final RedisClient client;
    private final ScanPageSource scanPageSource;

    public RedisQueueRegistry(RedisClient client) {
        this(client, (cursor, pattern) -> {
            ScanParams params = new ScanParams().match(pattern).count(SCAN_COUNT);
            ScanResult<String> result = client.scan(cursor, params);
            return new ScanPage(result.getCursor(), result.getResult());
        });
    }

    RedisQueueRegistry(RedisClient client, ScanPageSource scanPageSource) {
        this.client = Objects.requireNonNull(client, "RedisClient cannot be null");
        this.scanPageSource = Objects.requireNonNull(scanPageSource, "Scan source cannot be null");
    }

    @Override
    public void registerQueue(String queueName) {
        validateQueueName(queueName);
        try {
            client.sadd(REDIS_KEY, queueName);
        } catch (JedisException e) {
            throw new QueueException("Failed to register queue", e);
        }
    }

    @Override
    public List<String> listQueues() {
        try {
            return new ArrayList<>(new TreeSet<>(client.smembers(REDIS_KEY)));
        } catch (JedisException e) {
            throw new QueueException("Failed to list queues", e);
        }
    }

    /**
     * Repeats a full cursor scan of both existing key families. Partial runs are safe: names
     * already added remain registered, and a later invocation starts again at cursor zero.
     * Returns the number of names newly added by this invocation.
     */
    public long backfillExistingQueues() {
        try {
            long added = scanAndRegister(PRIORITY_QUEUE_PATTERN, "zset", "jobstream:priority-queue:");
            added += scanAndRegister(LEGACY_QUEUE_PATTERN, "list", "jobstream:queue:");
            return added;
        } catch (JedisException e) {
            throw new QueueException("Failed to backfill queue registry", e);
        }
    }

    private long scanAndRegister(String pattern, String expectedType, String keyPrefix) {
        long added = 0;
        String cursor = ScanParams.SCAN_POINTER_START;
        do {
            ScanPage page = scanPageSource.scan(cursor, pattern);
            if (page == null || page.cursor() == null || page.keys() == null) {
                throw new QueueException("Redis returned an invalid queue scan page");
            }
            for (String key : page.keys()) {
                if (key == null || key.equals(DLQ_KEY) || !key.startsWith(keyPrefix)) {
                    continue;
                }
                String queueName = key.substring(keyPrefix.length());
                if (queueName.isBlank() || !expectedType.equals(client.type(key))) {
                    continue;
                }
                added += client.sadd(REDIS_KEY, queueName);
            }
            cursor = page.cursor();
        } while (!ScanParams.SCAN_POINTER_START.equals(cursor));
        return added;
    }

    private static void validateQueueName(String queueName) {
        if (queueName == null || queueName.isBlank()) {
            throw new IllegalArgumentException("Queue name cannot be null or blank");
        }
    }

    @FunctionalInterface
    interface ScanPageSource {
        ScanPage scan(String cursor, String pattern);
    }

    record ScanPage(String cursor, List<String> keys) { }
}
