package io.github.pandeyayushk.jobstream.queue;

import io.github.pandeyayushk.jobstream.job.JobId;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.RedisClient;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

public class RedisJobQueueTest {

    @Test
    public void enqueueAndDequeuePreservesFifoOrder() {
        RedisClient client = RedisClient.builder()
                .hostAndPort("localhost", 6379)
                .build();

        RedisJobQueue redisJobQueue = new RedisJobQueue(client);

        JobId id1 = JobId.generate();
        JobId id2 = JobId.generate();
        JobId id3 = JobId.generate();

        String queueName = "fifo-test-"+ UUID.randomUUID();
        redisJobQueue.enqueue(id1, queueName);
        redisJobQueue.enqueue(id2, queueName);
        redisJobQueue.enqueue(id3, queueName);

        assertEquals(Optional.of(id1), redisJobQueue.dequeueNonBlocking(queueName));
        assertEquals(Optional.of(id2), redisJobQueue.dequeueNonBlocking(queueName));
        assertEquals(Optional.of(id3), redisJobQueue.dequeueNonBlocking(queueName));
        assertTrue(redisJobQueue.dequeueNonBlocking(queueName).isEmpty());
    }

    @Test
    public void dequeueReturnsEmptyAfterTimeout(){
        RedisClient client = RedisClient.builder()
                .hostAndPort("localhost", 6379)
                .build();

        RedisJobQueue redisJobQueue = new RedisJobQueue(client);
        String queueName="timeout-test-"+ UUID.randomUUID();
        Instant now=Instant.now();
        Optional<JobId> result=redisJobQueue.dequeue(queueName, Duration.ofSeconds(2));
        Instant after=Instant.now();
        assertTrue(result.isEmpty());
        assertTrue(Duration.between(now, after).toMillis() >= 2000);

    }

    @Test
    public void dequeueNonBlockingReturnsEmptyForEmptyQueue() {
        RedisClient client = RedisClient.builder()
                .hostAndPort("localhost", 6379)
                .build();

        RedisJobQueue redisJobQueue = new RedisJobQueue(client);
        String queueName = "nonblocking-empty-" + UUID.randomUUID();

        Optional<JobId> result = redisJobQueue.dequeueNonBlocking(queueName);

        assertTrue(result.isEmpty());
    }

    @Test
    public void sizeReturnsNumberOfQueuedJobs() {
        RedisClient client = RedisClient.builder()
                .hostAndPort("localhost", 6379)
                .build();

        RedisJobQueue redisJobQueue = new RedisJobQueue(client);
        String queueName = "size-test-" + UUID.randomUUID();

        JobId id1 = JobId.generate();
        JobId id2 = JobId.generate();
        JobId id3 = JobId.generate();

        redisJobQueue.enqueue(id1, queueName);
        redisJobQueue.enqueue(id2, queueName);
        redisJobQueue.enqueue(id3, queueName);

        assertEquals(3, redisJobQueue.size(queueName));

        redisJobQueue.dequeueNonBlocking(queueName);

        assertEquals(2, redisJobQueue.size(queueName));
    }

    @Test
    public void peekReturnsRequestedJobsWithoutRemovingThem() {
        RedisClient client = RedisClient.builder()
                .hostAndPort("localhost", 6379)
                .build();

        RedisJobQueue redisJobQueue = new RedisJobQueue(client);
        String queueName = "peek-test-" + UUID.randomUUID();

        JobId id1 = JobId.generate();
        JobId id2 = JobId.generate();
        JobId id3 = JobId.generate();

        redisJobQueue.enqueue(id1, queueName);
        redisJobQueue.enqueue(id2, queueName);
        redisJobQueue.enqueue(id3, queueName);

        List<JobId> result = redisJobQueue.peek(queueName, 2);

        assertEquals(List.of(id1, id2), result);
        assertEquals(3, redisJobQueue.size(queueName));
    }

    @Test
    public void peekReturnsAllAvailableJobsWhenCountExceedsQueueSize() {
        RedisClient client = RedisClient.builder()
                .hostAndPort("localhost", 6379)
                .build();

        RedisJobQueue redisJobQueue = new RedisJobQueue(client);
        String queueName = "peek-overflow-" + UUID.randomUUID();

        JobId id1 = JobId.generate();
        JobId id2 = JobId.generate();

        redisJobQueue.enqueue(id1, queueName);
        redisJobQueue.enqueue(id2, queueName);

        List<JobId> result = redisJobQueue.peek(queueName, 10);

        assertEquals(List.of(id1, id2), result);
        assertEquals(2, redisJobQueue.size(queueName));
    }

    @Test
    public void namedQueuesAreIndependent() {
        RedisClient client = RedisClient.builder()
                .hostAndPort("localhost", 6379)
                .build();

        RedisJobQueue redisJobQueue = new RedisJobQueue(client);

        String queueA = "queue-a-" + UUID.randomUUID();
        String queueB = "queue-b-" + UUID.randomUUID();

        JobId idA = JobId.generate();
        JobId idB = JobId.generate();

        redisJobQueue.enqueue(idA, queueA);
        redisJobQueue.enqueue(idB, queueB);

        assertEquals(Optional.of(idA), redisJobQueue.dequeueNonBlocking(queueA));
        assertEquals(Optional.of(idB), redisJobQueue.dequeueNonBlocking(queueB));

        assertEquals(0, redisJobQueue.size(queueA));
        assertEquals(0, redisJobQueue.size(queueB));
    }

    @Test
    public void enqueueRejectsNullJobId() {
        RedisClient client = RedisClient.builder()
                .hostAndPort("localhost", 6379)
                .build();

        RedisJobQueue redisJobQueue = new RedisJobQueue(client);
        String queueName = "null-job-" + UUID.randomUUID();

        assertThrows(
                IllegalArgumentException.class,
                () -> redisJobQueue.enqueue(null, queueName)
        );
    }

    @Test
    public void queueOperationsRejectNullQueueName() {
        RedisClient client = RedisClient.builder()
                .hostAndPort("localhost", 6379)
                .build();

        RedisJobQueue redisJobQueue = new RedisJobQueue(client);

        assertThrows(
                IllegalArgumentException.class,
                () -> redisJobQueue.enqueue(JobId.generate(), null)
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> redisJobQueue.dequeue(null, Duration.ofSeconds(1))
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> redisJobQueue.dequeueNonBlocking(null)
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> redisJobQueue.size(null)
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> redisJobQueue.peek(null, 1)
        );
    }

    @Test
    public void queueOperationsRejectBlankQueueName() {
        RedisClient client = RedisClient.builder()
                .hostAndPort("localhost", 6379)
                .build();

        RedisJobQueue redisJobQueue = new RedisJobQueue(client);

        assertThrows(
                IllegalArgumentException.class,
                () -> redisJobQueue.enqueue(JobId.generate(), "   ")
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> redisJobQueue.dequeue("   ", Duration.ofSeconds(1))
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> redisJobQueue.dequeueNonBlocking("   ")
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> redisJobQueue.size("   ")
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> redisJobQueue.peek("   ", 1)
        );
    }

    @Test
    public void dequeueRejectsInvalidTimeout() {
        RedisClient client = RedisClient.builder()
                .hostAndPort("localhost", 6379)
                .build();

        RedisJobQueue redisJobQueue = new RedisJobQueue(client);
        String queueName = "invalid-timeout-" + UUID.randomUUID();

        assertThrows(
                IllegalArgumentException.class,
                () -> redisJobQueue.dequeue(queueName, null)
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> redisJobQueue.dequeue(queueName, Duration.ZERO)
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> redisJobQueue.dequeue(queueName, Duration.ofSeconds(-1))
        );
    }

    @Test
    public void peekRejectsNegativeCount() {
        RedisClient client = RedisClient.builder()
                .hostAndPort("localhost", 6379)
                .build();

        RedisJobQueue redisJobQueue = new RedisJobQueue(client);
        String queueName = "negative-count-" + UUID.randomUUID();

        assertThrows(
                IllegalArgumentException.class,
                () -> redisJobQueue.peek(queueName, -1)
        );
    }

    @Test
    public void peekWithZeroCountReturnsEmptyWithoutRemovingJobs() {
        RedisClient client = RedisClient.builder()
                .hostAndPort("localhost", 6379)
                .build();

        RedisJobQueue redisJobQueue = new RedisJobQueue(client);
        String queueName = "zero-count-" + UUID.randomUUID();

        redisJobQueue.enqueue(JobId.generate(), queueName);
        redisJobQueue.enqueue(JobId.generate(), queueName);

        List<JobId> result = redisJobQueue.peek(queueName, 0);

        assertTrue(result.isEmpty());
        assertEquals(2, redisJobQueue.size(queueName));
    }

    @Test
    public void redisFailureDuringEnqueueThrowsQueueException() {
        RedisClient client = RedisClient.builder()
                .hostAndPort("localhost", 6390)
                .build();

        RedisJobQueue redisJobQueue = new RedisJobQueue(client);
        String queueName = "redis-failure-" + UUID.randomUUID();

        assertThrows(
                QueueException.class,
                () -> redisJobQueue.enqueue(JobId.generate(), queueName)
        );
    }

    @Test
    public void redisFailureDuringDequeueThrowsQueueException() {
        RedisClient client = RedisClient.builder()
                .hostAndPort("localhost", 6390)
                .build();

        RedisJobQueue redisJobQueue = new RedisJobQueue(client);
        String queueName = "redis-failure-" + UUID.randomUUID();

        assertThrows(
                QueueException.class,
                () -> redisJobQueue.dequeueNonBlocking(queueName)
        );
    }

    @Test
    public void concurrentConsumersDequeueEachJobExactlyOnce() throws ExecutionException, InterruptedException {
        RedisClient client = RedisClient.builder()
                .hostAndPort("localhost", 6379)
                .build();
        RedisJobQueue redisJobQueue=new RedisJobQueue(client);
        String queueName = "concurrent-test-" + UUID.randomUUID();

        Set<JobId> submittedJobs = new HashSet<>();

        for (int i = 0; i < 50; i++) {
            JobId jobId = JobId.generate();
            submittedJobs.add(jobId);
            redisJobQueue.enqueue(jobId, queueName);
        }

        assertEquals(50, redisJobQueue.size(queueName));

        ExecutorService executorService = Executors.newFixedThreadPool(10);

        Set<JobId> dequeuedJobs = ConcurrentHashMap.newKeySet();

        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            futures.add(executorService.submit(() -> {
                while (true) {
                    Optional<JobId> result =
                            redisJobQueue.dequeueNonBlocking(queueName);

                    if (result.isEmpty()) {
                        break;
                    }

                    dequeuedJobs.add(result.get());
                }
            }));
        }

        for (Future<?> future : futures) {
            future.get();
        }

        executorService.shutdown();

        assertEquals(50, dequeuedJobs.size());
        assertEquals(submittedJobs, dequeuedJobs);
        assertEquals(0, redisJobQueue.size(queueName));
    }
}
