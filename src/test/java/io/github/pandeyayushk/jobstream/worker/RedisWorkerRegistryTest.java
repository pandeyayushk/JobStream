package io.github.pandeyayushk.jobstream.worker;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.RedisClient;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class RedisWorkerRegistryTest {

    private RedisClient client;
    private RedisWorkerRegistry registry;

    @BeforeEach
    void setUp() {
        client = RedisClient.builder()
                .hostAndPort("localhost", 6379)
                .build();

        registry = new RedisWorkerRegistry(
                client,
                Duration.ofSeconds(30)
        );
    }

    @AfterEach
    void tearDown() {
        client.flushDB();
    }

    @Test
    void registerStoresWorkerMetadata() {
        WorkerId workerId = WorkerId.generate();
        Instant startedAt = Instant.now();

        WorkerInfo info = new WorkerInfo(
                workerId,
                WorkerStatus.STARTING,
                startedAt
        );

        registry.register(info);

        String workerKey = "jobstream:worker:" + workerId;

        Map<String, String> metadata = client.hgetAll(workerKey);

        assertEquals(workerId.toString(), metadata.get("id"));
        assertEquals(WorkerStatus.STARTING.name(), metadata.get("status"));
        assertEquals(startedAt.toString(), metadata.get("startedAt"));
    }

    @Test
    void registerCreatesHeartbeat() {
        WorkerId workerId = WorkerId.generate();

        WorkerInfo info = new WorkerInfo(
                workerId,
                WorkerStatus.STARTING,
                Instant.now()
        );

        registry.register(info);

        String heartbeatKey =
                "jobstream:worker:" + workerId + ":heartbeat";

        assertEquals("alive", client.get(heartbeatKey));
    }

    @Test
    void registerSetsHeartbeatTtl() {
        WorkerId workerId = WorkerId.generate();

        WorkerInfo info = new WorkerInfo(
                workerId,
                WorkerStatus.STARTING,
                Instant.now()
        );

        registry.register(info);

        String heartbeatKey =
                "jobstream:worker:" + workerId + ":heartbeat";

        long ttl = client.ttl(heartbeatKey);

        assertTrue(ttl > 0);
        assertTrue(ttl <= 30);
    }

    @Test
    void registerRejectsNullWorkerInfo() {
        assertThrowsExactly(
                NullPointerException.class,
                () -> registry.register(null)
        );
    }

    @Test
    void redisFailureThrowsWorkerException() {
        RedisClient unavailableClient = RedisClient.builder()
                .hostAndPort("localhost", 6399)
                .build();

        RedisWorkerRegistry unavailableRegistry =
                new RedisWorkerRegistry(
                        unavailableClient,
                        Duration.ofSeconds(30)
                );

        WorkerInfo info = new WorkerInfo(
                WorkerId.generate(),
                WorkerStatus.STARTING,
                Instant.now()
        );

        assertThrows(
                WorkerException.class,
                () -> unavailableRegistry.register(info)
        );
    }

    @Test
    void existingWorkerIsReturned() {
        WorkerId workerId = WorkerId.generate();
        Instant startedAt = Instant.now();

        WorkerInfo info = new WorkerInfo(
                workerId,
                WorkerStatus.STARTING,
                startedAt
        );

        registry.register(info);

        Optional<WorkerInfo> result = registry.getWorker(workerId);

        assertTrue(result.isPresent());

        WorkerInfo returnedInfo = result.get();

        assertEquals(workerId, returnedInfo.workerId());
        assertEquals(WorkerStatus.STARTING, returnedInfo.status());
        assertEquals(startedAt, returnedInfo.startedAt());
    }

    @Test
    void unknownWorkerReturnsEmpty() {
        WorkerId workerId = WorkerId.generate();
        Optional<WorkerInfo> result = registry.getWorker(workerId);
        assertTrue(result.isEmpty());
    }

    @Test
    public void nullWorkerIdIsRejected(){
        assertThrowsExactly(NullPointerException.class,()->
                registry.getWorker(null)
        );
    }

    @Test
    public void getWorkerRedisFailureThrowsWorkerException(){
        RedisClient unavailableClient = RedisClient.builder()
                .hostAndPort("localhost", 6399)
                .build();

        RedisWorkerRegistry unavailableRegistry =
                new RedisWorkerRegistry(
                        unavailableClient,
                        Duration.ofSeconds(30)
                );

        WorkerId id=WorkerId.generate();

        assertThrowsExactly(WorkerException.class,() ->
                unavailableRegistry.getWorker(id)
        );
    }

    @Test
    void malformedMetadataThrowsWorkerException() {
        WorkerId workerId = WorkerId.generate();

        String workerKey = "jobstream:worker:" + workerId;

        client.hset(
                workerKey,
                Map.of(
                        "id", workerId.toString(),
                        "status", "NOT_A_REAL_STATUS",
                        "startedAt", Instant.now().toString()
                )
        );

        assertThrowsExactly(WorkerException.class,() ->
                registry.getWorker(workerId)
        );
    }

    @Test
    void heartbeatRegisteredWorkerRefreshesHeartbeat() {
        WorkerId workerId = WorkerId.generate();

        WorkerInfo info = new WorkerInfo(
                workerId,
                WorkerStatus.RUNNING,
                Instant.now()
        );

        registry.register(info);

        String heartbeatKey =
                "jobstream:worker:" + workerId + ":heartbeat";

        // Force the existing heartbeat close to expiry.
        client.expire(heartbeatKey, 1);

        registry.heartbeat(workerId);

        assertEquals("alive", client.get(heartbeatKey));

        long ttl = client.ttl(heartbeatKey);

        assertTrue(ttl > 1);
        assertTrue(ttl <= 30);
    }

    @Test
    void heartbeatUnknownWorkerThrowsWorkerException() {
        WorkerId workerId = WorkerId.generate();

        assertThrowsExactly(
                WorkerException.class,
                () -> registry.heartbeat(workerId)
        );
    }

    @Test
    void heartbeatNullWorkerIdIsRejected() {
        assertThrowsExactly(
                NullPointerException.class,
                () -> registry.heartbeat(null)
        );
    }

    @Test
    void heartbeatRedisFailureThrowsWorkerException() {
        RedisClient unavailableClient = RedisClient.builder()
                .hostAndPort("localhost", 6399)
                .build();

        RedisWorkerRegistry unavailableRegistry =
                new RedisWorkerRegistry(
                        unavailableClient,
                        Duration.ofSeconds(30)
                );

        WorkerId workerId = WorkerId.generate();

        assertThrowsExactly(
                WorkerException.class,
                () -> unavailableRegistry.heartbeat(workerId)
        );
    }

    @Test
    void heartbeatDoesNotModifyWorkerMetadata() {
        WorkerId workerId = WorkerId.generate();
        Instant startedAt = Instant.now();

        WorkerInfo info = new WorkerInfo(
                workerId,
                WorkerStatus.RUNNING,
                startedAt
        );

        registry.register(info);

        String workerKey =
                "jobstream:worker:" + workerId;

        Map<String, String> metadataBefore =
                client.hgetAll(workerKey);

        registry.heartbeat(workerId);

        Map<String, String> metadataAfter =
                client.hgetAll(workerKey);

        assertEquals(metadataBefore, metadataAfter);
    }

    @Test
    void deregisterRemovesWorkerMetadata() {
        WorkerId workerId = WorkerId.generate();

        WorkerInfo info = new WorkerInfo(
                workerId,
                WorkerStatus.RUNNING,
                Instant.now()
        );

        registry.register(info);

        String workerKey =
                "jobstream:worker:" + workerId;

        assertFalse(client.hgetAll(workerKey).isEmpty());

        registry.deregister(workerId);

        assertTrue(client.hgetAll(workerKey).isEmpty());
    }

    @Test
    void deregisterRemovesHeartbeat() {
        WorkerId workerId = WorkerId.generate();

        WorkerInfo info = new WorkerInfo(
                workerId,
                WorkerStatus.RUNNING,
                Instant.now()
        );

        registry.register(info);

        String heartbeatKey =
                "jobstream:worker:" + workerId + ":heartbeat";

        assertEquals("alive", client.get(heartbeatKey));

        registry.deregister(workerId);

        assertNull(client.get(heartbeatKey));
    }

    @Test
    void deregisterUnknownWorkerIsNoOp() {
        WorkerId workerId = WorkerId.generate();

        assertDoesNotThrow(
                () -> registry.deregister(workerId)
        );

        String workerKey =
                "jobstream:worker:" + workerId;

        String heartbeatKey =
                "jobstream:worker:" + workerId + ":heartbeat";

        assertTrue(client.hgetAll(workerKey).isEmpty());
        assertNull(client.get(heartbeatKey));
    }

    @Test
    void deregisterNullWorkerIdIsRejected() {
        assertThrowsExactly(
                NullPointerException.class,
                () -> registry.deregister(null)
        );
    }

    @Test
    void deregisterRedisFailureThrowsWorkerException() {
        RedisClient unavailableClient = RedisClient.builder()
                .hostAndPort("localhost", 6399)
                .build();

        RedisWorkerRegistry unavailableRegistry =
                new RedisWorkerRegistry(
                        unavailableClient,
                        Duration.ofSeconds(30)
                );

        WorkerId workerId = WorkerId.generate();

        assertThrowsExactly(
                WorkerException.class,
                () -> unavailableRegistry.deregister(workerId)
        );
    }

    @Test
    void listActiveWorkersReturnsRegisteredWorkersWithLiveHeartbeats() {
        WorkerId firstWorkerId = WorkerId.generate();
        WorkerId secondWorkerId = WorkerId.generate();

        Instant firstStartedAt = Instant.now();
        Instant secondStartedAt = Instant.now();

        registry.register(
                new WorkerInfo(
                        firstWorkerId,
                        WorkerStatus.RUNNING,
                        firstStartedAt
                )
        );

        registry.register(
                new WorkerInfo(
                        secondWorkerId,
                        WorkerStatus.RUNNING,
                        secondStartedAt
                )
        );

        List<WorkerInfo> workers = registry.listActiveWorkers();

        assertEquals(2, workers.size());

        assertTrue(
                workers.stream()
                        .anyMatch(worker ->
                                worker.workerId().equals(firstWorkerId))
        );

        assertTrue(
                workers.stream()
                        .anyMatch(worker ->
                                worker.workerId().equals(secondWorkerId))
        );
    }

    @Test
    void listActiveWorkersExcludesWorkersWithExpiredHeartbeats() {
        WorkerId activeWorkerId = WorkerId.generate();
        WorkerId inactiveWorkerId = WorkerId.generate();

        registry.register(
                new WorkerInfo(
                        activeWorkerId,
                        WorkerStatus.RUNNING,
                        Instant.now()
                )
        );

        registry.register(
                new WorkerInfo(
                        inactiveWorkerId,
                        WorkerStatus.RUNNING,
                        Instant.now()
                )
        );

        String inactiveHeartbeatKey =
                "jobstream:worker:" + inactiveWorkerId + ":heartbeat";

        client.del(inactiveHeartbeatKey);

        List<WorkerInfo> workers = registry.listActiveWorkers();

        assertEquals(1, workers.size());
        assertEquals(activeWorkerId, workers.get(0).workerId());
    }

    @Test
    void listActiveWorkersReturnsEmptyWhenNoWorkersAreActive() {
        List<WorkerInfo> workers = registry.listActiveWorkers();

        assertTrue(workers.isEmpty());
    }

    @Test
    void listActiveWorkersIgnoresHeartbeatKeys() {
        WorkerId workerId = WorkerId.generate();

        registry.register(
                new WorkerInfo(
                        workerId,
                        WorkerStatus.RUNNING,
                        Instant.now()
                )
        );

        String heartbeatKey =
                "jobstream:worker:" + workerId + ":heartbeat";

        assertTrue(client.exists(heartbeatKey));

        List<WorkerInfo> workers = registry.listActiveWorkers();

        assertEquals(1, workers.size());
        assertEquals(workerId, workers.get(0).workerId());
    }

    @Test
    void listActiveWorkersRedisFailureThrowsWorkerException() {
        RedisClient unavailableClient = RedisClient.builder()
                .hostAndPort("localhost", 6399)
                .build();

        RedisWorkerRegistry unavailableRegistry =
                new RedisWorkerRegistry(
                        unavailableClient,
                        Duration.ofSeconds(30)
                );

        assertThrowsExactly(
                WorkerException.class,
                unavailableRegistry::listActiveWorkers
        );
    }

    @Test
    void heartbeatFailsWhenWorkerIsDeregisteredBeforeTransactionExec() {
        WorkerId workerId = WorkerId.generate();

        WorkerInfo info = new WorkerInfo(
                workerId,
                WorkerStatus.RUNNING,
                Instant.now()
        );

        registry.register(info);

        registry.deregister(workerId);

        assertThrows(
                WorkerException.class,
                () -> registry.heartbeat(workerId)
        );

        String heartbeatKey =
                "jobstream:worker:" + workerId + ":heartbeat";

        assertFalse(
                client.exists(heartbeatKey)
        );
    }
}