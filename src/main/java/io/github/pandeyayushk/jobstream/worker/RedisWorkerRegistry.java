package io.github.pandeyayushk.jobstream.worker;

import redis.clients.jedis.RedisClient;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class RedisWorkerRegistry implements WorkerRegistry{

    private final RedisClient client;
    private final Duration heartbeatTtl;

    public RedisWorkerRegistry(RedisClient client, Duration heartbeatTtl) {
        Objects.requireNonNull(client,"Redis Client can not be null");
        Objects.requireNonNull(heartbeatTtl,"Heartbeat Ttl cannot be null");
        if(heartbeatTtl.isZero()||heartbeatTtl.isNegative())throw new IllegalArgumentException("Heartbeat Ttl must be greater than zero");
        this.client = client;
        this.heartbeatTtl = heartbeatTtl;
    }

    @Override
    public void register(WorkerInfo info) {
        Objects.requireNonNull(info, "WorkerInfo cannot be null");

        String workerKey = "jobstream:worker:" + info.workerId();
        String heartbeatKey = workerKey + ":heartbeat";
        Map<String,String> metadata=Map.of("id",info.workerId().toString(),"status",info.status().name()
        ,"startedAt",info.startedAt().toString());
        try {
            var transaction = client.multi();
            transaction.hset(workerKey,metadata);
            transaction.set(heartbeatKey,"alive");
            transaction.expire(heartbeatKey, heartbeatTtl.getSeconds());
            transaction.exec();
        }catch (JedisException e){
            throw new WorkerException("Failed to register worker",e);
        }
    }

    @Override
    public void heartbeat(WorkerId workerId) {
        Objects.requireNonNull(
                workerId,
                "WorkerId cannot be null"
        );

        String workerKey =
                "jobstream:worker:" + workerId;

        String heartbeatKey =
                workerKey + ":heartbeat";

        try (var transaction = client.transaction(false)) {

            /*
             * Watch the worker metadata.
             *
             * If deregistration changes/deletes this key before EXEC,
             * the heartbeat transaction will be aborted.
             */
            transaction.watch(workerKey);

            Map<String, String> metadata =
                    client.hgetAll(workerKey);

            if (metadata.isEmpty()) {
                throw new WorkerException(
                        "Worker does not exist"
                );
            }

            transaction.multi();

            transaction.set(
                    heartbeatKey,
                    "alive"
            );

            transaction.expire(
                    heartbeatKey,
                    heartbeatTtl.getSeconds()
            );

            var results = transaction.exec();

            if (results == null) {
                throw new WorkerException(
                        "Worker registration changed during heartbeat"
                );
            }

        } catch (JedisException e) {
            throw new WorkerException(
                    "Failed to update worker heartbeat",
                    e
            );
        }
    }

    @Override
    public void deregister(WorkerId workerId) {
        Objects.requireNonNull(workerId,"WorkerId can not be null");
        String workerKey = "jobstream:worker:" + workerId;
        String heartbeatKey = workerKey + ":heartbeat";
        try {
            var transaction=client.multi();
            transaction.del(workerKey);
            transaction.del(heartbeatKey);
            transaction.exec();
        }catch (JedisException e){
            throw new WorkerException("Failed to deregister worker",e);
        }
    }

    @Override
    public List<WorkerInfo> listActiveWorkers() {
        List<WorkerInfo> activeWorkers = new ArrayList<>();
        try {
            String cursor = ScanParams.SCAN_POINTER_START;
            ScanParams scanParams = new ScanParams()
                    .match("jobstream:worker:*")
                    .count(100);
            do {
                ScanResult<String> result =
                        client.scan(cursor, scanParams);
                for (String key : result.getResult()) {
                    if (key.endsWith(":heartbeat")) {
                        continue;
                    }
                    String heartbeatKey = key + ":heartbeat";
                    if (!client.exists(heartbeatKey)) {
                        continue;
                    }
                    Map<String, String> metadata = client.hgetAll(key);
                    if (metadata.isEmpty()) {
                        continue;
                    }
                    WorkerId workerId =
                            WorkerId.fromString(metadata.get("id"));
                    WorkerStatus status =
                            WorkerStatus.valueOf(metadata.get("status"));
                    Instant startedAt =
                            Instant.parse(metadata.get("startedAt"));
                    activeWorkers.add(new WorkerInfo(workerId,status,startedAt)
                    );
                }
                cursor = result.getCursor();
            } while (!cursor.equals(ScanParams.SCAN_POINTER_START));
            return activeWorkers;
        } catch (JedisException e) {
            throw new WorkerException(
                    "Failed to list active workers",
                    e
            );
        } catch (RuntimeException e) {
            throw new WorkerException(
                    "Invalid worker metadata",
                    e
            );
        }
    }

    @Override
    public Optional<WorkerInfo> getWorker(WorkerId workerId) {
        Objects.requireNonNull(workerId, "WorkerId cannot be null");
        String workerKey = "jobstream:worker:" + workerId;
        try {
            Map<String, String> metadata = client.hgetAll(workerKey);
            if(metadata.isEmpty())return Optional.empty();
            WorkerId id = WorkerId.fromString(metadata.get("id"));
            WorkerStatus status = WorkerStatus.valueOf(metadata.get("status"));
            Instant startedAt = Instant.parse(metadata.get("startedAt"));
            return Optional.of(
                    new WorkerInfo(id, status, startedAt)
            );
        }catch (JedisException e){
            throw new WorkerException("Failed to find worker",e);
        }catch (RuntimeException e) {
            throw new WorkerException("Invalid worker metadata", e);
        }
    }
}
