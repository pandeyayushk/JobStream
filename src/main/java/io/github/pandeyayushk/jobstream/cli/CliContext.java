package io.github.pandeyayushk.jobstream.cli;

import io.github.pandeyayushk.jobstream.persistence.JobRepository;
import io.github.pandeyayushk.jobstream.persistence.RedisJobRepository;
import io.github.pandeyayushk.jobstream.queue.JobQueue;
import io.github.pandeyayushk.jobstream.queue.QueueCoordinator;
import io.github.pandeyayushk.jobstream.queue.QueueCoordinatorImp;
import io.github.pandeyayushk.jobstream.queue.RedisJobQueue;
import io.github.pandeyayushk.jobstream.queue.RedisJobSubmissionStore;
import io.github.pandeyayushk.jobstream.retry.DeadLetterQueue;
import io.github.pandeyayushk.jobstream.retry.RedisDeadLetterQueue;
import io.github.pandeyayushk.jobstream.serialization.JacksonJobSerializer;
import io.github.pandeyayushk.jobstream.serialization.JobSerializer;
import io.github.pandeyayushk.jobstream.worker.RedisWorkerRegistry;
import io.github.pandeyayushk.jobstream.worker.WorkerConfig;
import io.github.pandeyayushk.jobstream.worker.WorkerRegistry;
import redis.clients.jedis.RedisClient;

public final class CliContext implements AutoCloseable {

    private final String host;
    private final int port;

    private RedisClient client;
    private JobRepository jobRepository;
    private QueueCoordinator queueCoordinator;
    private JobQueue jobQueue;
    private WorkerRegistry workerRegistry;
    private DeadLetterQueue deadLetterQueue;

    public CliContext(String host, int port) {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException(
                    "Redis host cannot be null or blank"
            );
        }

        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException(
                    "Redis port is outside TCP range"
            );
        }

        this.host = host;
        this.port = port;
    }

    public JobRepository jobRepository() {
        initialize();
        return jobRepository;
    }

    public QueueCoordinator queueCoordinator() {
        initialize();
        return queueCoordinator;
    }

    public JobQueue jobQueue() {
        initialize();
        return jobQueue;
    }

    public WorkerRegistry workerRegistry() {
        initialize();
        return workerRegistry;
    }

    public DeadLetterQueue deadLetterQueue() {
        initialize();
        return deadLetterQueue;
    }

    private void initialize() {
        if (client != null) {
            return;
        }

        RedisClient created = RedisClient.create(
                "redis://" + host + ":" + port
        );

        try {
            JobSerializer serializer = new JacksonJobSerializer();

            jobRepository = new RedisJobRepository(
                    created,
                    serializer
            );

            RedisJobSubmissionStore submissionStore =
                    new RedisJobSubmissionStore(
                            created,
                            serializer
                    );

            queueCoordinator = new QueueCoordinatorImp(submissionStore);
            jobQueue = new RedisJobQueue(created);
            workerRegistry = new RedisWorkerRegistry(
                    created,
                    WorkerConfig.defaults().heartbeatTtl()
            );
            deadLetterQueue = new RedisDeadLetterQueue(
                    created,
                    serializer
            );
            client = created;
        } catch (RuntimeException e) {
            created.close();
            jobRepository = null;
            queueCoordinator = null;
            jobQueue = null;
            workerRegistry = null;
            deadLetterQueue = null;
            throw e;
        }
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
            client = null;
            jobRepository = null;
            queueCoordinator = null;
            jobQueue = null;
            workerRegistry = null;
            deadLetterQueue = null;
        }
    }
}
