package io.github.pandeyayushk.jobstream.cli;

import io.github.pandeyayushk.jobstream.persistence.JobRepository;
import io.github.pandeyayushk.jobstream.persistence.RedisJobRepository;
import io.github.pandeyayushk.jobstream.queue.QueueCoordinator;
import io.github.pandeyayushk.jobstream.queue.QueueCoordinatorImp;
import io.github.pandeyayushk.jobstream.queue.RedisJobSubmissionStore;
import io.github.pandeyayushk.jobstream.serialization.JacksonJobSerializer;
import io.github.pandeyayushk.jobstream.serialization.JobSerializer;
import redis.clients.jedis.RedisClient;

import java.util.Objects;

public final class CliContext implements AutoCloseable {

    private final String host;
    private final int port;

    private RedisClient client;
    private JobRepository jobRepository;
    private QueueCoordinator queueCoordinator;

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

    private void initialize() {
        if (client != null) {
            return;
        }

        client = RedisClient.create(
                "redis://" + host + ":" + port
        );

        JobSerializer serializer = new JacksonJobSerializer();

        jobRepository = new RedisJobRepository(
                client,
                serializer
        );

        RedisJobSubmissionStore submissionStore =
                new RedisJobSubmissionStore(
                        client,
                        serializer
                );

        queueCoordinator =
                new QueueCoordinatorImp(submissionStore);
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
            client = null;
            jobRepository = null;
            queueCoordinator = null;
        }
    }
}