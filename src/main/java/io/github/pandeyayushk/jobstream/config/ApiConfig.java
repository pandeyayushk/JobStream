package io.github.pandeyayushk.jobstream.config;

import java.net.InetAddress;
import java.net.UnknownHostException;

/** Immutable configuration for the future embedded HTTP API. */
public record ApiConfig(
        String bindAddress,
        int port,
        int maxRequestBodyBytes,
        int executorThreads,
        int executorQueueCapacity
) {
    public static final String DEFAULT_BIND_ADDRESS = "127.0.0.1";
    public static final int DEFAULT_PORT = 8080;
    public static final int DEFAULT_MAX_REQUEST_BODY_BYTES = 1_048_576;
    public static final int DEFAULT_EXECUTOR_THREADS = 8;
    public static final int DEFAULT_EXECUTOR_QUEUE_CAPACITY = 128;

    public ApiConfig {
        if (bindAddress == null || bindAddress.isBlank()) {
            throw new IllegalArgumentException(
                    "Bind address cannot be null or blank"
            );
        }
        bindAddress = bindAddress.trim();
        try {
            InetAddress.getByName(bindAddress);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("Bind address is invalid", e);
        }

        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException(
                    "Port must be between 1 and 65535"
            );
        }
        if (maxRequestBodyBytes <= 0) {
            throw new IllegalArgumentException(
                    "Maximum request body size must be positive"
            );
        }
        if (executorThreads <= 0) {
            throw new IllegalArgumentException(
                    "Executor thread count must be positive"
            );
        }
        if (executorQueueCapacity <= 0) {
            throw new IllegalArgumentException(
                    "Executor queue capacity must be positive"
            );
        }
    }

    public static ApiConfig defaults() {
        return new ApiConfig(
                DEFAULT_BIND_ADDRESS,
                DEFAULT_PORT,
                DEFAULT_MAX_REQUEST_BODY_BYTES,
                DEFAULT_EXECUTOR_THREADS,
                DEFAULT_EXECUTOR_QUEUE_CAPACITY
        );
    }
}
