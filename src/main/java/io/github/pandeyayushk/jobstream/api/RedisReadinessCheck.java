package io.github.pandeyayushk.jobstream.api;

import io.github.pandeyayushk.jobstream.config.RedisConfig;
import redis.clients.jedis.RedisClient;

import java.util.Objects;

/** Performs an authenticated Redis PING using the application's Redis configuration. */
public final class RedisReadinessCheck implements ReadinessCheck {
    private final RedisConfig config;

    public RedisReadinessCheck(RedisConfig config) {
        this.config = Objects.requireNonNull(config, "Redis config cannot be null");
    }

    @Override
    public void check() {
        try (RedisClient client = RedisClient.create(
                config.host(), config.port(), null, config.password()
        )) {
            if (!"PONG".equals(client.ping())) {
                throw new IllegalStateException("Redis readiness ping returned an unexpected response");
            }
        }
    }
}
