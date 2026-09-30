package io.github.pandeyayushk.jobstream.cli;

import org.junit.jupiter.api.Test;
import redis.clients.jedis.exceptions.JedisConnectionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CliExecutionExceptionHandlerTest {

    @Test
    void usesIllegalArgumentMessage() {
        String message = CliExecutionExceptionHandler.userMessage(
                new IllegalArgumentException("Invalid JSON payload: unexpected token")
        );

        assertEquals("Invalid JSON payload: unexpected token", message);
    }

    @Test
    void mapsJedisFailuresToRedisConnectionMessage() {
        String message = CliExecutionExceptionHandler.userMessage(
                new RuntimeException(
                        "Failed to get queue size",
                        new JedisConnectionException("Connection refused")
                )
        );

        assertTrue(message.startsWith("Unable to connect to Redis"));
        assertTrue(message.contains("Failed to get queue size"));
    }

    @Test
    void fallsBackWhenMessageIsMissing() {
        assertEquals(
                "Command failed.",
                CliExecutionExceptionHandler.userMessage(new RuntimeException())
        );
    }
}
