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
        assertTrue(message.contains("Connection refused"));
        org.junit.jupiter.api.Assertions.assertFalse(message.contains("Failed to get queue size"));
    }

    @Test
    void mapsNestedPersistenceExceptionToUnderlyingRedisMessage() {
        String message = CliExecutionExceptionHandler.userMessage(
                new io.github.pandeyayushk.jobstream.persistence.PersistenceException(
                        "Redis Exception",
                        new JedisConnectionException("Connection timed out")
                )
        );

        assertEquals("Unable to connect to Redis: Connection timed out", message);
    }

    @Test
    void mapsDirectJedisExceptionMessage() {
        String message = CliExecutionExceptionHandler.userMessage(
                new JedisConnectionException("Network unreachable")
        );

        assertEquals("Unable to connect to Redis: Network unreachable", message);
    }

    @Test
    void fallsBackToDefaultRedisMessageWhenNoDetail() {
        String message = CliExecutionExceptionHandler.userMessage(
                new RuntimeException(
                        "Outer application error",
                        new JedisConnectionException("")
                )
        );

        assertEquals("Unable to connect to Redis.", message);
    }

    @Test
    void fallsBackWhenMessageIsMissing() {
        assertEquals(
                "Command failed.",
                CliExecutionExceptionHandler.userMessage(new RuntimeException())
        );
    }
}
