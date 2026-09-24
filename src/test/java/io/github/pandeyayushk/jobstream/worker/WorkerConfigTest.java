package io.github.pandeyayushk.jobstream.worker;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class WorkerConfigTest {

    @Test
    void validValuesAreStored() {
        Duration heartbeatInterval = Duration.ofSeconds(10);
        Duration heartbeatTtl = Duration.ofSeconds(30);
        Duration shutdownTimeout = Duration.ofSeconds(20);

        WorkerConfig config = new WorkerConfig(
                "email",
                4,
                heartbeatInterval,
                heartbeatTtl,
                shutdownTimeout
        );

        assertEquals("email", config.queueName());
        assertEquals(4, config.concurrency());
        assertEquals(heartbeatInterval, config.heartbeatInterval());
        assertEquals(heartbeatTtl, config.heartbeatTtl());
        assertEquals(shutdownTimeout, config.shutdownTimeout());
    }

    @Test
    void defaultsReturnExpectedConfiguration() {
        WorkerConfig config = WorkerConfig.defaults();

        assertEquals("default", config.queueName());
        assertEquals(4, config.concurrency());
        assertEquals(Duration.ofSeconds(10), config.heartbeatInterval());
        assertEquals(Duration.ofSeconds(30), config.heartbeatTtl());
        assertEquals(Duration.ofSeconds(30), config.shutdownTimeout());
    }

    @Test
    void rejectsNullQueueName() {
        assertThrowsExactly(
                NullPointerException.class,
                () -> new WorkerConfig(
                        null, 4,
                        Duration.ofSeconds(10),
                        Duration.ofSeconds(30),
                        Duration.ofSeconds(30)
                )
        );
    }

    @Test
    void rejectsBlankQueueName() {
        assertThrowsExactly(
                IllegalArgumentException.class,
                () -> new WorkerConfig(
                        "   ", 4,
                        Duration.ofSeconds(10),
                        Duration.ofSeconds(30),
                        Duration.ofSeconds(30)
                )
        );
    }

    @Test
    void rejectsZeroConcurrency() {
        assertThrowsExactly(
                IllegalArgumentException.class,
                () -> new WorkerConfig(
                        "default", 0,
                        Duration.ofSeconds(10),
                        Duration.ofSeconds(30),
                        Duration.ofSeconds(30)
                )
        );
    }

    @Test
    void rejectsNegativeConcurrency() {
        assertThrowsExactly(
                IllegalArgumentException.class,
                () -> new WorkerConfig(
                        "default", -1,
                        Duration.ofSeconds(10),
                        Duration.ofSeconds(30),
                        Duration.ofSeconds(30)
                )
        );
    }

    @Test
    void rejectsNullHeartbeatInterval() {
        assertThrowsExactly(
                NullPointerException.class,
                () -> new WorkerConfig(
                        "default", 4,
                        null,
                        Duration.ofSeconds(30),
                        Duration.ofSeconds(30)
                )
        );
    }

    @Test
    void rejectsNonPositiveHeartbeatInterval() {
        assertThrowsExactly(
                IllegalArgumentException.class,
                () -> new WorkerConfig(
                        "default", 4,
                        Duration.ZERO,
                        Duration.ofSeconds(30),
                        Duration.ofSeconds(30)
                )
        );
    }

    @Test
    void rejectsNullHeartbeatTtl() {
        assertThrowsExactly(
                NullPointerException.class,
                () -> new WorkerConfig(
                        "default", 4,
                        Duration.ofSeconds(10),
                        null,
                        Duration.ofSeconds(30)
                )
        );
    }

    @Test
    void rejectsNonPositiveHeartbeatTtl() {
        assertThrowsExactly(
                IllegalArgumentException.class,
                () -> new WorkerConfig(
                        "default", 4,
                        Duration.ofSeconds(10),
                        Duration.ZERO,
                        Duration.ofSeconds(30)
                )
        );
    }

    @Test
    void rejectsNullShutdownTimeout() {
        assertThrowsExactly(
                NullPointerException.class,
                () -> new WorkerConfig(
                        "default", 4,
                        Duration.ofSeconds(10),
                        Duration.ofSeconds(30),
                        null
                )
        );
    }

    @Test
    void rejectsNonPositiveShutdownTimeout() {
        assertThrowsExactly(
                IllegalArgumentException.class,
                () -> new WorkerConfig(
                        "default", 4,
                        Duration.ofSeconds(10),
                        Duration.ofSeconds(30),
                        Duration.ZERO
                )
        );
    }

    @Test
    void rejectsHeartbeatIntervalEqualToTtl() {
        assertThrowsExactly(
                IllegalArgumentException.class,
                () -> new WorkerConfig(
                        "default", 4,
                        Duration.ofSeconds(30),
                        Duration.ofSeconds(30),
                        Duration.ofSeconds(30)
                )
        );
    }

    @Test
    void rejectsHeartbeatIntervalGreaterThanTtl() {
        assertThrowsExactly(
                IllegalArgumentException.class,
                () -> new WorkerConfig(
                        "default", 4,
                        Duration.ofSeconds(40),
                        Duration.ofSeconds(30),
                        Duration.ofSeconds(30)
                )
        );
    }

    @Test
    void acceptsHeartbeatIntervalLessThanTtl() {
        WorkerConfig config = new WorkerConfig(
                "default",
                4,
                Duration.ofSeconds(10),
                Duration.ofSeconds(30),
                Duration.ofSeconds(30)
        );

        assertEquals(Duration.ofSeconds(10), config.heartbeatInterval());
        assertEquals(Duration.ofSeconds(30), config.heartbeatTtl());
    }
}