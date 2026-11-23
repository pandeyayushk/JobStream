package io.github.pandeyayushk.jobstream.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ApiConfigTest {

    @Test
    void defaultsBindOnlyToLoopbackAndUseDocumentedLimits() {
        ApiConfig config = ApiConfig.defaults();

        assertEquals("127.0.0.1", config.bindAddress());
        assertEquals(8080, config.port());
        assertEquals(1_048_576, config.maxRequestBodyBytes());
        assertEquals(8, config.executorThreads());
        assertEquals(128, config.executorQueueCapacity());
    }

    @Test
    void acceptsValidCustomConfiguration() {
        ApiConfig config = new ApiConfig("127.0.0.2", 9090, 4096, 4, 32);

        assertEquals("127.0.0.2", config.bindAddress());
        assertEquals(9090, config.port());
        assertEquals(4096, config.maxRequestBodyBytes());
        assertEquals(4, config.executorThreads());
        assertEquals(32, config.executorQueueCapacity());
    }

    @Test
    void rejectsPortsOutsideTcpRange() {
        assertThrows(IllegalArgumentException.class, () -> configWithPort(0));
        assertThrows(IllegalArgumentException.class, () -> configWithPort(65536));
    }

    @Test
    void rejectsBlankAndInvalidBindAddresses() {
        assertThrows(IllegalArgumentException.class, () -> configWithAddress(null));
        assertThrows(IllegalArgumentException.class, () -> configWithAddress("  "));
        assertThrows(
                IllegalArgumentException.class,
                () -> configWithAddress("not a valid address")
        );
    }

    @Test
    void trimsValidBindAddress() {
        assertEquals("127.0.0.1", configWithAddress(" 127.0.0.1 ").bindAddress());
    }

    @Test
    void rejectsNonPositiveRequestSizeAndExecutorBounds() {
        assertThrows(IllegalArgumentException.class, () -> configWithRequestSize(0));
        assertThrows(IllegalArgumentException.class, () -> configWithRequestSize(-1));
        assertThrows(IllegalArgumentException.class, () -> configWithThreads(0));
        assertThrows(IllegalArgumentException.class, () -> configWithQueueCapacity(0));
    }

    private ApiConfig configWithPort(int port) {
        return new ApiConfig("127.0.0.1", port, 1024, 2, 8);
    }

    private ApiConfig configWithAddress(String address) {
        return new ApiConfig(address, 8080, 1024, 2, 8);
    }

    private ApiConfig configWithRequestSize(int size) {
        return new ApiConfig("127.0.0.1", 8080, size, 2, 8);
    }

    private ApiConfig configWithThreads(int threads) {
        return new ApiConfig("127.0.0.1", 8080, 1024, threads, 8);
    }

    private ApiConfig configWithQueueCapacity(int capacity) {
        return new ApiConfig("127.0.0.1", 8080, 1024, 2, capacity);
    }
}
