package io.github.pandeyayushk.jobstream.api;

import io.github.pandeyayushk.jobstream.config.ApiConfig;
import io.github.pandeyayushk.jobstream.serialization.JacksonJsonCodec;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JobStreamApiServerTest {
    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(2))
            .build();

    @Test
    void constructionDoesNotStartServerAndLifecycleIsExplicit() throws Exception {
        JobStreamApiServer server = newServer(() -> { });

        assertThrows(IllegalStateException.class, server::boundPort);
        server.start();
        int port = server.boundPort();
        assertTrue(port > 0);
        assertEquals(200, get(port, "/health/live").statusCode());

        server.stop();
        server.close();
        assertThrows(IllegalStateException.class, server::boundPort);
        assertThrows(IllegalStateException.class, server::start);
        assertThrows(IOException.class, () -> get(port, "/health/live"));
    }

    @Test
    void livenessReturnsJsonWithoutCallingRedis() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        try (JobStreamApiServer server = newStartedServer(() -> calls.incrementAndGet())) {
            HttpResponse<String> response = get(server.boundPort(), "/health/live");

            assertEquals(200, response.statusCode());
            assertEquals("{\"status\":\"UP\"}", response.body());
            assertJsonContentType(response);
            assertEquals(0, calls.get());
        }
    }

    @Test
    void readinessReturnsUpWhenRedisIsAvailable() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        try (JobStreamApiServer server = newStartedServer(calls::incrementAndGet)) {
            HttpResponse<String> response = get(server.boundPort(), "/health/ready");

            assertEquals(200, response.statusCode());
            assertEquals(
                    "{\"status\":\"UP\",\"dependencies\":{\"redis\":\"UP\"}}",
                    response.body()
            );
            assertJsonContentType(response);
            assertEquals(1, calls.get());
        }
    }

    @Test
    void readinessFailureIsSafeAndDoesNotStopServer() throws Exception {
        try (JobStreamApiServer server = newStartedServer(
                () -> { throw new IllegalStateException("password=private-secret"); }
        )) {
            HttpResponse<String> ready = get(server.boundPort(), "/health/ready");
            HttpResponse<String> live = get(server.boundPort(), "/health/live");

            assertEquals(503, ready.statusCode());
            assertEquals(
                    "{\"status\":\"DOWN\",\"dependencies\":{\"redis\":\"DOWN\"}}",
                    ready.body()
            );
            assertJsonContentType(ready);
            assertFalse(ready.body().contains("private-secret"));
            assertEquals(200, live.statusCode());
        }
    }

    @Test
    void unknownRoutesAndUnsupportedMethodsReturnJsonErrors() throws Exception {
        try (JobStreamApiServer server = newStartedServer(() -> { })) {
            HttpResponse<String> unknown = get(server.boundPort(), "/not-a-route");
            HttpResponse<String> wrongHealthPath = get(server.boundPort(), "/health/live/extra");
            HttpResponse<String> wrongMethod = send(server.boundPort(), "/health/live", "POST");

            assertEquals(404, unknown.statusCode());
            assertEquals("{\"error\":{\"code\":\"NOT_FOUND\",\"message\":\"Resource not found\"}}",
                    unknown.body());
            assertEquals(404, wrongHealthPath.statusCode());
            assertEquals(405, wrongMethod.statusCode());
            assertEquals("{\"error\":{\"code\":\"METHOD_NOT_ALLOWED\",\"message\":\"HTTP method not allowed\"}}",
                    wrongMethod.body());
            assertJsonContentType(unknown);
            assertJsonContentType(wrongMethod);
        }
    }

    @Test
    void closeReleasesBoundPortAndIsIdempotent() throws Exception {
        JobStreamApiServer first = newStartedServer(() -> { });
        int port = first.boundPort();
        assertEquals(200, get(port, "/health/live").statusCode());
        first.close();
        first.close();
        assertTrue(Thread.getAllStackTraces().keySet().stream()
                .noneMatch(thread -> thread.isAlive()
                        && thread.getName().startsWith("jobstream-api-")));

        ApiConfig fixedPortConfig = new ApiConfig("127.0.0.1", port, 1024, 2, 8);
        try (JobStreamApiServer second = new JobStreamApiServer(
                fixedPortConfig, () -> { }, JacksonJsonCodec.shared()
        )) {
            second.start();
            assertEquals(port, second.boundPort());
        }
    }

    private JobStreamApiServer newStartedServer(ReadinessCheck readinessCheck) {
        JobStreamApiServer server = newServer(readinessCheck);
        server.start();
        return server;
    }

    private JobStreamApiServer newServer(ReadinessCheck readinessCheck) {
        ApiConfig config = new ApiConfig("127.0.0.1", 0, 1024, 2, 8);
        return new JobStreamApiServer(config, readinessCheck, JacksonJsonCodec.shared());
    }

    private HttpResponse<String> get(int port, String path) throws IOException, InterruptedException {
        return send(port, path, "GET");
    }

    private HttpResponse<String> send(int port, String path, String method)
            throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + port + path))
                .timeout(Duration.ofSeconds(3))
                .method(method, HttpRequest.BodyPublishers.noBody())
                .build();
        return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private void assertJsonContentType(HttpResponse<String> response) {
        assertTrue(response.headers().firstValue("Content-Type").orElse("")
                .startsWith("application/json"));
    }
}
