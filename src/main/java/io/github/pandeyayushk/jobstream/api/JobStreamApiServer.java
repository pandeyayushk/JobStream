package io.github.pandeyayushk.jobstream.api;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.github.pandeyayushk.jobstream.config.ApiConfig;
import io.github.pandeyayushk.jobstream.serialization.JacksonJsonCodec;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/** Embedded JDK HTTP server providing process health endpoints. */
public final class JobStreamApiServer implements AutoCloseable {
    private static final Logger LOGGER = Logger.getLogger(JobStreamApiServer.class.getName());
    private static final String JSON_CONTENT_TYPE = "application/json; charset=utf-8";
    private static final long EXECUTOR_SHUTDOWN_SECONDS = 5;

    private final ApiConfig config;
    private final ReadinessCheck readinessCheck;
    private final JacksonJsonCodec jsonCodec;
    private Lifecycle lifecycle = Lifecycle.NEW;
    private HttpServer server;
    private ThreadPoolExecutor executor;

    public JobStreamApiServer(
            ApiConfig config,
            ReadinessCheck readinessCheck,
            JacksonJsonCodec jsonCodec
    ) {
        this.config = Objects.requireNonNull(config, "API config cannot be null");
        this.readinessCheck = Objects.requireNonNull(
                readinessCheck, "Readiness check cannot be null"
        );
        this.jsonCodec = Objects.requireNonNull(jsonCodec, "JSON codec cannot be null");
    }

    /** Starts the server once. Construction alone does not bind or start it. */
    public synchronized void start() {
        if (lifecycle != Lifecycle.NEW) {
            throw new IllegalStateException("HTTP API server can only be started once");
        }

        try {
            executor = new ThreadPoolExecutor(
                    config.executorThreads(),
                    config.executorThreads(),
                    0L,
                    TimeUnit.MILLISECONDS,
                    new ArrayBlockingQueue<>(config.executorQueueCapacity()),
                    namedThreadFactory(),
                    new ThreadPoolExecutor.AbortPolicy()
            );
            server = HttpServer.create();
            server.createContext("/health/live", new RouteHandler(Route.LIVE));
            server.createContext("/health/ready", new RouteHandler(Route.READY));
            server.createContext("/", new RouteHandler(Route.NOT_FOUND));
            server.setExecutor(executor);
            server.bind(new InetSocketAddress(config.bindAddress(), config.port()), 0);
            server.start();
            lifecycle = Lifecycle.RUNNING;
        } catch (IOException | RuntimeException e) {
            lifecycle = Lifecycle.STOPPED;
            if (server != null) {
                server.stop(0);
                server = null;
            }
            shutdownExecutor();
            throw new IllegalStateException("Failed to start HTTP API server", e);
        }
    }

    /** Stops a running server. Calling stop before start is an invalid lifecycle operation. */
    public synchronized void stop() {
        if (lifecycle == Lifecycle.NEW) {
            throw new IllegalStateException("HTTP API server has not been started");
        }
        if (lifecycle == Lifecycle.RUNNING) {
            stopResources();
            lifecycle = Lifecycle.STOPPED;
        }
    }

    /** Returns the bound port while running, including the assigned port when configured with 0. */
    public synchronized int boundPort() {
        if (lifecycle != Lifecycle.RUNNING || server == null) {
            throw new IllegalStateException("HTTP API server is not running");
        }
        return server.getAddress().getPort();
    }

    @Override
    public synchronized void close() {
        if (lifecycle == Lifecycle.RUNNING) {
            stopResources();
        }
        lifecycle = Lifecycle.STOPPED;
    }

    private void stopResources() {
        HttpServer runningServer = server;
        server = null;
        if (runningServer != null) {
            runningServer.stop(0);
        }
        shutdownExecutor();
    }

    private void shutdownExecutor() {
        ThreadPoolExecutor runningExecutor = executor;
        executor = null;
        if (runningExecutor == null) {
            return;
        }
        runningExecutor.shutdown();
        try {
            if (!runningExecutor.awaitTermination(EXECUTOR_SHUTDOWN_SECONDS, TimeUnit.SECONDS)) {
                runningExecutor.shutdownNow();
                if (!runningExecutor.awaitTermination(EXECUTOR_SHUTDOWN_SECONDS, TimeUnit.SECONDS)) {
                    LOGGER.warning("HTTP API executor did not terminate within the shutdown timeout");
                }
            }
        } catch (InterruptedException e) {
            runningExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private ThreadFactory namedThreadFactory() {
        AtomicInteger threadNumber = new AtomicInteger();
        return task -> {
            Thread thread = new Thread(task, "jobstream-api-" + threadNumber.incrementAndGet());
            thread.setDaemon(false);
            return thread;
        };
    }

    private final class RouteHandler implements HttpHandler {
        private final Route route;

        private RouteHandler(Route route) {
            this.route = route;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                String path = exchange.getRequestURI().getPath();
                if (route == Route.NOT_FOUND || !route.path.equals(path)) {
                    sendError(exchange, ApiException.notFound());
                    return;
                }
                if (!"GET".equals(exchange.getRequestMethod())) {
                    sendError(exchange, ApiException.methodNotAllowed());
                    return;
                }
                if (route == Route.LIVE) {
                    sendJson(exchange, 200, new HealthResponse("UP"));
                } else {
                    sendReadiness(exchange);
                }
            } catch (IOException e) {
                // The peer may close its connection before receiving a response.
                LOGGER.log(Level.FINE, "HTTP client disconnected before response completed");
                throw e;
            } catch (RuntimeException e) {
                LOGGER.log(Level.SEVERE, "Unexpected HTTP API handler failure ({0})",
                        e.getClass().getSimpleName());
                if (exchange.getResponseCode() == -1) {
                    sendError(exchange, ApiException.internal(e));
                }
            } finally {
                exchange.close();
            }
        }
    }

    private void sendReadiness(HttpExchange exchange) throws IOException {
        Exception failure = null;
        try {
            readinessCheck.check();
        } catch (Exception e) {
            failure = e;
        }
        if (failure == null) {
            sendJson(exchange, 200, new ReadinessResponse("UP", new Dependencies("UP")));
        } else {
            LOGGER.log(Level.WARNING, "Redis readiness check failed ({0})",
                    failure.getClass().getSimpleName());
            sendJson(exchange, 503, new ReadinessResponse("DOWN", new Dependencies("DOWN")));
        }
    }

    private void sendError(HttpExchange exchange, ApiException exception) throws IOException {
        sendJson(exchange, exception.httpStatus(), ErrorResponse.from(exception));
    }

    private void sendJson(HttpExchange exchange, int status, Object response) throws IOException {
        byte[] body;
        try {
            body = jsonCodec.encode(response).getBytes(StandardCharsets.UTF_8);
        } catch (RuntimeException e) {
            LOGGER.log(Level.SEVERE, "Failed to encode HTTP response ({0})",
                    e.getClass().getSimpleName());
            status = 500;
            body = "{\"error\":{\"code\":\"INTERNAL_ERROR\",\"message\":\"An internal error occurred\"}}"
                    .getBytes(StandardCharsets.UTF_8);
        }
        exchange.getResponseHeaders().set("Content-Type", JSON_CONTENT_TYPE);
        exchange.sendResponseHeaders(status, body.length);
        exchange.getResponseBody().write(body);
    }

    private enum Lifecycle { NEW, RUNNING, STOPPED }

    private enum Route {
        LIVE("/health/live"), READY("/health/ready"), NOT_FOUND("");

        private final String path;

        Route(String path) {
            this.path = path;
        }
    }

    private record HealthResponse(String status) { }
    private record Dependencies(String redis) { }
    private record ReadinessResponse(String status, Dependencies dependencies) { }
}
