package io.github.pandeyayushk.jobstream.executor;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class DefaultExecutorRegistryTest {

    @Test
    void registersAndRetrievesExecutor() {
        DefaultExecutorRegistry registry =
                new DefaultExecutorRegistry();

        JobExecutor executor =
                job -> ExecutionResult.success();

        registry.register("email:send", executor);

        assertSame(
                executor,
                registry.get("email:send").orElseThrow()
        );
    }

    @Test
    void returnsEmptyForUnknownJobType() {
        DefaultExecutorRegistry registry =
                new DefaultExecutorRegistry();

        assertTrue(
                registry.get("unknown").isEmpty()
        );
    }

    @Test
    void hasExecutorReturnsTrueForRegisteredExecutor() {
        DefaultExecutorRegistry registry =
                new DefaultExecutorRegistry();

        registry.register(
                "email:send",
                job -> ExecutionResult.success()
        );

        assertTrue(
                registry.hasExecutor("email:send")
        );
    }

    @Test
    void hasExecutorReturnsFalseForUnknownJobType() {
        DefaultExecutorRegistry registry =
                new DefaultExecutorRegistry();

        assertFalse(
                registry.hasExecutor("unknown")
        );
    }

    @Test
    void replacesExistingExecutorForSameJobType() {
        DefaultExecutorRegistry registry =
                new DefaultExecutorRegistry();

        JobExecutor firstExecutor =
                job -> ExecutionResult.success();

        JobExecutor secondExecutor =
                job -> ExecutionResult.failure("failed");

        registry.register(
                "email:send",
                firstExecutor
        );

        registry.register(
                "email:send",
                secondExecutor
        );

        assertSame(
                secondExecutor,
                registry.get("email:send").orElseThrow()
        );
    }

    @Test
    void rejectsNullJobTypeDuringRegistration() {
        DefaultExecutorRegistry registry =
                new DefaultExecutorRegistry();

        assertThrowsExactly(
                NullPointerException.class,
                () -> registry.register(
                        null,
                        job -> ExecutionResult.success()
                )
        );
    }

    @Test
    void rejectsBlankJobTypeDuringRegistration() {
        DefaultExecutorRegistry registry =
                new DefaultExecutorRegistry();

        assertThrowsExactly(
                IllegalArgumentException.class,
                () -> registry.register(
                        "   ",
                        job -> ExecutionResult.success()
                )
        );
    }

    @Test
    void rejectsNullExecutorDuringRegistration() {
        DefaultExecutorRegistry registry =
                new DefaultExecutorRegistry();

        assertThrowsExactly(
                NullPointerException.class,
                () -> registry.register(
                        "email:send",
                        null
                )
        );
    }

    @Test
    void rejectsNullJobTypeDuringLookup() {
        DefaultExecutorRegistry registry =
                new DefaultExecutorRegistry();

        assertThrowsExactly(
                NullPointerException.class,
                () -> registry.get(null)
        );
    }

    @Test
    void concurrentRegistrationAndLookupIsSafe() throws Exception {
        DefaultExecutorRegistry registry =
                new DefaultExecutorRegistry();

        int threadCount = 8;

        ExecutorService executorService =
                Executors.newFixedThreadPool(threadCount);

        CountDownLatch startLatch =
                new CountDownLatch(1);

        List<Runnable> tasks = new ArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            int index = i;

            tasks.add(() -> {
                try {
                    startLatch.await();

                    String jobType =
                            "job:type:" + index;

                    JobExecutor executor =
                            job -> ExecutionResult.success();

                    registry.register(
                            jobType,
                            executor
                    );

                    assertTrue(
                            registry.hasExecutor(jobType)
                    );

                    assertTrue(
                            registry.get(jobType).isPresent()
                    );

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    fail("Test thread was interrupted");
                }
            });
        }

        for (Runnable task : tasks) {
            executorService.submit(task);
        }

        startLatch.countDown();

        executorService.shutdown();

        assertTrue(
                executorService.awaitTermination(
                        5,
                        TimeUnit.SECONDS
                )
        );

        for (int i = 0; i < threadCount; i++) {
            assertTrue(
                    registry.hasExecutor(
                            "job:type:" + i
                    )
            );
        }
    }
}