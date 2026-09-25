package io.github.pandeyayushk.jobstream.worker;

import io.github.pandeyayushk.jobstream.job.Job;
import io.github.pandeyayushk.jobstream.job.JobId;
import io.github.pandeyayushk.jobstream.job.JobStatus;
import io.github.pandeyayushk.jobstream.payload.Payload;
import io.github.pandeyayushk.jobstream.persistence.JobRepository;
import io.github.pandeyayushk.jobstream.queue.JobQueue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class WorkerTest {

    private final List<Worker> workers = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (Worker worker : workers) {
            if (worker.status() != WorkerStatus.STOPPED) {
                worker.stop();
            }
        }
    }

    @Test
    void workerStartsAndReachesRunningState() {
        TestQueue queue = new TestQueue();
        TestRepository repository = new TestRepository();
        TestRegistry registry = new TestRegistry();

        Worker worker = createWorker(
                queue,
                repository,
                registry,
                job -> {
                },
                WorkerConfig.defaults()
        );

        worker.start();

        assertEquals(
                WorkerStatus.RUNNING,
                worker.status()
        );

        assertEquals(
                WorkerStatus.RUNNING,
                registry.lastRegisteredStatus()
        );
    }

    @Test
    void workerStopsAndDeregisters() {
        TestRegistry registry = new TestRegistry();

        Worker worker = createWorker(
                new TestQueue(),
                new TestRepository(),
                registry,
                job -> {
                },
                WorkerConfig.defaults()
        );

        worker.start();
        worker.stop();

        assertEquals(
                WorkerStatus.STOPPED,
                worker.status()
        );

        assertEquals(1, registry.deregisterCount());
    }

    @Test
    void workerProcessesQueuedJobAndCompletesIt() throws Exception {
        TestQueue queue = new TestQueue();
        TestRepository repository = new TestRepository();

        Job job = queuedJob();

        repository.save(job);
        queue.enqueue(job.id(), "default");

        CountDownLatch handled = new CountDownLatch(1);

        Worker worker = createWorker(
                queue,
                repository,
                new TestRegistry(),
                processedJob -> handled.countDown(),
                WorkerConfig.defaults()
        );

        worker.start();

        assertTrue(
                handled.await(3, TimeUnit.SECONDS)
        );

        assertEquals(
                JobStatus.COMPLETED,
                repository.findById(job.id())
                        .orElseThrow()
                        .status()
        );
    }

    @Test
    void workerChangesQueuedJobToProcessingBeforeHandler() throws Exception {
        TestQueue queue = new TestQueue();
        TestRepository repository = new TestRepository();

        Job job = queuedJob();

        repository.save(job);
        queue.enqueue(job.id(), "default");

        CountDownLatch handlerStarted = new CountDownLatch(1);
        AtomicInteger observedProcessingJobs = new AtomicInteger();

        Worker worker = createWorker(
                queue,
                repository,
                new TestRegistry(),
                processingJob -> {
                    if (processingJob.status() == JobStatus.PROCESSING) {
                        observedProcessingJobs.incrementAndGet();
                    }

                    handlerStarted.countDown();
                },
                WorkerConfig.defaults()
        );

        worker.start();

        assertTrue(
                handlerStarted.await(3, TimeUnit.SECONDS)
        );

        assertEquals(
                1,
                observedProcessingJobs.get()
        );
    }

    @Test
    void handlerFailureMarksJobAsFailed() throws Exception {
        TestQueue queue = new TestQueue();
        TestRepository repository = new TestRepository();

        Job job = queuedJob();

        repository.save(job);
        queue.enqueue(job.id(), "default");

        CountDownLatch handlerCalled = new CountDownLatch(1);

        Worker worker = createWorker(
                queue,
                repository,
                new TestRegistry(),
                processedJob -> {
                    handlerCalled.countDown();
                    throw new IllegalStateException(
                            "handler failed"
                    );
                },
                WorkerConfig.defaults()
        );

        worker.start();

        assertTrue(
                handlerCalled.await(3, TimeUnit.SECONDS)
        );

        waitForStatus(
                repository,
                job.id(),
                JobStatus.FAILED
        );

        assertEquals(
                JobStatus.FAILED,
                repository.findById(job.id())
                        .orElseThrow()
                        .status()
        );

        assertEquals(
                WorkerStatus.RUNNING,
                worker.status()
        );
    }

    @Test
    void handlerFailureDoesNotStopWorker() throws Exception {
        TestQueue queue = new TestQueue();
        TestRepository repository = new TestRepository();

        Job firstJob = queuedJob();
        Job secondJob = queuedJob();

        repository.save(firstJob);
        repository.save(secondJob);

        queue.enqueue(firstJob.id(), "default");
        queue.enqueue(secondJob.id(), "default");

        CountDownLatch secondJobHandled =
                new CountDownLatch(1);

        Worker worker = createWorker(
                queue,
                repository,
                new TestRegistry(),
                processedJob -> {
                    if (processedJob.id().equals(firstJob.id())) {
                        throw new IllegalStateException(
                                "first job failed"
                        );
                    }

                    secondJobHandled.countDown();
                },
                WorkerConfig.defaults()
        );

        worker.start();

        assertTrue(
                secondJobHandled.await(
                        3,
                        TimeUnit.SECONDS
                )
        );

        assertEquals(
                JobStatus.FAILED,
                repository.findById(firstJob.id())
                        .orElseThrow()
                        .status()
        );

        assertEquals(
                JobStatus.COMPLETED,
                repository.findById(secondJob.id())
                        .orElseThrow()
                        .status()
        );

        assertEquals(
                WorkerStatus.RUNNING,
                worker.status()
        );
    }

    @Test
    void orphanJobIdDoesNotStopWorker() throws Exception {
        TestQueue queue = new TestQueue();
        TestRepository repository = new TestRepository();
        TestRegistry registry = new TestRegistry();

        JobId orphanId = JobId.generate();

        queue.enqueue(
                orphanId,
                "default"
        );

        Worker worker = createWorker(
                queue,
                repository,
                registry,
                job -> fail("Handler should not be called"),
                WorkerConfig.defaults()
        );

        worker.start();

        Thread.sleep(200);

        assertEquals(
                WorkerStatus.RUNNING,
                worker.status()
        );
    }

    @Test
    void workerSendsHeartbeat() throws Exception {
        TestRegistry registry = new TestRegistry();

        WorkerConfig config = new WorkerConfig(
                "default",
                1,
                Duration.ofMillis(100),
                Duration.ofSeconds(1),
                Duration.ofSeconds(2)
        );

        Worker worker = createWorker(
                new TestQueue(),
                new TestRepository(),
                registry,
                job -> {
                },
                config
        );

        worker.start();

        assertTrue(
                registry.heartbeatLatch.await(
                        2,
                        TimeUnit.SECONDS
                )
        );
    }

    @Test
    void workerRespectsConfiguredConcurrency()
            throws Exception {

        TestQueue queue = new TestQueue();
        TestRepository repository = new TestRepository();

        int concurrency = 2;

        WorkerConfig config = new WorkerConfig(
                "default",
                concurrency,
                Duration.ofSeconds(10),
                Duration.ofSeconds(30),
                Duration.ofSeconds(2)
        );

        List<Job> jobs = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            Job job = queuedJob();

            jobs.add(job);
            repository.save(job);
            queue.enqueue(job.id(), "default");
        }

        AtomicInteger running =
                new AtomicInteger();

        AtomicInteger maximumRunning =
                new AtomicInteger();

        CountDownLatch firstTwoStarted =
                new CountDownLatch(2);

        CountDownLatch releaseJobs =
                new CountDownLatch(1);

        Worker worker = createWorker(
                queue,
                repository,
                new TestRegistry(),
                job -> {
                    int current =
                            running.incrementAndGet();

                    maximumRunning.accumulateAndGet(
                            current,
                            Math::max
                    );

                    firstTwoStarted.countDown();

                    try {
                        releaseJobs.await(
                                3,
                                TimeUnit.SECONDS
                        );
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        running.decrementAndGet();
                    }
                },
                config
        );

        worker.start();

        assertTrue(
                firstTwoStarted.await(
                        3,
                        TimeUnit.SECONDS
                )
        );

        Thread.sleep(200);

        assertEquals(
                2,
                maximumRunning.get()
        );

        releaseJobs.countDown();
    }

    @Test
    void stopPreventsNewJobsFromBeingAcquired()
            throws Exception {

        TestQueue queue = new TestQueue();
        TestRepository repository = new TestRepository();

        CountDownLatch handlerStarted =
                new CountDownLatch(1);

        CountDownLatch releaseHandler =
                new CountDownLatch(1);

        AtomicInteger handled =
                new AtomicInteger();

        Job firstJob = queuedJob();
        Job secondJob = queuedJob();

        repository.save(firstJob);
        repository.save(secondJob);

        queue.enqueue(firstJob.id(), "default");
        queue.enqueue(secondJob.id(), "default");

        Worker worker = createWorker(
                queue,
                repository,
                new TestRegistry(),
                job -> {
                    handled.incrementAndGet();
                    handlerStarted.countDown();

                    try {
                        releaseHandler.await(
                                3,
                                TimeUnit.SECONDS
                        );
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                },
                new WorkerConfig(
                        "default",
                        1,
                        Duration.ofSeconds(10),
                        Duration.ofSeconds(30),
                        Duration.ofSeconds(2)
                )
        );

        worker.start();

        assertTrue(
                handlerStarted.await(
                        3,
                        TimeUnit.SECONDS
                )
        );

        Thread stopThread =
                new Thread(worker::stop);

        stopThread.start();

        Thread.sleep(100);

        assertTrue(
                worker.status() == WorkerStatus.STOPPING
                        || worker.status() == WorkerStatus.STOPPED
        );

        releaseHandler.countDown();

        stopThread.join(3000);

        assertEquals(
                WorkerStatus.STOPPED,
                worker.status()
        );

        assertEquals(
                1,
                handled.get()
        );
    }

    @Test
    void stopWaitsForInFlightJobToFinish()
            throws Exception {

        TestQueue queue = new TestQueue();
        TestRepository repository = new TestRepository();

        Job job = queuedJob();

        repository.save(job);
        queue.enqueue(job.id(), "default");

        CountDownLatch handlerStarted =
                new CountDownLatch(1);

        CountDownLatch releaseHandler =
                new CountDownLatch(1);

        Worker worker = createWorker(
                queue,
                repository,
                new TestRegistry(),
                processedJob -> {
                    handlerStarted.countDown();

                    try {
                        releaseHandler.await(
                                3,
                                TimeUnit.SECONDS
                        );
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                },
                new WorkerConfig(
                        "default",
                        1,
                        Duration.ofSeconds(10),
                        Duration.ofSeconds(30),
                        Duration.ofSeconds(3)
                )
        );

        worker.start();

        assertTrue(
                handlerStarted.await(
                        3,
                        TimeUnit.SECONDS
                )
        );

        Thread stopThread =
                new Thread(worker::stop);

        stopThread.start();

        Thread.sleep(100);

        assertTrue(stopThread.isAlive());

        releaseHandler.countDown();

        stopThread.join(3000);

        assertEquals(
                WorkerStatus.STOPPED,
                worker.status()
        );

        assertEquals(
                JobStatus.COMPLETED,
                repository.findById(job.id())
                        .orElseThrow()
                        .status()
        );
    }

    @Test
    void startingWorkerTwiceIsRejected() {
        Worker worker = createWorker(
                new TestQueue(),
                new TestRepository(),
                new TestRegistry(),
                job -> {
                },
                WorkerConfig.defaults()
        );

        worker.start();

        assertThrowsExactly(
                WorkerException.class,
                worker::start
        );
    }

    @Test
    void stoppingStoppedWorkerIsSafe() {
        Worker worker = createWorker(
                new TestQueue(),
                new TestRepository(),
                new TestRegistry(),
                job -> {
                },
                WorkerConfig.defaults()
        );

        assertDoesNotThrow(worker::stop);

        assertEquals(
                WorkerStatus.STOPPED,
                worker.status()
        );
    }

    private Worker createWorker(
            JobQueue queue,
            JobRepository repository,
            WorkerRegistry registry,
            WorkerJobHandler handler,
            WorkerConfig config
    ) {
        Worker worker = new Worker(
                WorkerId.generate(),
                queue,
                repository,
                registry,
                handler,
                config
        );

        workers.add(worker);

        return worker;
    }

    private Job queuedJob() {
        Job job = Job.create(
                "test-job",
                Payload.empty()
        );

        return job.withStatus(
                JobStatus.QUEUED
        );
    }

    private void waitForStatus(
            TestRepository repository,
            JobId jobId,
            JobStatus expectedStatus
    ) throws InterruptedException {

        long deadline =
                System.currentTimeMillis() + 3000;

        while (System.currentTimeMillis() < deadline) {

            Optional<Job> job =
                    repository.findById(jobId);

            if (job.isPresent()
                    && job.get().status() == expectedStatus) {
                return;
            }

            Thread.sleep(10);
        }

        fail(
                "Job did not reach status "
                        + expectedStatus
        );
    }

    private static class TestQueue
            implements JobQueue {

        private final ConcurrentLinkedQueue<JobId> jobs =
                new ConcurrentLinkedQueue<>();

        @Override
        public void enqueue(
                JobId jobId,
                String queueName
        ) {
            jobs.add(jobId);
        }

        @Override
        public Optional<JobId> dequeue(
                String queueName,
                Duration timeout
        ) {
            return Optional.ofNullable(
                    jobs.poll()
            );
        }

        @Override
        public Optional<JobId> dequeueNonBlocking(
                String queueName
        ) {
            return Optional.ofNullable(
                    jobs.poll()
            );
        }

        @Override
        public long size(String queueName) {
            return jobs.size();
        }

        @Override
        public List<JobId> peek(
                String queueName,
                int count
        ) {
            return jobs.stream()
                    .limit(count)
                    .toList();
        }
    }

    private static class TestRepository
            implements JobRepository {

        private final Map<JobId, Job> jobs =
                new ConcurrentHashMap<>();

        @Override
        public void save(Job job) {
            jobs.put(job.id(), job);
        }

        @Override
        public Optional<Job> findById(JobId id) {
            return Optional.ofNullable(
                    jobs.get(id)
            );
        }

        @Override
        public boolean delete(JobId id) {
            return jobs.remove(id) != null;
        }

        @Override
        public List<Job> findByStatus(
                JobStatus status
        ) {
            return jobs.values()
                    .stream()
                    .filter(job ->
                            job.status() == status)
                    .toList();
        }

        @Override
        public boolean exists(JobId id) {
            return jobs.containsKey(id);
        }
    }

    private static class TestRegistry
            implements WorkerRegistry {

        private final List<WorkerInfo> registrations =
                new ArrayList<>();

        private final CountDownLatch heartbeatLatch =
                new CountDownLatch(1);

        private int deregisterCount;

        @Override
        public synchronized void register(
                WorkerInfo info
        ) {
            registrations.add(info);
        }

        @Override
        public void heartbeat(
                WorkerId workerId
        ) {
            heartbeatLatch.countDown();
        }

        @Override
        public synchronized void deregister(
                WorkerId workerId
        ) {
            deregisterCount++;
        }

        @Override
        public List<WorkerInfo> listActiveWorkers() {
            return List.of();
        }

        @Override
        public Optional<WorkerInfo> getWorker(
                WorkerId workerId
        ) {
            return registrations.stream()
                    .filter(info ->
                            info.workerId().equals(workerId))
                    .reduce((first, second) -> second);
        }

        synchronized WorkerStatus lastRegisteredStatus() {
            if (registrations.isEmpty()) {
                return null;
            }

            return registrations.get(
                    registrations.size() - 1
            ).status();
        }

        synchronized int deregisterCount() {
            return deregisterCount;
        }
    }


}