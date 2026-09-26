package io.github.pandeyayushk.jobstream.worker;

import io.github.pandeyayushk.jobstream.executor.ExecutionResult;
import io.github.pandeyayushk.jobstream.executor.ExecutorRegistry;
import io.github.pandeyayushk.jobstream.executor.JobExecutor;
import io.github.pandeyayushk.jobstream.job.Job;
import io.github.pandeyayushk.jobstream.job.JobId;
import io.github.pandeyayushk.jobstream.job.JobStatus;
import io.github.pandeyayushk.jobstream.persistence.JobRepository;
import io.github.pandeyayushk.jobstream.queue.JobQueue;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class Worker {

    private static final Duration DEQUEUE_TIMEOUT =
            Duration.ofMillis(500);

    private final WorkerId workerId;
    private final JobQueue queue;
    private final JobRepository jobRepository;
    private final WorkerRegistry workerRegistry;
    private final ExecutorRegistry executorRegistry;
    private final WorkerConfig config;

    private volatile WorkerStatus status;

    private ExecutorService processingExecutor;
    private ExecutorService acquisitionExecutor;
    private ScheduledExecutorService heartbeatExecutor;
    private Semaphore processingPermits;

    private Instant startedAt;

    public Worker(
            WorkerId workerId,
            JobQueue queue,
            JobRepository jobRepository,
            WorkerRegistry workerRegistry,
            ExecutorRegistry executorRegistry,
            WorkerConfig config
    ) {
        this.workerId = Objects.requireNonNull(
                workerId,
                "WorkerId cannot be null"
        );
        this.queue = Objects.requireNonNull(
                queue,
                "JobQueue cannot be null"
        );
        this.jobRepository = Objects.requireNonNull(
                jobRepository,
                "JobRepository cannot be null"
        );
        this.workerRegistry = Objects.requireNonNull(
                workerRegistry,
                "WorkerRegistry cannot be null"
        );
        this.executorRegistry = Objects.requireNonNull(
                executorRegistry,
                "ExecutorRegistry cannot be null"
        );
        this.config = Objects.requireNonNull(
                config,
                "WorkerConfig cannot be null"
        );
        this.status = WorkerStatus.STOPPED;
    }

    public WorkerStatus status() {
        return status;
    }

    public synchronized void start() {
        if (status != WorkerStatus.STOPPED) {
            throw new WorkerException(
                    "Worker cannot start from status " + status
            );
        }

        startedAt = Instant.now();
        status = WorkerStatus.STARTING;

        WorkerInfo startingInfo =
                new WorkerInfo(
                        workerId,
                        WorkerStatus.STARTING,
                        startedAt
                );

        try {
            workerRegistry.register(startingInfo);

            processingExecutor =
                    Executors.newFixedThreadPool(
                            config.concurrency()
                    );

            acquisitionExecutor =
                    Executors.newSingleThreadExecutor();

            heartbeatExecutor =
                    Executors.newSingleThreadScheduledExecutor();

            processingPermits =
                    new Semaphore(config.concurrency());

            heartbeatExecutor.scheduleAtFixedRate(
                    this::sendHeartbeat,
                    config.heartbeatInterval().toMillis(),
                    config.heartbeatInterval().toMillis(),
                    TimeUnit.MILLISECONDS
            );

            status = WorkerStatus.RUNNING;

            workerRegistry.register(
                    new WorkerInfo(
                            workerId,
                            WorkerStatus.RUNNING,
                            startedAt
                    )
            );

            acquisitionExecutor.submit(
                    this::acquisitionLoop
            );

        } catch (RuntimeException e) {
            cleanupAfterFailedStart();
            status = WorkerStatus.STOPPED;

            throw new WorkerException(
                    "Failed to start worker",
                    e
            );
        }
    }

    public synchronized void stop() {
        if (status == WorkerStatus.STOPPED) {
            return;
        }

        if (status != WorkerStatus.RUNNING) {
            return;
        }

        status = WorkerStatus.STOPPING;

        /*
         * First stop acquisition.
         *
         * The acquisition thread must finish before the processing
         * executor is shut down. Otherwise it could dequeue a job
         * and then fail to submit it for processing.
         */
        if (acquisitionExecutor != null) {
            acquisitionExecutor.shutdownNow();

            try {
                if (!acquisitionExecutor.awaitTermination(
                        config.shutdownTimeout().toMillis(),
                        TimeUnit.MILLISECONDS
                )) {
                    acquisitionExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                acquisitionExecutor.shutdownNow();
            }
        }

        /*
         * Acquisition has stopped, so now allow already-running jobs
         * to finish.
         */
        if (processingExecutor != null) {
            processingExecutor.shutdown();

            try {
                if (!processingExecutor.awaitTermination(
                        config.shutdownTimeout().toMillis(),
                        TimeUnit.MILLISECONDS
                )) {
                    processingExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                processingExecutor.shutdownNow();
            }
        }

        if (heartbeatExecutor != null) {
            heartbeatExecutor.shutdownNow();
        }

        try {
            workerRegistry.deregister(workerId);
        } catch (WorkerException e) {
            status = WorkerStatus.STOPPED;
            throw e;
        }

        status = WorkerStatus.STOPPED;
    }

    private void acquisitionLoop() {
        while (status == WorkerStatus.RUNNING) {

            boolean permitAcquired = false;

            try {
                processingPermits.acquire();
                permitAcquired = true;

                if (status != WorkerStatus.RUNNING) {
                    processingPermits.release();
                    break;
                }

                Optional<JobId> jobId =
                        queue.dequeue(
                                config.queueName(),
                                DEQUEUE_TIMEOUT
                        );

                if (jobId.isEmpty()) {
                    processingPermits.release();
                    continue;
                }

                /*
                 * Shutdown may have started while dequeue was blocking.
                 *
                 * Do not lose the dequeued job. Put it back into the
                 * queue because it has not been handed to a processor.
                 */
                if (status != WorkerStatus.RUNNING) {
                    queue.enqueue(
                            jobId.get(),
                            config.queueName()
                    );

                    processingPermits.release();
                    break;
                }

                try {
                    processingExecutor.submit(
                            () -> processJob(jobId.get())
                    );

                    permitAcquired = false;

                } catch (RejectedExecutionException e) {
                    /*
                     * The job was removed from the queue but the
                     * processing executor rejected it. Return the job
                     * to the queue so it is not lost during shutdown.
                     */
                    queue.enqueue(
                            jobId.get(),
                            config.queueName()
                    );

                    processingPermits.release();

                    if (status != WorkerStatus.RUNNING) {
                        break;
                    }
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                if (permitAcquired) {
                    processingPermits.release();
                }

                break;

            } catch (RuntimeException e) {

                if (permitAcquired) {
                    processingPermits.release();
                }

                if (status != WorkerStatus.RUNNING) {
                    break;
                }
            }
        }
    }

    private void processJob(JobId jobId) {
        try {
            Optional<Job> optionalJob =
                    jobRepository.findById(jobId);

            /*
             * Orphaned JobId.
             *
             * The queue contained a JobId that does not exist
             * in the authoritative repository. Do not allow this
             * to terminate the worker.
             */
            if (optionalJob.isEmpty()) {
                return;
            }

            Job job = optionalJob.get();

            /*
             * The queue should contain QUEUED jobs.
             * If the repository says otherwise, do not execute it.
             */
            if (job.status() != JobStatus.QUEUED) {
                return;
            }

            Job processingJob =
                    job.withStatus(JobStatus.PROCESSING);

            jobRepository.save(processingJob);

            JobExecutor executor =
                    executorRegistry.get(processingJob.type())
                            .orElse(null);

            if (executor == null) {
                markFailed(
                        processingJob,
                        "No executor registered for job type: "
                                + processingJob.type()
                );
                return;
            }

            ExecutionResult result;

            try {
                result = executor.execute(processingJob);

            } catch (Throwable t) {
                markFailed(
                        processingJob,
                        executionFailureMessage(t)
                );
                return;
            }

            if (result == null) {
                markFailed(
                        processingJob,
                        "Executor returned null ExecutionResult"
                );
                return;
            }

            if (result.isSuccess()) {
                Job completedJob =
                        processingJob.withStatus(
                                JobStatus.COMPLETED
                        );

                jobRepository.save(completedJob);
                return;
            }

            markFailed(
                    processingJob,
                    result.getErrorMessage()
                            .orElse("Job execution failed")
            );

        } catch (RuntimeException e) {
            /*
             * A malformed/orphaned job or repository/runtime failure
             * must not terminate the worker processing thread.
             */
        } finally {
            processingPermits.release();
        }
    }

    private void markFailed(Job job, String reason) {
        Job failedJob = job
                .withStatus(JobStatus.FAILED)
                .withMetadata("failure.reason", reason);

        jobRepository.save(failedJob);
    }

    private String executionFailureMessage(
            Throwable throwable
    ) {
        String message = throwable.getMessage();

        if (message == null || message.isBlank()) {
            return throwable.getClass().getName();
        }

        return message;
    }

    private void sendHeartbeat() {
        if (status != WorkerStatus.RUNNING) {
            return;
        }

        try {
            workerRegistry.heartbeat(workerId);
        } catch (WorkerException e) {
            /*
             * Heartbeat failure must not terminate the worker.
             */
        }
    }

    private void cleanupAfterFailedStart() {
        if (acquisitionExecutor != null) {
            acquisitionExecutor.shutdownNow();
        }

        if (processingExecutor != null) {
            processingExecutor.shutdownNow();
        }

        if (heartbeatExecutor != null) {
            heartbeatExecutor.shutdownNow();
        }

        try {
            workerRegistry.deregister(workerId);
        } catch (WorkerException ignored) {
            /*
             * Best-effort cleanup.
             */
        }
    }
}