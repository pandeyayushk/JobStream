package io.github.pandeyayushk.jobstream.worker;

import io.github.pandeyayushk.jobstream.executor.ExecutionResult;
import io.github.pandeyayushk.jobstream.executor.ExecutorRegistry;
import io.github.pandeyayushk.jobstream.executor.JobExecutor;
import io.github.pandeyayushk.jobstream.job.Job;
import io.github.pandeyayushk.jobstream.job.JobId;
import io.github.pandeyayushk.jobstream.job.JobStatus;
import io.github.pandeyayushk.jobstream.persistence.JobRepository;
import io.github.pandeyayushk.jobstream.queue.JobQueue;
import io.github.pandeyayushk.jobstream.retry.DeadLetterQueue;
import io.github.pandeyayushk.jobstream.retry.RetryPolicy;

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


    private final WorkerId workerId;
    private final JobQueue queue;
    private final JobRepository jobRepository;
    private final WorkerRegistry workerRegistry;
    private final ExecutorRegistry executorRegistry;
    private final WorkerConfig config;
    private final RetryPolicy retryPolicy;
    private final DeadLetterQueue deadLetterQueue;

    private volatile WorkerStatus status;

    private ExecutorService processingExecutor;
    private ExecutorService acquisitionExecutor;
    private ScheduledExecutorService heartbeatExecutor;
    private ScheduledExecutorService retryExecutor;

    private Semaphore processingPermits;

    private Instant startedAt;

    public Worker(
            WorkerId workerId,
            JobQueue queue,
            JobRepository jobRepository,
            WorkerRegistry workerRegistry,
            ExecutorRegistry executorRegistry,
            RetryPolicy retryPolicy,
            DeadLetterQueue deadLetterQueue,
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

        this.retryPolicy = Objects.requireNonNull(
                retryPolicy,
                "RetryPolicy cannot be null"
        );

        this.deadLetterQueue = Objects.requireNonNull(
                deadLetterQueue,
                "DeadLetterQueue cannot be null"
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

            retryExecutor =
                    Executors.newScheduledThreadPool(
                            Math.max(1, config.concurrency())
                    );

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

        /*
         * Retry scheduling is independent from processing.
         *
         * Give already-scheduled retry tasks an opportunity to finish
         * within the normal shutdown window. If they cannot finish,
         * cancel them rather than keeping the worker alive indefinitely.
         */
        if (retryExecutor != null) {
            retryExecutor.shutdown();

            try {
                if (!retryExecutor.awaitTermination(
                        config.shutdownTimeout().toMillis(),
                        TimeUnit.MILLISECONDS
                )) {
                    retryExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                retryExecutor.shutdownNow();
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
                        queue.dequeueNonBlocking(
                                config.queueName()
                        );

                if (jobId.isEmpty()) {
                    processingPermits.release();

                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }

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
                String reason =
                        "No executor registered for job type: "
                                + processingJob.type();

                handleFailure(
                        processingJob,
                        reason,
                        new IllegalStateException(reason)
                );

                return;
            }

            ExecutionResult result;

            try {
                result = executor.execute(processingJob);

            } catch (Throwable t) {
                handleFailure(
                        processingJob,
                        executionFailureMessage(t),
                        t
                );

                return;
            }

            if (result == null) {
                String reason =
                        "Executor returned null ExecutionResult";

                handleFailure(
                        processingJob,
                        reason,
                        new IllegalStateException(reason)
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

            String reason =
                    result.getErrorMessage()
                            .orElse("Job execution failed");

            Throwable cause =
                    result.getErrorCause()
                            .orElseGet(
                                    () -> new RuntimeException(reason)
                            );

            handleFailure(
                    processingJob,
                    reason,
                    cause
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

    private Job markFailed(Job job, String reason) {
        Job failedJob = job
                .withStatus(JobStatus.FAILED)
                .withMetadata("failure.reason", reason);

        jobRepository.save(failedJob);

        return failedJob;
    }

    private void handleFailure(
            Job job,
            String reason,
            Throwable cause
    ) {
        Job failedJob = markFailed(job, reason);

        boolean shouldRetry;

        try {
            shouldRetry =
                    retryPolicy.shouldRetry(
                            failedJob,
                            cause
                    );
        } catch (RuntimeException e) {
            /*
             * A broken retry policy must not leave the job in an
             * ambiguous retry state. Route the failure to the DLQ.
             */
            deadLetterQueue.moveToDeadLetter(
                    failedJob,
                    "Retry policy evaluation failed: "
                            + executionFailureMessage(e)
            );

            return;
        }

        if (!shouldRetry
                || failedJob.retryCount() >= failedJob.maxRetries()) {

            deadLetterQueue.moveToDeadLetter(
                    failedJob,
                    reason
            );

            return;
        }

        Job retryingJob =
                failedJob.withRetryAttempt(reason);

        jobRepository.save(retryingJob);

        Duration backoff;

        try {
            backoff =
                    Objects.requireNonNull(
                            retryPolicy.computeBackoff(
                                    retryingJob
                            ),
                            "RetryPolicy returned null backoff"
                    );

            if (backoff.isNegative()) {
                throw new IllegalArgumentException(
                        "RetryPolicy returned negative backoff"
                );
            }

        } catch (RuntimeException e) {
            /*
             * The retry decision succeeded, but the policy could not
             * calculate a valid delay. Do not leave the job stranded
             * in RETRYING. Quarantine it instead.
             */
            deadLetterQueue.moveToDeadLetter(
                    retryingJob.withStatus(JobStatus.FAILED),
                    "Retry backoff calculation failed: "
                            + executionFailureMessage(e)
            );

            return;
        }

        scheduleRetry(
                retryingJob.id(),
                backoff
        );
    }

    private void scheduleRetry(
            JobId jobId,
            Duration backoff
    ) {
        if (retryExecutor == null
                || retryExecutor.isShutdown()) {

            /*
             * The worker is shutting down. Avoid losing the job.
             * Put it back immediately rather than leaving it in
             * RETRYING forever.
             */
            requeueRetry(jobId);
            return;
        }

        try {
            retryExecutor.schedule(
                    () -> requeueRetry(jobId),
                    backoff.toMillis(),
                    TimeUnit.MILLISECONDS
            );

        } catch (RejectedExecutionException e) {
            /*
             * The scheduler shut down between the check above and
             * schedule(). Safely fall back to immediate requeue.
             */
            requeueRetry(jobId);
        }
    }

    private void requeueRetry(JobId jobId) {
        try {
            Optional<Job> optionalJob =
                    jobRepository.findById(jobId);

            if (optionalJob.isEmpty()) {
                return;
            }

            Job retryingJob = optionalJob.get();

            /*
             * A retry task should only requeue a job that is still
             * waiting in RETRYING state.
             */
            if (retryingJob.status() != JobStatus.RETRYING) {
                return;
            }

            Job queuedJob =
                    retryingJob.withStatus(
                            JobStatus.QUEUED
                    );

            jobRepository.save(queuedJob);

            queue.enqueue(
                    queuedJob.id(),
                    config.queueName()
            );

        } catch (RuntimeException e) {
            /*
             * Do not allow one failed retry scheduling operation to
             * terminate the scheduler thread.
             */
        }
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

        if (retryExecutor != null) {
            retryExecutor.shutdownNow();
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