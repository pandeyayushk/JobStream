package io.github.pandeyayushk.jobstream.queue;

import io.github.pandeyayushk.jobstream.job.Job;
import io.github.pandeyayushk.jobstream.job.JobStatus;
import io.github.pandeyayushk.jobstream.metrics.JobMetrics;
import io.github.pandeyayushk.jobstream.metrics.ProcessLocalJobMetrics;
import io.github.pandeyayushk.jobstream.payload.Payload;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class QueueCoordinatorImpTest {

    @Test
    public void submitTransitionsJobToQueued() {
        RecordingSubmissionStore store = new RecordingSubmissionStore();
        QueueCoordinatorImp coordinator = new QueueCoordinatorImp(store);

        Job originalJob = Job.create(
                "email",
                Payload.empty()
        );

        Job persisted = coordinator.submit(originalJob, "default");

        assertNotNull(store.originalJob);
        assertNotNull(store.queuedJob);

        assertEquals(
                JobStatus.PENDING,
                store.originalJob.status()
        );

        assertEquals(
                JobStatus.QUEUED,
                store.queuedJob.status()
        );

        assertEquals("default", store.queueName);
        assertEquals(JobStatus.QUEUED, persisted.status());
        assertEquals(1, persisted.revision());
        assertEquals(JobStatus.PENDING, originalJob.status());
        assertEquals(0, originalJob.revision());
    }

    @Test
    public void submitPassesSameJobIdToSubmissionStore() {
        RecordingSubmissionStore store = new RecordingSubmissionStore();
        QueueCoordinatorImp coordinator = new QueueCoordinatorImp(store);

        Job originalJob = Job.create(
                "email",
                Payload.empty()
        );

        coordinator.submit(originalJob, "default");

        assertEquals(
                originalJob.id(),
                store.originalJob.id()
        );

        assertEquals(
                originalJob.id(),
                store.queuedJob.id()
        );
    }

    @Test
    public void successfulSubmissionRecordsOnceAfterReturningPersistedSnapshot() {
        RecordingSubmissionStore store = new RecordingSubmissionStore();
        ProcessLocalJobMetrics metrics = new ProcessLocalJobMetrics();
        QueueCoordinatorImp coordinator = new QueueCoordinatorImp(store, metrics);

        Job persisted = coordinator.submit(Job.create("email", Payload.empty()), "default");

        assertEquals(JobStatus.QUEUED, persisted.status());
        assertEquals(1, metrics.snapshot().jobsSubmittedTotal());
    }

    @Test
    public void failedSubmissionAndFaultyMetricsDoNotChangeSubmissionOutcome() {
        JobMetrics faultyMetrics = new JobMetrics() {
            @Override public void recordJobSubmitted() { throw new IllegalStateException("metrics failed"); }
            @Override public void recordJobCompleted() { }
            @Override public void recordJobFailed() { }
            @Override public void recordJobRetried() { }
            @Override public void recordJobMovedToDlq() { }
            @Override public io.github.pandeyayushk.jobstream.metrics.JobMetricsSnapshot snapshot() {
                return new io.github.pandeyayushk.jobstream.metrics.JobMetricsSnapshot(0, 0, 0, 0, 0);
            }
        };
        QueueCoordinatorImp successful = new QueueCoordinatorImp(new RecordingSubmissionStore(), faultyMetrics);
        assertDoesNotThrow(() -> successful.submit(Job.create("email", Payload.empty()), "default"));

        JobSubmissionStore failingStore = (original, queued, queue) -> {
            throw new QueueException("persistence failed");
        };
        ProcessLocalJobMetrics metrics = new ProcessLocalJobMetrics();
        QueueCoordinatorImp failing = new QueueCoordinatorImp(failingStore, metrics);
        assertThrows(QueueException.class,
                () -> failing.submit(Job.create("email", Payload.empty()), "default"));
        assertEquals(0, metrics.snapshot().jobsSubmittedTotal());
    }

    @Test
    public void submitRejectsNullJob() {
        RecordingSubmissionStore store = new RecordingSubmissionStore();
        QueueCoordinatorImp coordinator = new QueueCoordinatorImp(store);

        assertThrows(
                NullPointerException.class,
                () -> coordinator.submit(null, "default")
        );
    }

    @Test
    public void submitRejectsNullQueueName() {
        RecordingSubmissionStore store = new RecordingSubmissionStore();
        QueueCoordinatorImp coordinator = new QueueCoordinatorImp(store);

        Job job = Job.create(
                "email",
                Payload.empty()
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> coordinator.submit(job, null)
        );
    }

    @Test
    public void submitRejectsBlankQueueName() {
        RecordingSubmissionStore store = new RecordingSubmissionStore();
        QueueCoordinatorImp coordinator = new QueueCoordinatorImp(store);

        Job job = Job.create(
                "email",
                Payload.empty()
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> coordinator.submit(job, "   ")
        );
    }

    @Test
    public void submitPropagatesInvalidJobTransition() {
        RecordingSubmissionStore store = new RecordingSubmissionStore();
        QueueCoordinatorImp coordinator = new QueueCoordinatorImp(store);

        Job pendingJob = Job.create(
                "email",
                Payload.empty()
        );

        Job queuedJob = pendingJob.withStatus(JobStatus.QUEUED);
        Job processingJob = queuedJob.withStatus(JobStatus.PROCESSING);

        assertThrows(
                IllegalStateException.class,
                () -> coordinator.submit(processingJob, "default")
        );

        assertNull(store.originalJob);
        assertNull(store.queuedJob);
    }

    private static class RecordingSubmissionStore
            implements JobSubmissionStore {

        private Job originalJob;
        private Job queuedJob;
        private String queueName;

        @Override
        public Job submit(
                Job originalJob,
                Job queuedJob,
                String queueName
        ) {
            this.originalJob = originalJob;
            this.queuedJob = queuedJob;
            this.queueName = queueName;
            return queuedJob.withRevision(originalJob.revision() + 1);
        }
    }
}
