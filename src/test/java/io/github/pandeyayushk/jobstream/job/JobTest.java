package io.github.pandeyayushk.jobstream.job;

import io.github.pandeyayushk.jobstream.payload.Payload;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class JobTest {


    @Test
    public void createInitializesJob(){
        Instant beforeCreation=Instant.now();
        Payload payload=Payload.empty();
        Job job=Job.create("email:send",payload);
        Instant afterCreation=Instant.now();
        assertEquals(JobStatus.PENDING,job.status());
        assertFalse(job.createdAt().isBefore(beforeCreation));
        assertFalse(job.createdAt().isAfter(afterCreation));
        assertEquals(job.createdAt(), job.updatedAt());
        assertEquals("email:send",job.type());
        assertEquals(payload,job.payload());
        assertTrue(job.metadata().isEmpty());
    }

    @Test
    public void blankTypeThrowsException(){
        Payload payload=Payload.empty();
        assertThrowsExactly(IllegalArgumentException.class,()->
                Job.create("",payload)
        );
        assertThrowsExactly(IllegalArgumentException.class,()->
                Job.create("     ",payload)
        );
    }

    @Test
    public void nullPayloadThrowsException(){
        assertThrowsExactly(NullPointerException.class,()->
                Job.create("email:send", null)
        );
    }

    @Test
    public void withStatusReturnsNewJob(){
        Payload payload=Payload.empty();
        Job original = Job.create("email:send",payload);
        Instant originalUpdatedAt = original.updatedAt();
        Job newJob = original.withStatus(JobStatus.QUEUED);

        assertEquals(JobStatus.PENDING,original.status());
        assertEquals(JobStatus.QUEUED,newJob.status());
        assertEquals(original.id(),newJob.id());
        assertEquals(original.type(),newJob.type());
        assertEquals(original.payload(),newJob.payload());
        assertEquals(original.createdAt(),newJob.createdAt());
        assertTrue(newJob.updatedAt().compareTo(originalUpdatedAt)>=0);
        assertNotSame(original, newJob);
    }

    @Test
    public  void illegalStatusTransitionThrowsException(){
        JobId id= JobId.generate();
        JobStatus status=JobStatus.COMPLETED;
        Payload payload= Payload.empty();
        Instant now=Instant.now();
        Map<String,String> metadata=Map.of();
        Job completedJob=Job.reconstitute(id,"email:send",status,payload,now,now,metadata);

        assertThrowsExactly(IllegalStateException.class,()->
                completedJob.withStatus(JobStatus.QUEUED)
        );
    }

    @Test
    public void reconstitutePreservesState() {
        JobId id = JobId.generate();
        String type = "email:send";
        JobStatus status = JobStatus.PROCESSING;
        Payload payload = Payload.empty();
        Instant createdAt = Instant.parse("2026-09-20T10:00:00Z");
        Instant updatedAt = Instant.parse("2026-09-20T11:00:00Z");

        Map<String, String> metadata = Map.of(
                "source", "api",
                "priority", "high"
        );

        Job job = Job.reconstitute(
                id,
                type,
                status,
                payload,
                createdAt,
                updatedAt,
                metadata
        );

        assertEquals(id, job.id());
        assertEquals(type, job.type());
        assertEquals(status, job.status());
        assertEquals(payload, job.payload());
        assertEquals(createdAt, job.createdAt());
        assertEquals(updatedAt, job.updatedAt());
        assertEquals(metadata, job.metadata());
    }

    @Test
    public void metadataIsImmutable() {
        Map<String, String> originalMetadata = new HashMap<>();
        originalMetadata.put("source", "api");

        Job job = Job.reconstitute(
                JobId.generate(),
                "email:send",
                JobStatus.PENDING,
                Payload.empty(),
                Instant.now(),
                Instant.now(),
                originalMetadata
        );

        originalMetadata.put("source", "worker");

        assertEquals("api", job.metadata().get("source"));

        assertThrowsExactly(
                UnsupportedOperationException.class,
                () -> job.metadata().put("priority", "high")
        );
    }

    @Test
    public void withMetadataReturnsNewJob() {
        Job original = Job.create(
                "email:send",
                Payload.empty()
        );

        Instant originalUpdatedAt = original.updatedAt();

        Job newJob = original.withMetadata(
                "failure.reason",
                "Invalid email"
        );

        assertEquals(
                "Invalid email",
                newJob.metadata().get("failure.reason")
        );

        assertTrue(
                newJob.updatedAt().compareTo(originalUpdatedAt) >= 0
        );

        assertTrue(original.metadata().isEmpty());

        assertNotSame(original, newJob);
        assertEquals(original.id(), newJob.id());
        assertEquals(original.type(), newJob.type());
        assertEquals(original.status(), newJob.status());
        assertEquals(original.payload(), newJob.payload());
        assertEquals(original.createdAt(), newJob.createdAt());
    }

    @Test
    public void withMetadataReplacesExistingValue() {
        Job original = Job.reconstitute(
                JobId.generate(),
                "email:send",
                JobStatus.FAILED,
                Payload.empty(),
                Instant.now(),
                Instant.now(),
                Map.of(
                        "failure.reason",
                        "old reason"
                )
        );

        Job updated = original.withMetadata(
                "failure.reason",
                "new reason"
        );

        assertEquals(
                "new reason",
                updated.metadata().get("failure.reason")
        );

        assertEquals(
                "old reason",
                original.metadata().get("failure.reason")
        );
    }

    @Test
    public void withMetadataRejectsNullKey() {
        Job job = Job.create(
                "email:send",
                Payload.empty()
        );

        assertThrowsExactly(
                NullPointerException.class,
                () -> job.withMetadata(null, "reason")
        );
    }

    @Test
    public void withMetadataRejectsBlankKey() {
        Job job = Job.create(
                "email:send",
                Payload.empty()
        );

        assertThrowsExactly(
                IllegalArgumentException.class,
                () -> job.withMetadata("   ", "reason")
        );
    }

    @Test
    public void withMetadataRejectsNullValue() {
        Job job = Job.create(
                "email:send",
                Payload.empty()
        );

        assertThrowsExactly(
                NullPointerException.class,
                () -> job.withMetadata(
                        "failure.reason",
                        null
                )
        );
    }

    @Test
    void newJobStartsWithDefaultRetryState() {
        Job job = Job.create("test-job", Payload.empty());

        assertEquals(0, job.retryCount());
        assertEquals(3, job.maxRetries());
        assertTrue(job.lastErrorReason().isEmpty());
        assertTrue(job.lastFailedAt().isEmpty());
    }

    @Test
    void failedJobCanCreateRetryAttempt() {
        Job job = Job.create("test-job", Payload.empty())
                .withStatus(JobStatus.QUEUED)
                .withStatus(JobStatus.PROCESSING)
                .withStatus(JobStatus.FAILED);

        Job retryingJob = job.withRetryAttempt("Connection failed");

        assertEquals(JobStatus.RETRYING, retryingJob.status());
        assertEquals(1, retryingJob.retryCount());
        assertEquals(3, retryingJob.maxRetries());
        assertEquals(
                "Connection failed",
                retryingJob.lastErrorReason().orElseThrow()
        );
        assertTrue(retryingJob.lastFailedAt().isPresent());
    }

    @Test
    void retryAttemptDoesNotMutateOriginalJob() {
        Job job = Job.create("test-job", Payload.empty())
                .withStatus(JobStatus.QUEUED)
                .withStatus(JobStatus.PROCESSING)
                .withStatus(JobStatus.FAILED);

        Job retryingJob = job.withRetryAttempt("Temporary failure");

        assertEquals(JobStatus.FAILED, job.status());
        assertEquals(0, job.retryCount());
        assertTrue(job.lastErrorReason().isEmpty());
        assertTrue(job.lastFailedAt().isEmpty());

        assertEquals(JobStatus.RETRYING, retryingJob.status());
        assertEquals(1, retryingJob.retryCount());
    }

    @Test
    void retryCountIncrementsAcrossRetryAttempts() {
        Job job = Job.create("test-job", Payload.empty())
                .withStatus(JobStatus.QUEUED)
                .withStatus(JobStatus.PROCESSING)
                .withStatus(JobStatus.FAILED);

        Job firstRetry = job.withRetryAttempt("First failure");

        Job secondFailure = firstRetry
                .withStatus(JobStatus.QUEUED)
                .withStatus(JobStatus.PROCESSING)
                .withStatus(JobStatus.FAILED);

        Job secondRetry = secondFailure.withRetryAttempt("Second failure");

        assertEquals(1, firstRetry.retryCount());
        assertEquals(2, secondRetry.retryCount());

        assertEquals(
                "Second failure",
                secondRetry.lastErrorReason().orElseThrow()
        );
    }

    @Test
    void retryAttemptRequiresFailedStatus() {
        Job job = Job.create("test-job", Payload.empty());

        assertThrows(
                IllegalStateException.class,
                () -> job.withRetryAttempt("Failure")
        );
    }

    @Test
    void retryAttemptFailsWhenRetriesAreExhausted() {
        Job job = Job.create("test-job", Payload.empty())
                .withStatus(JobStatus.QUEUED)
                .withStatus(JobStatus.PROCESSING)
                .withStatus(JobStatus.FAILED);

        Job retry1 = job.withRetryAttempt("Failure 1");

        Job failedAgain1 = retry1
                .withStatus(JobStatus.QUEUED)
                .withStatus(JobStatus.PROCESSING)
                .withStatus(JobStatus.FAILED);

        Job retry2 = failedAgain1.withRetryAttempt("Failure 2");

        Job failedAgain2 = retry2
                .withStatus(JobStatus.QUEUED)
                .withStatus(JobStatus.PROCESSING)
                .withStatus(JobStatus.FAILED);

        Job retry3 = failedAgain2.withRetryAttempt("Failure 3");

        assertEquals(3, retry3.retryCount());

        Job failedAgain3 = retry3
                .withStatus(JobStatus.QUEUED)
                .withStatus(JobStatus.PROCESSING)
                .withStatus(JobStatus.FAILED);

        assertThrows(
                IllegalStateException.class,
                () -> failedAgain3.withRetryAttempt("Failure 4")
        );
    }

    @Test
    void retryAttemptRejectsNullFailureReason() {
        Job job = Job.create("test-job", Payload.empty())
                .withStatus(JobStatus.QUEUED)
                .withStatus(JobStatus.PROCESSING)
                .withStatus(JobStatus.FAILED);

        assertThrows(
                NullPointerException.class,
                () -> job.withRetryAttempt(null)
        );
    }

    @Test
    void retryAttemptRejectsBlankFailureReason() {
        Job job = Job.create("test-job", Payload.empty())
                .withStatus(JobStatus.QUEUED)
                .withStatus(JobStatus.PROCESSING)
                .withStatus(JobStatus.FAILED);

        assertThrows(
                IllegalArgumentException.class,
                () -> job.withRetryAttempt("   ")
        );
    }

    @Test
    void reconstituteRestoresRetryState() {
        Instant createdAt = Instant.now().minusSeconds(60);
        Instant updatedAt = Instant.now();
        Instant lastFailedAt = Instant.now().minusSeconds(10);

        JobId id = JobId.generate();

        Job job = Job.reconstitute(
                id,
                "test-job",
                JobStatus.RETRYING,
                Payload.empty(),
                createdAt,
                updatedAt,
                Map.of("key", "value"),
                2,
                3,
                "Connection timeout",
                lastFailedAt
        );

        assertEquals(id, job.id());
        assertEquals(2, job.retryCount());
        assertEquals(3, job.maxRetries());
        assertEquals(
                "Connection timeout",
                job.lastErrorReason().orElseThrow()
        );
        assertEquals(
                lastFailedAt,
                job.lastFailedAt().orElseThrow()
        );
    }

    @Test
    void oldReconstituteOverloadUsesDefaultRetryState() {
        Job job = Job.reconstitute(
                JobId.generate(),
                "test-job",
                JobStatus.PENDING,
                Payload.empty(),
                Instant.now(),
                Instant.now(),
                Map.of()
        );

        assertEquals(0, job.retryCount());
        assertEquals(3, job.maxRetries());
        assertTrue(job.lastErrorReason().isEmpty());
        assertTrue(job.lastFailedAt().isEmpty());
    }

    @Test
    void resetRetryForRequeueChangesDeadJobToQueuedAndResetsRetryCount() {
        Job job = Job.create("test-job", Payload.empty());

        Job failed = job
                .withStatus(JobStatus.QUEUED)
                .withStatus(JobStatus.PROCESSING)
                .withStatus(JobStatus.FAILED);

        Job retrying = failed.withRetryAttempt("temporary failure");

        Job queued = retrying.withStatus(JobStatus.QUEUED);

        Job failedAgain = queued
                .withStatus(JobStatus.PROCESSING)
                .withStatus(JobStatus.FAILED);

        Job dead = failedAgain
                .withRetryAttempt("another failure")
                .withStatus(JobStatus.QUEUED)
                .withStatus(JobStatus.PROCESSING)
                .withStatus(JobStatus.FAILED)
                .withStatus(JobStatus.DEAD);

        Job requeued = dead.resetRetryForRequeue();

        assertEquals(JobStatus.QUEUED, requeued.status());
        assertEquals(0, requeued.retryCount());
        assertEquals(dead.maxRetries(), requeued.maxRetries());
    }


    @Test
    void resetRetryForRequeuePreservesDiagnosticsAndMetadata() {
        Job job = Job.create("test-job", Payload.empty());

        Job failed = job
                .withStatus(JobStatus.QUEUED)
                .withStatus(JobStatus.PROCESSING)
                .withStatus(JobStatus.FAILED)
                .withMetadata("failure.type", "test")
                .withMetadata("custom.key", "custom-value");

        Job retrying = failed.withRetryAttempt("temporary failure");

        Job dead = retrying
                .withStatus(JobStatus.QUEUED)
                .withStatus(JobStatus.PROCESSING)
                .withStatus(JobStatus.FAILED)
                .withStatus(JobStatus.DEAD);

        Job requeued = dead.resetRetryForRequeue();

        assertEquals(dead.lastErrorReason(), requeued.lastErrorReason());
        assertEquals(dead.lastFailedAt(), requeued.lastFailedAt());
        assertEquals(dead.metadata(), requeued.metadata());
        assertEquals(dead.maxRetries(), requeued.maxRetries());
        assertEquals(0, requeued.retryCount());
        assertEquals(JobStatus.QUEUED, requeued.status());
    }


    @Test
    void resetRetryForRequeueRejectsNonDeadJob() {
        Job job = Job.create("test-job", Payload.empty());

        assertThrows(
                IllegalStateException.class,
                job::resetRetryForRequeue
        );
    }
}
