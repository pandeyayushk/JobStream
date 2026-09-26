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
}
