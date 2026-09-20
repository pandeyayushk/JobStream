package io.github.pandeyayushk.jobstream.job;

import io.github.pandeyayushk.jobstream.payload.Payload;
import org.junit.jupiter.api.Test;

import java.time.Instant;
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
}
