package io.github.pandeyayushk.jobstream.job;

import io.github.pandeyayushk.jobstream.payload.Payload;

import java.time.Instant;
import java.util.Map;

public final class Job {

    private final JobId id;
    private final String type;
    private final JobStatus status;
    private final Payload payload;
    private final Instant createdAt;
    private final Instant updatedAt;
    private final Map<String,String> metadata;

     private Job(JobId id, String type, JobStatus status, Payload payload, Instant createdAt, Instant updatedAt, Map<String, String> metadata) {
        if(id==null)throw new NullPointerException("Job Id cannot be null");
        if(type==null)throw new NullPointerException("Job type cannot be null");
        if(type.isBlank())throw new IllegalArgumentException("Job type cannot be blank");
        if(status==null)throw new NullPointerException("Job status cannot be null");
        if(payload==null)throw new NullPointerException("Job payload cannot be null");
        if(createdAt==null)throw new NullPointerException("Job createdAt time cannot be null");
        if(updatedAt==null)throw new NullPointerException("Job updatedAt time cannot be null");
        this.id = id;
        this.type = type;
        this.status = status;
        this.payload = payload;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.metadata = Map.copyOf(metadata);
    }

    public static Job create(String type, Payload payload){
         JobId id=JobId.generate();
         JobStatus status=JobStatus.PENDING;
         Instant now=Instant.now();
        Map<String,String> metadata=Map.of();
         return new Job(id,type,status,payload, now, now,metadata);
    }

    public static Job reconstitute(JobId id, String type, JobStatus status, Payload payload,
                            Instant createdAt, Instant updatedAt, Map<String, String> metadata){
         return new Job(id,type,status,payload,createdAt,updatedAt,metadata);
    }

    public Job withStatus(JobStatus newStatus){
         if(!status.isValidTransition(newStatus))throw new IllegalStateException("Invalid next status for this job");
         Instant updatedAt=Instant.now();
         return new Job(id,type,newStatus,payload,createdAt,updatedAt,metadata);
    }


    public JobId id() {
        return id;
    }

    public String type() {
        return type;
    }

    public JobStatus status() {
        return status;
    }

    public Payload payload() {
        return payload;
    }

    public Instant createdAt() {
        return createdAt;
    }

    public Instant updatedAt() {
        return updatedAt;
    }

    public Map<String, String> metadata() {
        return metadata;
    }
}
