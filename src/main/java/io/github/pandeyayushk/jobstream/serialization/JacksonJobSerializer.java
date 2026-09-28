package io.github.pandeyayushk.jobstream.serialization;

import io.github.pandeyayushk.jobstream.job.Job;

import io.github.pandeyayushk.jobstream.job.JobId;
import io.github.pandeyayushk.jobstream.job.JobStatus;
import io.github.pandeyayushk.jobstream.payload.Payload;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.Map;

public class JacksonJobSerializer implements JobSerializer{


    private record JobData(
            String id,
            String type,
            JobStatus status,
            Map<String, Object> payload,
            Instant createdAt,
            Instant updatedAt,
            Map<String, String> metadata,
            int retryCount,
            int maxRetries,
            String lastErrorReason,
            Instant lastFailedAt
    ) {}

    private final ObjectMapper mapper = new ObjectMapper();
    @Override
    public String serialize(Job job) throws SerializationException {
        JobData data=new JobData(
                job.id().toString(),
                job.type(),
                job.status(),
                job.payload().asMap(),
                job.createdAt(),
                job.updatedAt(),
                job.metadata(),
                job.retryCount(),
                job.maxRetries(),
                job.lastErrorReason().orElse(null),
                job.lastFailedAt().orElse(null)
        );
        String json;
        try{
             json=mapper.writeValueAsString(data);


        } catch (JacksonException e) {
            throw new SerializationException("Failed to serialize Job",e);
        }
        return json;
    }

    @Override
    public Job deserialize(String json) throws SerializationException {
        Job job;
        try{
            JobData jobData=mapper.readValue(json,JobData.class);
            JobId id=JobId.fromString(jobData.id());
            Payload payload=Payload.of(jobData.payload());
            job=Job.reconstitute(id,
                    jobData.type(),
                    jobData.status(),
                    payload,
                    jobData.createdAt(),
                    jobData.updatedAt(),
                    jobData.metadata(),
                    jobData.retryCount(),
                    jobData.maxRetries(),
                    jobData.lastErrorReason(),
                    jobData.lastFailedAt());
        }catch (JacksonException | IllegalArgumentException e){
            throw new SerializationException("Failed to deserialize Job",e);
        }
        return job;
    }
}
