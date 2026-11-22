package io.github.pandeyayushk.jobstream.job;

import io.github.pandeyayushk.jobstream.payload.Payload;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

public final class Job {

    private final JobId id;
    private final String type;
    private final JobStatus status;
    private final Payload payload;
    private final Instant createdAt;
    private final Instant updatedAt;
    private final Map<String,String> metadata;
    private static final int DEFAULT_MAX_RETRIES=3;
    private final int maxRetries;
    private final int retryCount;
    private final String lastErrorReason;
    private final Instant lastFailedAt;
    private final int priority;
    private final Instant scheduledAt;

     private Job(JobId id, String type, JobStatus status, Payload payload, Instant createdAt, Instant updatedAt, Map<String, String> metadata) {
         this(
                 id,
                 type,
                 status,
                 payload,
                 createdAt,
                 updatedAt,
                 metadata,
                 0,
                 DEFAULT_MAX_RETRIES,
                 null,
                 null,
                 0,
                 null
         );
    }

    private Job(
            JobId id,
            String type,
            JobStatus status,
            Payload payload,
            Instant createdAt,
            Instant updatedAt,
            Map<String, String> metadata,
            int retryCount,
            int maxRetries,
            String lastErrorReason,
            Instant lastFailedAt
    ) {
        this(
                id,
                type,
                status,
                payload,
                createdAt,
                updatedAt,
                metadata,
                retryCount,
                maxRetries,
                lastErrorReason,
                lastFailedAt,
                0,
                null
        );
    }

    private Job(
            JobId id,
            String type,
            JobStatus status,
            Payload payload,
            Instant createdAt,
            Instant updatedAt,
            Map<String, String> metadata,
            int retryCount,
            int maxRetries,
            String lastErrorReason,
            Instant lastFailedAt,
            int priority,
            Instant scheduledAt
    ) {
        if (id == null) {
            throw new NullPointerException("Job Id cannot be null");
        }

        if (type == null) {
            throw new NullPointerException("Job type cannot be null");
        }

        if (type.isBlank()) {
            throw new IllegalArgumentException("Job type cannot be blank");
        }

        if (status == null) {
            throw new NullPointerException("Job status cannot be null");
        }

        if (payload == null) {
            throw new NullPointerException("Job payload cannot be null");
        }

        if (createdAt == null) {
            throw new NullPointerException("Job createdAt time cannot be null");
        }

        if (updatedAt == null) {
            throw new NullPointerException("Job updatedAt time cannot be null");
        }

        if (metadata == null) {
            throw new NullPointerException("Job metadata cannot be null");
        }

        if (retryCount < 0) {
            throw new IllegalArgumentException("Retry count cannot be negative");
        }

        if (maxRetries <= 0) {
            throw new IllegalArgumentException("Max retries must be greater than zero");
        }

        if (retryCount > maxRetries) {
            throw new IllegalArgumentException(
                    "Retry count cannot exceed max retries"
            );
        }

        this.id = id;
        this.type = type;
        this.status = status;
        this.payload = payload;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.metadata = Map.copyOf(metadata);
        this.retryCount = retryCount;
        this.maxRetries = maxRetries;
        this.lastErrorReason = lastErrorReason;
        this.lastFailedAt = lastFailedAt;
        this.priority = priority;
        this.scheduledAt = scheduledAt;
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
        return reconstitute(
                id,
                type,
                status,
                payload,
                createdAt,
                updatedAt,
                metadata,
                0,
                DEFAULT_MAX_RETRIES,
                null,
                null
        );
    }

    public static Job reconstitute(
            JobId id,
            String type,
            JobStatus status,
            Payload payload,
            Instant createdAt,
            Instant updatedAt,
            Map<String, String> metadata,
            int retryCount,
            int maxRetries,
            String lastErrorReason,
            Instant lastFailedAt
    ) {
        return new Job(
                id,
                type,
                status,
                payload,
                createdAt,
                updatedAt,
                metadata,
                retryCount,
                maxRetries,
                lastErrorReason,
                lastFailedAt
        );
    }

    public static Job reconstitute(
            JobId id,
            String type,
            JobStatus status,
            Payload payload,
            Instant createdAt,
            Instant updatedAt,
            Map<String, String> metadata,
            int retryCount,
            int maxRetries,
            String lastErrorReason,
            Instant lastFailedAt,
            int priority,
            Instant scheduledAt
    ) {
        return new Job(
                id,
                type,
                status,
                payload,
                createdAt,
                updatedAt,
                metadata,
                retryCount,
                maxRetries,
                lastErrorReason,
                lastFailedAt,
                priority,
                scheduledAt
        );
    }

    public Job withStatus(JobStatus newStatus){
         if(!status.isValidTransition(newStatus))throw new IllegalStateException("Invalid next status for this job");
        return copy(
                newStatus,
                metadata,
                retryCount,
                lastErrorReason,
                lastFailedAt
        );
    }

    public Job withMetadata(String key, String value) {
        if (key == null) {
            throw new NullPointerException("Metadata key cannot be null");
        }

        if (key.isBlank()) {
            throw new IllegalArgumentException(
                    "Metadata key cannot be blank"
            );
        }

        if (value == null) {
            throw new NullPointerException(
                    "Metadata value cannot be null"
            );
        }

        Map<String, String> newMetadata =
                new java.util.HashMap<>(metadata);

        newMetadata.put(key, value);

        return copy(
                status,
                newMetadata,
                retryCount,
                lastErrorReason,
                lastFailedAt
        );
    }

    public Job withRetryAttempt(String failureReason) {
        if (status != JobStatus.FAILED) {
            throw new IllegalStateException(
                    "Only failed jobs can proceed for retry"
            );
        }

        if (failureReason == null) {
            throw new NullPointerException(
                    "Failure reason cannot be null"
            );
        }

        if (failureReason.isBlank()) {
            throw new IllegalArgumentException(
                    "Failure reason cannot be blank"
            );
        }

        if (retryCount >= maxRetries) {
            throw new IllegalStateException(
                    "Job has exhausted its retry attempts"
            );
        }

        int newRetryCount = retryCount + 1;
        Instant failureTime = Instant.now();

        return new Job(
                id,
                type,
                JobStatus.RETRYING,
                payload,
                createdAt,
                failureTime,
                metadata,
                newRetryCount,
                maxRetries,
                failureReason,
                failureTime,
                priority,
                scheduledAt
        );
    }

    private Job copy(
            JobStatus newStatus,
            Map<String, String> newMetadata,
            int newRetryCount,
            String newLastErrorReason,
            Instant newLastFailedAt
    ) {
        return new Job(
                id,
                type,
                newStatus,
                payload,
                createdAt,
                Instant.now(),
                newMetadata,
                newRetryCount,
                maxRetries,
                newLastErrorReason,
                newLastFailedAt,
                priority,
                scheduledAt
        );
    }

    public Job withPriority(int newPriority) {
        return new Job(
                id,
                type,
                status,
                payload,
                createdAt,
                nextUpdatedAt(),
                metadata,
                retryCount,
                maxRetries,
                lastErrorReason,
                lastFailedAt,
                newPriority,
                scheduledAt
        );
    }

    public Job withScheduledAt(Instant instant) {
        if (instant == null) {
            throw new NullPointerException("Scheduled time cannot be null");
        }

        return new Job(
                id,
                type,
                status,
                payload,
                createdAt,
                nextUpdatedAt(),
                metadata,
                retryCount,
                maxRetries,
                lastErrorReason,
                lastFailedAt,
                priority,
                instant
        );
    }

    private Instant nextUpdatedAt() {
        Instant now = Instant.now();
        return now.isAfter(updatedAt) ? now : updatedAt.plusNanos(1);
    }

    public Job resetRetryForRequeue() {
        if (status != JobStatus.DEAD) {
            throw new IllegalStateException(
                    "Only dead jobs can be requeued"
            );
        }

        return new Job(
                id,
                type,
                JobStatus.QUEUED,
                payload,
                createdAt,
                Instant.now(),
                metadata,
                0,
                maxRetries,
                lastErrorReason,
                lastFailedAt,
                priority,
                scheduledAt
        );
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

    public int retryCount() {
        return retryCount;
    }

    public int maxRetries() {
        return maxRetries;
    }

    public Optional<String> lastErrorReason() {
        return Optional.ofNullable(lastErrorReason);
    }

    public Optional<Instant> lastFailedAt() {
        return Optional.ofNullable(lastFailedAt);
    }

    public int priority() {
        return priority;
    }

    public Optional<Instant> scheduledAt() {
        return Optional.ofNullable(scheduledAt);
    }
}
