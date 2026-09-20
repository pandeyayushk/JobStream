package io.github.pandeyayushk.jobstream.job;

import java.util.Objects;
import java.util.UUID;

public record JobId(UUID value) {

    public JobId {
        Objects.requireNonNull(value, "JobId value cannot be null");
    }

    /**
     * Factory Method generating a random UUID for jobs
     *
     * @return the JobID object
     */
    public static JobId generate() {
        return new JobId(UUID.randomUUID());
    }

    /**
     * Parsing a UUID String with validation
     *
     * @param uuidString
     * @return
     */
    public static JobId fromString(String uuidString) {
        return new JobId(UUID.fromString(uuidString));
    }

    @Override
    public String toString() {
        return value.toString();
    }


}



