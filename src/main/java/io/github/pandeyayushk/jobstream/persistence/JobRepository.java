package io.github.pandeyayushk.jobstream.persistence;

import io.github.pandeyayushk.jobstream.job.Job;
import io.github.pandeyayushk.jobstream.job.JobId;
import io.github.pandeyayushk.jobstream.job.JobStatus;

import java.util.List;
import java.util.Optional;

public interface JobRepository {
    /**
     * Persists the supplied revision if it is current, then returns the new persisted snapshot.
     * A stale snapshot is rejected with {@link ConcurrentJobUpdateException}.
     */
    Job save(Job job);
    Optional<Job> findById(JobId id);
    boolean delete(JobId id);
    List<Job> findByStatus(JobStatus status);
    boolean exists(JobId id);
}
