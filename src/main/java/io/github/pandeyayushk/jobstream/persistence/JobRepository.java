package io.github.pandeyayushk.jobstream.persistence;

import io.github.pandeyayushk.jobstream.job.Job;
import io.github.pandeyayushk.jobstream.job.JobId;
import io.github.pandeyayushk.jobstream.job.JobStatus;

import java.util.List;
import java.util.Optional;

public interface JobRepository {
    void save(Job job);
    Optional<Job> findById(JobId id);
    boolean delete(JobId id);
    List<Job> findByStatus(JobStatus status);
    boolean exists(JobId id);
}
