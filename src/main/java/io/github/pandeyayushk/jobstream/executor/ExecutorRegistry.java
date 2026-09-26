package io.github.pandeyayushk.jobstream.executor;

import java.util.Optional;

public interface ExecutorRegistry {

    void register(String jobType, JobExecutor executor);

    Optional<JobExecutor> get(String jobType);

    boolean hasExecutor(String jobType);
}