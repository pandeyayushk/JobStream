package io.github.pandeyayushk.jobstream.executor;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class DefaultExecutorRegistry implements ExecutorRegistry {

    private final ConcurrentMap<String, JobExecutor> executors =
            new ConcurrentHashMap<>();

    @Override
    public void register(String jobType, JobExecutor executor) {
        Objects.requireNonNull(
                jobType,
                "Job type cannot be null"
        );

        if (jobType.isBlank()) {
            throw new IllegalArgumentException(
                    "Job type cannot be blank"
            );
        }

        Objects.requireNonNull(
                executor,
                "Job executor cannot be null"
        );

        executors.put(jobType, executor);
    }

    @Override
    public Optional<JobExecutor> get(String jobType) {
        Objects.requireNonNull(
                jobType,
                "Job type cannot be null"
        );

        return Optional.ofNullable(
                executors.get(jobType)
        );
    }

    @Override
    public boolean hasExecutor(String jobType) {
        return get(jobType).isPresent();
    }
}