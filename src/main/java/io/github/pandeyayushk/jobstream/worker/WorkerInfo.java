package io.github.pandeyayushk.jobstream.worker;

import java.time.Instant;
import java.util.Objects;

public record WorkerInfo(WorkerId workerId,WorkerStatus status,Instant startedAt){
    public WorkerInfo{
        Objects.requireNonNull(workerId,"WorkerId can not be null");
        Objects.requireNonNull(status,"WorkerStatus can not be null");
        Objects.requireNonNull(startedAt,"StartedAt can not be null");
    }
}