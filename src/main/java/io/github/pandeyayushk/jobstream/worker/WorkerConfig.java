package io.github.pandeyayushk.jobstream.worker;

import java.time.Duration;
import java.util.Objects;

public record WorkerConfig(String queueName, int concurrency, Duration heartbeatInterval, Duration heartbeatTtl,
        Duration shutdownTimeout) {
    public WorkerConfig{
        Objects.requireNonNull(queueName,"Queue name can not be null");
        if(queueName.isBlank())throw new IllegalArgumentException("Queue name can not be blank");
        if(concurrency<=0)throw new IllegalArgumentException("Concurrency must be greater than zero");
        Objects.requireNonNull(heartbeatInterval,"Heartbeat interval cannot be null");
        if(heartbeatInterval.isZero()||heartbeatInterval.isNegative())throw new IllegalArgumentException("Heartbeat Interval must be greater than zero");
        Objects.requireNonNull(heartbeatTtl,"Heartbeat Ttl cannot be null");
        if(heartbeatTtl.isZero()||heartbeatTtl.isNegative())throw new IllegalArgumentException("Heartbeat Ttl must be greater than zero");
        Objects.requireNonNull(shutdownTimeout,"Shutdown Timeout cannot be null");
        if(shutdownTimeout.isZero()||shutdownTimeout.isNegative())throw new IllegalArgumentException("Shutdown Timeout must be greater than zero");
        if(heartbeatInterval.compareTo(heartbeatTtl)>=0)throw new IllegalArgumentException("Heartbeat Interval must be less than HeartBeatTtl");
    }

    public static WorkerConfig defaults(){
        return new WorkerConfig("default",4,Duration.ofSeconds(10),Duration.ofSeconds(30),Duration.ofSeconds(30));
    }
}
