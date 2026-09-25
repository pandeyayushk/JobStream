package io.github.pandeyayushk.jobstream.worker;

import java.util.List;
import java.util.Optional;

public interface WorkerRegistry {
    void register(WorkerInfo info);
    void heartbeat(WorkerId workerId);
    void deregister(WorkerId workerId);
    List<WorkerInfo> listActiveWorkers();
    Optional<WorkerInfo> getWorker(WorkerId workerId);
}
