package io.github.pandeyayushk.jobstream.worker;

import io.github.pandeyayushk.jobstream.job.Job;

public interface WorkerJobHandler {
    void handle(Job job);
}
