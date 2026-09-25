package io.github.pandeyayushk.jobstream.worker;

import io.github.pandeyayushk.jobstream.job.Job;
import io.github.pandeyayushk.jobstream.payload.Payload;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class WorkerJobHandlerTest {

    @Test
    void handlerReceivesJob() {
        AtomicReference<Job> receivedJob = new AtomicReference<>();

        WorkerJobHandler handler =
                receivedJob::set;

        Job job = Job.create(
                "test-job",
                Payload.empty()
        );

        handler.handle(job);

        assertSame(job, receivedJob.get());
    }

    @Test
    void handlerExceptionPropagates() {
        WorkerJobHandler handler = job -> {
            throw new IllegalStateException("handler failed");
        };

        Job job = Job.create(
                "test-job",
                Payload.empty()
        );

        IllegalStateException exception = assertThrows(
                IllegalStateException.class,
                () -> handler.handle(job)
        );

        assertEquals("handler failed", exception.getMessage());
    }
}