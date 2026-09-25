package io.github.pandeyayushk.jobstream.worker;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

public class WorkerInfoTest {
    @Test
    void validValuesAreStored() {
        WorkerId workerId = WorkerId.generate();
        WorkerStatus status = WorkerStatus.STARTING;
        Instant startedAt = Instant.now();

        WorkerInfo workerInfo = new WorkerInfo(workerId, status, startedAt);

        assertEquals(workerId, workerInfo.workerId());
        assertEquals(status, workerInfo.status());
        assertEquals(startedAt, workerInfo.startedAt());
    }
    @Test
    public void rejectsNullWorkedId(){
        assertThrowsExactly(NullPointerException.class,()->
                new WorkerInfo(null,WorkerStatus.STARTING, Instant.now())
        );
    }
    @Test
    public void rejectsNullWorkerStatus(){
        assertThrowsExactly(NullPointerException.class,()->
                new WorkerInfo(WorkerId.generate(),null, Instant.now())
        );
    }
    @Test
    public void rejectsNullStartedAt(){
        assertThrowsExactly(NullPointerException.class,()->
                new WorkerInfo(WorkerId.generate(),WorkerStatus.STARTING, null)
        );
    }
    @Test
    void equalValuesProduceEqualWorkerInfo() {
        WorkerId workerId = WorkerId.generate();
        WorkerStatus status = WorkerStatus.STARTING;
        Instant startedAt = Instant.now();

        WorkerInfo first = new WorkerInfo(workerId, status, startedAt);
        WorkerInfo second = new WorkerInfo(workerId, status, startedAt);

        assertEquals(first, second);
    }
}
