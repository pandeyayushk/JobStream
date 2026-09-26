package io.github.pandeyayushk.jobstream.executor;

import io.github.pandeyayushk.jobstream.job.Job;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class JobExecutorTest {

    @Test
    void executorCanExecuteJobAndReturnSuccess() {
        JobExecutor executor =
                job -> ExecutionResult.success();

        ExecutionResult result =
                executor.execute(null);

        assertTrue(result.isSuccess());
    }
}