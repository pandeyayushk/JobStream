package io.github.pandeyayushk.jobstream.executor;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ExecutionResultTest {

    @Test
    void successCreatesSuccessfulResult() {
        ExecutionResult result =
                ExecutionResult.success();

        assertTrue(result.isSuccess());
        assertTrue(result.getErrorMessage().isEmpty());
        assertTrue(result.getErrorCause().isEmpty());
    }

    @Test
    void failureWithMessageCreatesFailedResult() {
        ExecutionResult result =
                ExecutionResult.failure("Something failed");

        assertFalse(result.isSuccess());
        assertEquals(
                "Something failed",
                result.getErrorMessage().orElseThrow()
        );
        assertTrue(result.getErrorCause().isEmpty());
    }

    @Test
    void failureWithThrowableCreatesFailedResult() {
        IllegalStateException exception =
                new IllegalStateException("Something failed");

        ExecutionResult result =
                ExecutionResult.failure(exception);

        assertFalse(result.isSuccess());
        assertEquals(
                "Something failed",
                result.getErrorMessage().orElseThrow()
        );
        assertSame(
                exception,
                result.getErrorCause().orElseThrow()
        );
    }

    @Test
    void failureRejectsNullMessage() {
        assertThrowsExactly(
                NullPointerException.class,
                () -> ExecutionResult.failure((String) null)
        );
    }

    @Test
    void failureRejectsBlankMessage() {
        assertThrowsExactly(
                IllegalArgumentException.class,
                () -> ExecutionResult.failure("   ")
        );
    }

    @Test
    void failureRejectsNullCause() {
        assertThrowsExactly(
                NullPointerException.class,
                () -> ExecutionResult.failure((Throwable) null)
        );
    }
}