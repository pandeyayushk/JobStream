package io.github.pandeyayushk.jobstream.executor;

import java.util.Objects;
import java.util.Optional;

public final class ExecutionResult {

    private final boolean success;
    private final String errorMessage;
    private final Throwable errorCause;

    private ExecutionResult(
            boolean success,
            String errorMessage,
            Throwable errorCause
    ) {
        this.success = success;
        this.errorMessage = errorMessage;
        this.errorCause = errorCause;
    }

    public static ExecutionResult success() {
        return new ExecutionResult(
                true,
                null,
                null
        );
    }

    public static ExecutionResult failure(String message) {
        Objects.requireNonNull(
                message,
                "Failure message cannot be null"
        );

        if (message.isBlank()) {
            throw new IllegalArgumentException(
                    "Failure message cannot be blank"
            );
        }

        return new ExecutionResult(
                false,
                message,
                null
        );
    }

    public static ExecutionResult failure(Throwable cause) {
        Objects.requireNonNull(
                cause,
                "Failure cause cannot be null"
        );

        return new ExecutionResult(
                false,
                cause.getMessage(),
                cause
        );
    }

    public boolean isSuccess() {
        return success;
    }

    public Optional<String> getErrorMessage() {
        return Optional.ofNullable(errorMessage);
    }

    public Optional<Throwable> getErrorCause() {
        return Optional.ofNullable(errorCause);
    }
}