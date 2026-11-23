package io.github.pandeyayushk.jobstream.api;

import java.util.Objects;

/** The stable JSON error envelope returned by the HTTP API. */
public record ErrorResponse(Error error) {
    public ErrorResponse {
        Objects.requireNonNull(error, "Error cannot be null");
    }

    public static ErrorResponse from(ApiException exception) {
        Objects.requireNonNull(exception, "API exception cannot be null");
        return new ErrorResponse(new Error(
                exception.errorCode().name(),
                exception.errorCode().safeMessage()
        ));
    }

    public record Error(String code, String message) {
        public Error {
            Objects.requireNonNull(code, "Error code cannot be null");
            Objects.requireNonNull(message, "Error message cannot be null");
        }
    }
}
