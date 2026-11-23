package io.github.pandeyayushk.jobstream.api;

/** Stable client-facing error codes and their HTTP status mapping. */
public enum ApiErrorCode {
    VALIDATION_ERROR(400, "Request validation failed"),
    REQUEST_TOO_LARGE(413, "Request body exceeds the configured size limit"),
    NOT_FOUND(404, "Resource not found"),
    CONFLICT(409, "Request conflicts with the current resource state"),
    SERVICE_UNAVAILABLE(503, "A required service is unavailable"),
    INTERNAL_ERROR(500, "An internal error occurred");

    private final int httpStatus;
    private final String safeMessage;

    ApiErrorCode(int httpStatus, String safeMessage) {
        this.httpStatus = httpStatus;
        this.safeMessage = safeMessage;
    }

    public int httpStatus() {
        return httpStatus;
    }

    public String safeMessage() {
        return safeMessage;
    }
}
