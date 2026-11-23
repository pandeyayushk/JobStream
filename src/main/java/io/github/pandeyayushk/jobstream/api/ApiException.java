package io.github.pandeyayushk.jobstream.api;

import java.util.Objects;

/** Internal API failure carrying a safe client error and an optional diagnostic cause. */
public final class ApiException extends RuntimeException {
    private final ApiErrorCode errorCode;

    private ApiException(ApiErrorCode errorCode, Throwable cause) {
        super(Objects.requireNonNull(errorCode, "Error code cannot be null").safeMessage(), cause);
        this.errorCode = errorCode;
    }

    public static ApiException validation() {
        return new ApiException(ApiErrorCode.VALIDATION_ERROR, null);
    }

    public static ApiException requestTooLarge() {
        return new ApiException(ApiErrorCode.REQUEST_TOO_LARGE, null);
    }

    public static ApiException notFound() {
        return new ApiException(ApiErrorCode.NOT_FOUND, null);
    }

    public static ApiException conflict() {
        return new ApiException(ApiErrorCode.CONFLICT, null);
    }

    public static ApiException unavailable(Throwable cause) {
        return new ApiException(ApiErrorCode.SERVICE_UNAVAILABLE, cause);
    }

    public static ApiException internal(Throwable cause) {
        return new ApiException(ApiErrorCode.INTERNAL_ERROR, cause);
    }

    public ApiErrorCode errorCode() {
        return errorCode;
    }

    public int httpStatus() {
        return errorCode.httpStatus();
    }
}
