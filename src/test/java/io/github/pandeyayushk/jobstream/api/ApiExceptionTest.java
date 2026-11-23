package io.github.pandeyayushk.jobstream.api;

import io.github.pandeyayushk.jobstream.serialization.JacksonJsonCodec;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ApiExceptionTest {

    @Test
    void mapsErrorKindsToStableCodesAndStatuses() {
        assertMapping(ApiException.validation(), ApiErrorCode.VALIDATION_ERROR, 400);
        assertMapping(ApiException.requestTooLarge(), ApiErrorCode.REQUEST_TOO_LARGE, 413);
        assertMapping(ApiException.notFound(), ApiErrorCode.NOT_FOUND, 404);
        assertMapping(ApiException.conflict(), ApiErrorCode.CONFLICT, 409);
        assertMapping(
                ApiException.unavailable(new IllegalStateException("internal")),
                ApiErrorCode.SERVICE_UNAVAILABLE,
                503
        );
        assertMapping(
                ApiException.internal(new IllegalStateException("internal")),
                ApiErrorCode.INTERNAL_ERROR,
                500
        );
    }

    @Test
    void preservesCauseButDoesNotExposeInternalDetailsInResponse() {
        RuntimeException cause = new RuntimeException(
                "redis://operator:secret@internal-host stack trace details"
        );
        ApiException exception = ApiException.internal(cause);

        assertSame(cause, exception.getCause());
        assertEquals("An internal error occurred", exception.getMessage());

        String json = JacksonJsonCodec.shared().encode(ErrorResponse.from(exception));
        assertTrue(json.contains("\"code\":\"INTERNAL_ERROR\""));
        assertTrue(json.contains("\"message\":\"An internal error occurred\""));
        assertFalse(json.contains("operator"));
        assertFalse(json.contains("secret"));
        assertFalse(json.contains("stack trace"));
    }

    @Test
    void unavailableResponseUsesSafeMessageAndKeepsDiagnosticCause() {
        RuntimeException cause = new RuntimeException("password=private-value");
        ApiException exception = ApiException.unavailable(cause);
        ErrorResponse response = ErrorResponse.from(exception);

        assertSame(cause, exception.getCause());
        assertEquals("SERVICE_UNAVAILABLE", response.error().code());
        assertEquals("A required service is unavailable", response.error().message());
    }

    private void assertMapping(
            ApiException exception,
            ApiErrorCode expectedCode,
            int expectedStatus
    ) {
        assertEquals(expectedCode, exception.errorCode());
        assertEquals(expectedStatus, exception.httpStatus());
        assertEquals(expectedCode.name(), ErrorResponse.from(exception).error().code());
        assertEquals(expectedCode.safeMessage(), ErrorResponse.from(exception).error().message());
    }
}
