package io.github.pandeyayushk.jobstream.serialization;

import io.github.pandeyayushk.jobstream.api.ApiException;
import io.github.pandeyayushk.jobstream.api.ErrorResponse;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class JacksonJsonCodecTest {

    @Test
    void exposesOneSharedCodecForApiCliAndJobSerialization() {
        assertSame(JacksonJsonCodec.shared(), JacksonJsonCodec.shared());
    }

    @Test
    void encodesErrorEnvelopeAndDecodesJsonObjects() {
        JacksonJsonCodec codec = JacksonJsonCodec.shared();
        ErrorResponse response = ErrorResponse.from(ApiException.validation());

        String json = codec.encode(response);
        Map<String, Object> decoded = codec.decodeObject(json);

        assertEquals(
                "VALIDATION_ERROR",
                ((Map<?, ?>) decoded.get("error")).get("code")
        );
        assertEquals(Map.of("ok", true), codec.decodeObject("{\"ok\":true}"));
    }
}
