package io.github.pandeyayushk.jobstream.serialization;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import java.util.Map;

/** Shared Jackson 3 codec for persisted Jobs, CLI JSON input, and API DTOs. */
public final class JacksonJsonCodec {
    private static final JacksonJsonCodec INSTANCE = new JacksonJsonCodec();

    private final ObjectMapper mapper = new ObjectMapper();

    private JacksonJsonCodec() {
    }

    public static JacksonJsonCodec shared() {
        return INSTANCE;
    }

    public String encode(Object value) throws JacksonException {
        return mapper.writeValueAsString(value);
    }

    public <T> T decode(String json, Class<T> targetType) throws JacksonException {
        return mapper.readValue(json, targetType);
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> decodeObject(String json) throws JacksonException {
        Object value = mapper.readValue(json, Map.class);
        if (!(value instanceof Map<?, ?>)) {
            throw new IllegalArgumentException("JSON value must be an object");
        }
        return (Map<String, Object>) value;
    }
}
