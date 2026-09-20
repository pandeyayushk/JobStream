package io.github.pandeyayushk.jobstream.payload;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

public class PayloadTest {

    @Test
    public void typedValuesAreAccessible(){
        Map<String, Object> data = Map.of(
                "name", "Ayush",
                "age", 21,
                "active", true
        );
        Payload payload=Payload.of(data);
        assertEquals(Optional.of("Ayush"),payload.getString("name") );
        assertEquals(Optional.of(21),payload.getInt("age") );
        assertEquals(Optional.of(true),payload.getBoolean("active") );
    }

    @Test
    public void wrongTypesReturnEmpty(){
        Map<String, Object> data = Map.of(
                "name", "Ayush",
                "age", 21,
                "active", true
        );
        Payload payload=Payload.of(data);
        assertEquals(Optional.empty(),payload.getInt("name"));
        assertEquals(Optional.empty(),payload.getString("age"));
        assertEquals(Optional.empty(),payload.getString("active"));
    }

    @Test
    public void missingKeysReturnEmpty(){
        Map<String, Object> data = Map.of(
                "name", "Ayush",
                "age", 21,
                "active", true
        );
        Payload payload=Payload.of(data);
        assertEquals(Optional.empty(),payload.getString("missing"));
    }

    @Test
    public void modifyingOriginalMapDoesNotAffectPayload(){
        Map<String, Object> original = new HashMap<>();
        original.put("name", "Ayush");

        Payload payload = Payload.of(original);

        original.put("name", "Raj");

        assertEquals(Optional.of("Ayush"), payload.getString("name"));
    }

    @Test
    public void modifyingAsMapThrowsException(){
        Map<String, Object> data = Map.of(
                "name", "Ayush",
                "age", 21,
                "active", true
        );
        Payload payload=Payload.of(data);
        Map<String ,Object> map=payload.asMap();
        assertThrowsExactly(UnsupportedOperationException.class, () ->
                map.put("name","raj")
        );
    }

    @Test
    public void nullMapThrowsException(){
        assertThrowsExactly(NullPointerException.class,() ->
                Payload.of(null)
        );
    }

    @Test
    public void nullKeysThrowException(){
        Map<String, Object> data = Map.of(
                "name", "Ayush",
                "age", 21,
                "active", true
        );
        Payload payload=Payload.of(data);
        assertThrowsExactly(NullPointerException.class,() ->
                payload.getString(null)
        );
        assertThrowsExactly(NullPointerException.class,() ->
                payload.getInt(null)
        );
        assertThrowsExactly(NullPointerException.class,() ->
                payload.getBoolean(null)
        );
        assertThrowsExactly(NullPointerException.class,() ->
                payload.containsKey(null)
        );
    }

    @Test
    public void emptyPayloadContainsNoValues(){
        assertFalse(Payload.empty().containsKey("anything"));
    }
}
