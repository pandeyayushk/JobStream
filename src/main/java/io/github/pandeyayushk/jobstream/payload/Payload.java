package io.github.pandeyayushk.jobstream.payload;

import java.util.Map;
import java.util.Optional;

;

public final class Payload {
    private final Map<String,Object> map;
    /**
     * Creates an empty Payload.
     */
    private Payload(){
        map=Map.of();
    }

    /**
     * Creates a Payload from the given data.
     *
     * @param data payload data
     */
    private Payload(Map<String, Object> data){
        map=Map.copyOf(data);
    }

    /**
     * Creates a Payload from the given data.
     *
     * @param data payload data
     * @return a Payload containing the given data
     */
    public static Payload of(Map<String, Object> data){
        return new Payload(data);
    }

    /**
     * Creates an empty Payload.
     *
     * @return an empty Payload
     */
    public static Payload empty(){
        return new Payload();
    }


    /**
     * Gets a String value associated with the given key.
     *
     * @param key payload key
     * @return an Optional containing the String value, or empty if missing or wrong type
     */
    public Optional<String> getString(String key){
        if(key==null){
            throw new NullPointerException("Key cannot be null");
        }
        Object value=map.get(key);
        if(value instanceof String stringValue){
            return Optional.of(stringValue);
        }
        return Optional.empty();
    }

    /**
     * Gets an Integer value associated with the given key.
     *
     * @param key payload key
     * @return an Optional containing the Integer value, or empty if missing or wrong type
     */
    public Optional<Integer> getInt(String key){
        if(key==null){
            throw new NullPointerException("Key cannot be null");
        }
        Object value=map.get(key);
        if(value instanceof Integer integerValue){
            return Optional.of(integerValue);
        }
        return Optional.empty();
    }

    /**
     * Gets a Boolean value associated with the given key.
     *
     * @param key payload key
     * @return an Optional containing the Boolean value, or empty if missing or wrong type
     */
    public Optional<Boolean> getBoolean(String key){
        if(key==null){
            throw new NullPointerException("Key cannot be null");
        }
        Object value=map.get(key);
        if(value instanceof Boolean booleanValue){
            return Optional.of(booleanValue);
        }
        return Optional.empty();
    }

    /**
     * Returns the complete immutable payload map.
     *
     * @return immutable payload data
     */
    public Map<String, Object> asMap(){
        return this.map;
    }

    /**
     * Checks whether the payload contains the given key.
     *
     * @param key payload key
     * @return true if the key exists, otherwise false
     */
    public boolean containsKey(String key){
        if(key==null)throw new NullPointerException("Key cannot be null");
        return map.containsKey(key);
    }

}

