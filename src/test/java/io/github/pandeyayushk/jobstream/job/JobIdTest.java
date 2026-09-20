package io.github.pandeyayushk.jobstream.job;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class JobIdTest {
    @Test
    public void NotNullUUIDTest(){
        JobId obj =JobId.generate();
        assertNotNull(obj);
        assertNotNull(obj.value());
    }

    @Test
    public void UniqueUUIDTest(){
        JobId id1 =JobId.generate();
        JobId id2=JobId.generate();
        assertNotEquals(id1,id2);

    }

    @Test
    public void fromString(){
        JobId uuid=JobId.generate();
        JobId id=JobId.fromString(uuid.toString());
        assertEquals(uuid,id);
    }

    @Test
    public void MalformedUUID() {
        assertThrowsExactly(IllegalArgumentException.class, () ->
                JobId.fromString("uid23")
        );
    }
}
