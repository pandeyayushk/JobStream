package io.github.pandeyayushk.jobstream.worker;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

public class WorkerIdTest {
    @Test
    public void generateReturnsNonNullIdWithNonNullUuid(){
        WorkerId obj =WorkerId.generate();
        assertNotNull(obj);
        assertNotNull(obj.value());
    }

    @Test
    public void generateReturnsUniqueIds(){
        WorkerId id1 =WorkerId.generate();
        WorkerId id2=WorkerId.generate();
        assertNotEquals(id1,id2);

    }

    @Test
    public void fromStringRoundTrip(){
        WorkerId uuid=WorkerId.generate();
        WorkerId id=WorkerId.fromString(uuid.toString());
        assertEquals(uuid,id);
    }

    @Test
    public void fromStringRejectsMalformedUuid() {
        assertThrowsExactly(IllegalArgumentException.class, () ->
                WorkerId.fromString("uid23")
        );
    }

    @Test
    public void constructorRejectsNullUuid() {
        assertThrowsExactly(
                NullPointerException.class,
                () -> new WorkerId(null)
        );
    }
}
