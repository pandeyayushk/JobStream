package io.github.pandeyayushk.jobstream;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;


public class MainTest {
    @Test
    public void startingJobStreamReturnsStartupMessage(){
        String result=Main.startingJobStream();
        assertEquals("JobStream starting...",result);
    }
}
