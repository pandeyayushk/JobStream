package io.github.pandeyayushk.jobstream.job;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class JobStatusTest {

    @Test
    public void ContainsAllExpectedStatuses(){
       assertEquals(JobStatus.PENDING,JobStatus.values()[0]);
       assertEquals(JobStatus.QUEUED,JobStatus.values()[1]);
       assertEquals(JobStatus.PROCESSING,JobStatus.values()[2]);
       assertEquals(JobStatus.COMPLETED,JobStatus.values()[3]);
       assertEquals(JobStatus.FAILED,JobStatus.values()[4]);
       assertEquals(JobStatus.RETRYING,JobStatus.values()[5]);
       assertEquals(JobStatus.DEAD,JobStatus.values()[6]);
    }

    @Test
    public void IdentifiesTerminalStatuses(){
        assertTrue(JobStatus.COMPLETED.isTerminal());
        assertTrue(JobStatus.DEAD.isTerminal());
        assertFalse(JobStatus.PENDING.isTerminal());
        assertFalse(JobStatus.QUEUED.isTerminal());
        assertFalse(JobStatus.PROCESSING.isTerminal());
        assertFalse(JobStatus.FAILED.isTerminal());
        assertFalse(JobStatus.RETRYING.isTerminal());
    }

    @Test
    public void AcceptsValidTransitions(){
        assertTrue(JobStatus.PENDING.isValidTransition(JobStatus.QUEUED));
        assertTrue(JobStatus.QUEUED.isValidTransition(JobStatus.PROCESSING));
        assertTrue(JobStatus.PROCESSING.isValidTransition(JobStatus.COMPLETED));
        assertTrue(JobStatus.PROCESSING.isValidTransition(JobStatus.FAILED));
        assertTrue(JobStatus.FAILED.isValidTransition(JobStatus.RETRYING));
        assertTrue(JobStatus.FAILED.isValidTransition(JobStatus.DEAD));
        assertTrue(JobStatus.RETRYING.isValidTransition(JobStatus.QUEUED));
        assertTrue(JobStatus.DEAD.isValidTransition(JobStatus.QUEUED));
    }

    @Test
    public void RejectsInvalidTransitions(){
        assertFalse(JobStatus.PENDING.isValidTransition(JobStatus.PROCESSING));
        assertFalse(JobStatus.QUEUED.isValidTransition(JobStatus.COMPLETED));
        assertFalse(JobStatus.PROCESSING.isValidTransition(JobStatus.QUEUED));
        assertFalse(JobStatus.FAILED.isValidTransition(JobStatus.COMPLETED));
        assertFalse(JobStatus.RETRYING.isValidTransition(JobStatus.COMPLETED));
        assertFalse(JobStatus.COMPLETED.isValidTransition(JobStatus.QUEUED));
        assertFalse(JobStatus.DEAD.isValidTransition(JobStatus.PROCESSING));
    }

}


