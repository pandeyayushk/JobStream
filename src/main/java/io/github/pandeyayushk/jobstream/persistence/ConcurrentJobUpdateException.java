package io.github.pandeyayushk.jobstream.persistence;

public class ConcurrentJobUpdateException extends PersistenceException {

    public ConcurrentJobUpdateException(String message) {
        super(message);
    }
}
