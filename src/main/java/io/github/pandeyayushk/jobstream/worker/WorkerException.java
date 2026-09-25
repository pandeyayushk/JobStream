package io.github.pandeyayushk.jobstream.worker;

public class WorkerException extends RuntimeException {
    public WorkerException(String message) {
        super(message);
    }
    public WorkerException(String message,Throwable cause){
        super(message,cause);
    }
}
