package io.github.pandeyayushk.jobstream.queue;

public class QueueException extends RuntimeException {
    public QueueException(String message) {
        super(message);
    }
    public  QueueException(String message,Throwable cause){
        super(message,cause);
    }
}
