package io.github.pandeyayushk.jobstream.retry;

import redis.clients.jedis.exceptions.JedisException;

public class RetryException extends RuntimeException {
    public RetryException(String message) {
        super(message);
    }
    public  RetryException(String message,Throwable cause){
        super(message,cause);
    }
}
