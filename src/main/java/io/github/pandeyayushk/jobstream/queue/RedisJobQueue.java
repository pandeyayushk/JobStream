package io.github.pandeyayushk.jobstream.queue;

import io.github.pandeyayushk.jobstream.job.JobId;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.exceptions.JedisException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class RedisJobQueue implements JobQueue{
    private static final String QUEUE_PREFIX = "jobstream:queue:";
    private static final String DEFAULT_QUEUE_NAME = "default";
    private final RedisClient client ;
    public RedisJobQueue(RedisClient client){
        this.client =client;
    }


    @Override
    public void enqueue(JobId jobId, String queueName) {
        validateQueueName(queueName);
        validateJobId(jobId);
        String key=QUEUE_PREFIX+ queueName;
        try {
            client.lpush(key, String.valueOf(jobId));
        }catch (JedisException e){
            throw new QueueException("Job can not be added to queue",e);
        }

    }

    @Override
    public Optional<JobId> dequeue(String queueName, Duration timeout) {
        if(timeout==null||timeout.isNegative()|| timeout.isZero()){
            throw new IllegalArgumentException("Duration can not be zero, negative and null");
        }
        validateQueueName(queueName);
        String key=QUEUE_PREFIX+queueName;
        try {
            long timeoutSeconds = timeout.toSeconds();
            if (timeout.toMillis() % 1000 != 0) {
                timeoutSeconds++;
            }
            List<String> result=client.brpop((int)timeoutSeconds,key);
            if(result==null||result.isEmpty())return Optional.empty();
            String jobIdString = result.get(1);
            try {
                return Optional.of(JobId.fromString(jobIdString));
            } catch (IllegalArgumentException e) {
                throw new QueueException("Redis returned an invalid JobId", e);
            }
        }catch (JedisException e){
            throw new QueueException("Job can not be removed from queue",e);
        }

    }

    @Override
    public Optional<JobId> dequeueNonBlocking(String queueName) {
        validateQueueName(queueName);
        String key=QUEUE_PREFIX+queueName;
        try {
            String jobId=client.rpop(key);
            if(jobId==null)return Optional.empty();
            try {
                return Optional.of(JobId.fromString(jobId));
            } catch (IllegalArgumentException e) {
                throw new QueueException("Redis returned an invalid JobId", e);
            }
        }catch (JedisException e){
            throw new QueueException("Job cannot be removed from queue",e);
        }
    }

    @Override
    public long size(String queueName) {
        validateQueueName(queueName);
        String key=QUEUE_PREFIX+queueName;
        try {
            return client.llen(key);
        }catch (JedisException e){
            throw new QueueException("Failed to get queue size",e);
        }
    }

    @Override
    public List<JobId> peek(String queueName, int count) {
        validateQueueName(queueName);
        if(count<0)throw new IllegalArgumentException("Count can not be negative");
        if (count == 0) {
            return List.of();
        }
        String key=QUEUE_PREFIX+queueName;
        try {
            List<String> result=client.lrange(key,0,count-1);
            List<JobId> ids=new ArrayList<>();
            try {
                for(String s:result){
                    JobId jobId=JobId.fromString(s);
                    ids.add(jobId);
                }
                return ids;
            } catch (IllegalArgumentException e) {
                throw new QueueException("Redis returned an invalid JobId", e);
            }
        }catch (JedisException e){
            throw new QueueException("Failed to peek queue",e);
        }
    }

    private void validateQueueName(String queueName){
        if(queueName==null)throw new IllegalArgumentException("Queue name can not be null");
        if(queueName.isBlank())throw new IllegalArgumentException("Queue name can not be empty");

    }
    private void validateJobId(JobId jobId){
        if(jobId==null)throw new IllegalArgumentException("Job id can not be null");
    }
}
