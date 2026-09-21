package io.github.pandeyayushk.jobstream.persistence;

import io.github.pandeyayushk.jobstream.job.Job;
import io.github.pandeyayushk.jobstream.job.JobId;
import io.github.pandeyayushk.jobstream.job.JobStatus;
import io.github.pandeyayushk.jobstream.serialization.JobSerializer;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.exceptions.JedisException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class RedisJobRepository implements JobRepository{
    private final JobSerializer serializer;
    private final RedisClient client;
    private static final String JOB_KEY_PREFIX = "jobstream:job:";
    private static final String STATUS_KEY_PREFIX = "jobstream:status:";
    public RedisJobRepository(RedisClient client, JobSerializer serializer){
        this.serializer = serializer;
        this.client = client;
    }




    @Override
    public void save(Job job) {
        String key=JOB_KEY_PREFIX+job.id().toString();
        String json=serializer.serialize(job);
        String status_key = STATUS_KEY_PREFIX + job.status();
        try{
            String temp = client.get(key);
            if (temp!=null) {
                Job restored = serializer.deserialize(temp);
                String oldStatusKey = STATUS_KEY_PREFIX + restored.status();
                if (!status_key.equals(oldStatusKey)) {
                    client.srem(oldStatusKey, job.id().toString());
                }
            }
            client.set(key, json);
            client.sadd(status_key, job.id().toString());
        }catch (JedisException e){
            throw new PersistenceException("Redis Exception",e);
        }
    }

    @Override
    public Optional<Job> findById (JobId id) {
        String key=JOB_KEY_PREFIX+id.toString();

        try{
            String json=client.get(key);
            if(json==null)return Optional.empty();
            Job job=serializer.deserialize(json);
            return Optional.of(job);

        }catch (JedisException e){
            throw new PersistenceException("Redis Exception",e);
        }
    }

    @Override
    public boolean delete(JobId id) {
        String key=JOB_KEY_PREFIX+id.toString();
        try {
            String temp=client.get(key);
            if(temp!=null){
                Job restored=serializer.deserialize(temp);
                String statusKey=STATUS_KEY_PREFIX+restored.status();
                client.del(key);
                client.srem(statusKey,restored.id().toString());
                return true;
            }
        }catch (JedisException e){
            throw new PersistenceException("Redis Exception",e);
        }
        return false;
    }

    @Override
    public List<Job> findByStatus(JobStatus status) {
        String statusKey = STATUS_KEY_PREFIX+status.name();

        try {
            Set<String> jobIds =client.smembers(statusKey);

            List<Job> jobs = new ArrayList<>();

            for (String jobId : jobIds) {
                String key = JOB_KEY_PREFIX+jobId;
                String json = client.get(key);

                if (json != null) {
                    Job job = serializer.deserialize(json);
                    jobs.add(job);
                }
            }

            return jobs;

        } catch (JedisException e) {
            throw new PersistenceException("Redis Exception", e);
        }
    }

    @Override
    public boolean exists(JobId id) {
        String key=JOB_KEY_PREFIX+id.toString();
        try{
            return client.exists(key);
        }catch (JedisException e){
            throw new PersistenceException("Redis Exception",e);
        }
    }
}
