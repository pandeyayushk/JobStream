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
    private static final String SAVE_SCRIPT = """
            local current = redis.call('GET', KEYS[1])
            local oldStatus = nil
            local currentRevision = '0'
            if current then
                local ok, data = pcall(cjson.decode, current)
                if not ok or type(data) ~= 'table' then return -2 end
                oldStatus = data.status
                currentRevision = string.match(current, '.*"revision"%s*:%s*(%d+)%s*}%s*$') or '0'
                if currentRevision ~= ARGV[1] then return -1 end
            elseif ARGV[1] ~= '0' then
                return -1
            end

            local targetType = redis.call('TYPE', KEYS[2]).ok
            if targetType ~= 'none' and targetType ~= 'set' then return -2 end
            if oldStatus then
                local oldType = redis.call('TYPE', ARGV[4] .. oldStatus).ok
                if oldType ~= 'none' and oldType ~= 'set' then return -2 end
            end

            redis.call('SET', KEYS[1], ARGV[2])
            if oldStatus then redis.call('SREM', ARGV[4] .. oldStatus, ARGV[3]) end
            redis.call('SADD', KEYS[2], ARGV[3])
            return 1
            """;
    private static final String DELETE_SCRIPT = """
            local current = redis.call('GET', KEYS[1])
            if not current then return 0 end
            local ok, data = pcall(cjson.decode, current)
            if not ok or type(data) ~= 'table' or type(data.status) ~= 'string' then return -1 end
            local statusKey = ARGV[2] .. data.status
            local statusType = redis.call('TYPE', statusKey).ok
            if statusType ~= 'none' and statusType ~= 'set' then return -1 end
            redis.call('DEL', KEYS[1])
            redis.call('SREM', statusKey, ARGV[1])
            return 1
            """;
    public RedisJobRepository(RedisClient client, JobSerializer serializer){
        this.serializer = serializer;
        this.client = client;
    }




    @Override
    public Job save(Job job) {
        if (job == null) {
            throw new NullPointerException("Job cannot be null");
        }
        String key = JOB_KEY_PREFIX + job.id();
        try {
            long nextRevision;
            try {
                nextRevision = Math.addExact(job.revision(), 1);
            } catch (ArithmeticException e) {
                throw new PersistenceException("Job revision exhausted for " + job.id(), e);
            }

            Job persistedJob = job.withRevision(nextRevision);
            Object result = client.eval(
                    SAVE_SCRIPT,
                    List.of(key, STATUS_KEY_PREFIX + job.status()),
                    List.of(
                            Long.toString(job.revision()),
                            serializer.serialize(persistedJob),
                            job.id().toString(),
                            STATUS_KEY_PREFIX
                    )
            );
            long status = ((Number) result).longValue();
            if (status == -1) {
                throw new ConcurrentJobUpdateException(
                        "Job changed concurrently while saving " + job.id()
                );
            }
            if (status != 1) {
                throw new PersistenceException("Unable to save Job " + job.id());
            }
            return persistedJob;
        } catch (JedisException e) {
            throw new PersistenceException("Redis Exception", e);
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
        if (id == null) {
            throw new NullPointerException("JobId cannot be null");
        }
        String key = JOB_KEY_PREFIX + id;
        try {
            Object result = client.eval(
                    DELETE_SCRIPT,
                    List.of(key),
                    List.of(id.toString(), STATUS_KEY_PREFIX)
            );
            long status = ((Number) result).longValue();
            if (status == -1) {
                throw new PersistenceException("Unable to atomically delete Job " + id);
            }
            return status == 1;
        }catch (JedisException e){
            throw new PersistenceException("Redis Exception",e);
        }
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
