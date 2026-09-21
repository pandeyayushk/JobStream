package io.github.pandeyayushk.jobstream.serialization;

import io.github.pandeyayushk.jobstream.job.Job;

public interface JobSerializer {

    String serialize(Job job) throws SerializationException;
    Job deserialize(String json) throws SerializationException;
}
