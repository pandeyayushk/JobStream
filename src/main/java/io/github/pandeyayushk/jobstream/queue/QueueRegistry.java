package io.github.pandeyayushk.jobstream.queue;

import java.util.List;

/** Discovers named active queues independently of their current depth. */
public interface QueueRegistry {
    void registerQueue(String queueName);

    List<String> listQueues();
}
