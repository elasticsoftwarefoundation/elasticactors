package org.elasticsoftware.elasticactors.cluster.scheduler;

import org.elasticsoftware.elasticactors.ShardKey;

import java.util.List;

/**
 * @author Joost van de Wijgerd
 */
public interface ScheduledMessageRepository {
    void create(ShardKey shardKey, ScheduledMessage scheduledMessage);

    void delete(ShardKey shardKey, ScheduledMessage scheduledMessage);

    List<ScheduledMessage> getAll(ShardKey shardKey);
}
