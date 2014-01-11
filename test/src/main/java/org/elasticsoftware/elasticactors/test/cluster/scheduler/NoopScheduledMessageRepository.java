package org.elasticsoftware.elasticactors.test.cluster.scheduler;

import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.cluster.scheduler.ScheduledMessage;
import org.elasticsoftware.elasticactors.cluster.scheduler.ScheduledMessageRepository;

import java.util.Collections;
import java.util.List;

/**
 * @author Joost van de Wijgerd
 */
public final class NoopScheduledMessageRepository implements ScheduledMessageRepository {
    @Override
    public void create(ShardKey shardKey, ScheduledMessage scheduledMessage) {
        // do nothing
    }

    @Override
    public void delete(ShardKey shardKey, ScheduledMessage scheduledMessage) {
        // do nothing
    }

    @Override
    public List<ScheduledMessage> getAll(ShardKey shardKey) {
        return Collections.emptyList();
    }
}
