package org.elasticsoftware.elasticactors.kafka.scheduler;

import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.cluster.scheduler.ScheduledMessage;
import org.elasticsoftware.elasticactors.cluster.scheduler.ScheduledMessageKey;
import org.elasticsoftware.elasticactors.cluster.scheduler.ScheduledMessageRepository;

import java.util.Collections;
import java.util.List;

public final class KafkaScheduledMessageRepository implements ScheduledMessageRepository {
    @Override
    public void create(ShardKey shardKey, ScheduledMessage scheduledMessage) {

    }

    @Override
    public void delete(ShardKey shardKey, ScheduledMessageKey scheduledMessage) {

    }

    @Override
    public List<ScheduledMessage> getAll(ShardKey shardKey) {
        return Collections.emptyList();
    }
}
