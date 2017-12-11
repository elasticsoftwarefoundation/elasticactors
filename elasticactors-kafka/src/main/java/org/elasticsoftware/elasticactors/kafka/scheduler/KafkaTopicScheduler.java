package org.elasticsoftware.elasticactors.kafka.scheduler;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.cluster.scheduler.InternalScheduler;
import org.elasticsoftware.elasticactors.cluster.scheduler.ScheduledMessageKey;
import org.elasticsoftware.elasticactors.scheduler.ScheduledMessageRef;
import org.elasticsoftware.elasticactors.scheduler.Scheduler;

import java.util.concurrent.TimeUnit;

public final class KafkaTopicScheduler implements Scheduler, InternalScheduler {
    @Override
    public ScheduledMessageRef scheduleOnce(ActorRef sender, Object message, ActorRef receiver, long delay, TimeUnit timeUnit) {
        return null;
    }

    @Override
    public void cancel(ShardKey shardKey, ScheduledMessageKey messageKey) {

    }
}
