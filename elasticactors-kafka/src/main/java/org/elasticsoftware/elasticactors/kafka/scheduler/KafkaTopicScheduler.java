package org.elasticsoftware.elasticactors.kafka.scheduler;

import org.elasticsoftware.elasticactors.*;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.cluster.scheduler.*;
import org.elasticsoftware.elasticactors.kafka.KafkaActorShard;
import org.elasticsoftware.elasticactors.scheduler.ScheduledMessageRef;
import org.elasticsoftware.elasticactors.scheduler.Scheduler;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;

import java.nio.ByteBuffer;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

public final class KafkaTopicScheduler implements Scheduler {
    private final InternalActorSystem actorSystem;

    public KafkaTopicScheduler(InternalActorSystem internalActorSystem) {
        this.actorSystem = internalActorSystem;
    }


    @Override
    public ScheduledMessageRef scheduleOnce(ActorRef sender, Object message, ActorRef receiver, long delay, TimeUnit timeUnit) {
        // this method only works when sender is a local persistent actor (so no temp or service actor)
        if(sender instanceof ActorContainerRef) {
            ActorContainer actorContainer = ((ActorContainerRef)sender).getActorContainer();
            if(actorContainer instanceof KafkaActorShard) {
                KafkaActorShard actorShard = (KafkaActorShard) actorContainer;
                if(actorShard.getOwningNode() != null && actorShard.getOwningNode().isLocal()) {
                    // we're in business
                    try {
                        long fireTime = System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(delay,timeUnit);
                        MessageSerializer serializer = actorSystem.getSerializer(message.getClass());
                        ByteBuffer serializedMessage = serializer.serialize(message);
                        byte[] serializedBytes = new byte[serializedMessage.remaining()];
                        serializedMessage.get(serializedBytes);
                        ScheduledMessage scheduledMessage = new ScheduledMessageImpl(fireTime, sender, receiver, message.getClass(), serializedBytes);
                        actorShard.schedule(scheduledMessage);
                        return new ScheduledMessageShardRef(actorSystem.getParent().getClusterName(), actorShard, new ScheduledMessageKey(scheduledMessage.getId(),fireTime));
                    } catch(Exception e) {
                        throw new RejectedExecutionException(e);
                    }
                }
            }
        }
        // sender param didn't fit the criteria
        throw new IllegalArgumentException(format("sender ref: %s needs to be a non-temp, non-service, locally sharded actor ref",(sender == null) ? "null" : sender.toString()));
    }

}
