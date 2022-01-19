/*
 *   Copyright 2013 - 2022 The Original Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.elasticsoftware.elasticactors.kafka.scheduler;

import org.elasticsoftware.elasticactors.ActorContainer;
import org.elasticsoftware.elasticactors.ActorContainerRef;
import org.elasticsoftware.elasticactors.ActorContextHolder;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.cluster.scheduler.ScheduledMessage;
import org.elasticsoftware.elasticactors.cluster.scheduler.ScheduledMessageKey;
import org.elasticsoftware.elasticactors.cluster.scheduler.ScheduledMessageShardRef;
import org.elasticsoftware.elasticactors.kafka.KafkaActorShard;
import org.elasticsoftware.elasticactors.messaging.ScheduledMessageImpl;
import org.elasticsoftware.elasticactors.scheduler.ScheduledMessageRef;
import org.elasticsoftware.elasticactors.scheduler.Scheduler;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;

import java.nio.ByteBuffer;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

public final class KafkaTopicScheduler implements Scheduler {
    private final InternalActorSystem actorSystem;

    public KafkaTopicScheduler(InternalActorSystem internalActorSystem) {
        this.actorSystem = internalActorSystem;
    }


    @Override
    public ScheduledMessageRef scheduleOnce(
        Object message,
        ActorRef receiver,
        long delay,
        TimeUnit timeUnit)
    {
        // this method only works when sender is a local persistent actor (so no temp or service actor)
        ActorRef sender = ActorContextHolder.getSelf();
        if(sender instanceof ActorContainerRef) {
            ActorContainer actorContainer = ((ActorContainerRef)sender).getActorContainer();
            if(actorContainer instanceof KafkaActorShard) {
                KafkaActorShard actorShard = (KafkaActorShard) actorContainer;
                if(actorShard.getOwningNode() != null && actorShard.getOwningNode().isLocal()) {
                    // we're in business
                    try {
                        long fireTime = System.currentTimeMillis() + timeUnit.toMillis(delay);
                        MessageSerializer serializer = actorSystem.getSerializer(message.getClass());
                        ByteBuffer serializedMessage = serializer.serialize(message);
                        ScheduledMessage scheduledMessage = new ScheduledMessageImpl(
                            fireTime,
                            sender,
                            receiver,
                            message.getClass(),
                            serializedMessage,
                            message
                        );
                        actorShard.schedule(scheduledMessage);
                        return new ScheduledMessageShardRef(actorSystem.getParent().getClusterName(), actorShard, new ScheduledMessageKey(scheduledMessage.getId(),fireTime));
                    } catch(Exception e) {
                        throw new RejectedExecutionException(e);
                    }
                }
            }
        }
        throw new IllegalStateException(
            "Cannot determine an appropriate ActorRef(self). Only use this method while inside an "
                + "ElasticActor Lifecycle or on(Message) method on a Persistent Actor!");
    }

}
