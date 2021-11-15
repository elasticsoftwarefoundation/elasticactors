/*
 *   Copyright 2013 - 2019 The Original Authors
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

package org.elasticsoftware.elasticactors.cluster;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorShard;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.messaging.internal.InternalHashKeyUtils;
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author Joost van de Wijgerd
 */
public final class ActorSystemEventRegistryImpl implements ActorSystemEventListenerService {
    private static final Logger logger = LoggerFactory.getLogger(ActorSystemEventRegistryImpl.class);
    private ActorSystemEventListenerRepository eventListenerRepository;
    private InternalActorSystem actorSystem;

    public ActorSystemEventRegistryImpl() {
    }

    @Inject
    public void setEventListenerRepository(ActorSystemEventListenerRepository eventListenerRepository) {
        this.eventListenerRepository = eventListenerRepository;
    }

    @Inject
    public void setActorSystem(InternalActorSystem actorSystem) {
        this.actorSystem = actorSystem;
    }

    @Override
    public void register(ActorRef receiver, ActorSystemEvent event, Object message) throws IOException {
        if(!(receiver instanceof ActorShardRef)) {
            throw new IllegalArgumentException("ActorRef must be referencing a Persistent Actor (i.e. annotated with @Actor)");
        }
        // ugly casting needed here
        ShardKey shardKey = ((ActorShard)((ActorShardRef) receiver).getActorContainer()).getKey();
        // store the reference
        MessageSerializer serializer = actorSystem.getSerializer(message.getClass());
        ByteBuffer serializedMessage = serializer.serialize(message);
        eventListenerRepository.create(
            shardKey,
            event,
            new ActorSystemEventListenerImpl(
                receiver.getActorId(),
                message.getClass(),
                serializedMessage,
                InternalHashKeyUtils.getMessageQueueAffinityKey(message)
            )
        );
    }

    @Override
    public void deregister(ActorRef receiver, ActorSystemEvent event) {
        if(!(receiver instanceof ActorShardRef)) {
            throw new IllegalArgumentException("ActorRef must be referencing a Persistent Actor (i.e. annotated with @Actor)");
        }
        // ugly casting needed here
        ShardKey shardKey = ((ActorShard)((ActorShardRef) receiver).getActorContainer()).getKey();
        eventListenerRepository.delete(shardKey, event, receiver);
    }

    @Override
    public void generateEvents(ActorShard actorShard, ActorSystemEvent actorSystemEvent) {
        List<ActorSystemEventListener> listeners = eventListenerRepository.getAll(actorShard.getKey(), actorSystemEvent);
        for (ActorSystemEventListener listener : listeners) {
            MessageDeserializer deserializer = actorSystem.getDeserializer(listener.getMessageClass());
            if(deserializer != null) {
                try {
                    Object message = deserializer.deserialize(listener.getMessageBytes());
                    ActorRef receiver = actorSystem.actorFor(listener.getActorId());
                    actorShard.sendMessage(null, receiver, message);
                } catch(Exception e) {
                    logger.error("Exception while sending message [{}] to actorId [{}] for ActorSystemEvent.{}",listener.getMessageClass().getName(),listener.getActorId(),actorSystemEvent.name(),e);
                }
            }
        }
    }

}
