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

package org.elasticsoftware.elasticactors.client;

import com.google.common.collect.ImmutableList;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorShard;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.InternalMessageImpl;
import org.elasticsoftware.elasticactors.messaging.MessageHandler;
import org.elasticsoftware.elasticactors.messaging.MessageHandlerEventListener;
import org.elasticsoftware.elasticactors.messaging.MessageQueue;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactory;
import org.elasticsoftware.elasticactors.serialization.Message;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;
import org.elasticsoftware.elasticactors.serialization.SerializationContext;
import org.elasticsoftware.elasticactors.serialization.SerializationFramework;

import java.util.List;

final class ClientActorShard implements ActorShard, MessageHandler {

    private static final String HANDLE_MESSAGES_ERROR =
            "Client message handlers can't handle messages";
    private final ShardKey key;
    private final String actorPath;
    private final MessageQueueFactory messageQueueFactory;
    private final SerializationFrameworkCache serializationFrameworkCache;

    private MessageQueue messageQueue;

    ClientActorShard(
            ShardKey key,
            MessageQueueFactory messageQueueFactory,
            SerializationFrameworkCache serializationFrameworkCache) {
        this.key = key;
        this.messageQueueFactory = messageQueueFactory;
        this.serializationFrameworkCache = serializationFrameworkCache;
        this.actorPath = String.format("%s/shards/%d", key.getActorSystemName(), key.getShardId());
    }

    @Override
    public ShardKey getKey() {
        return key;
    }

    @Override
    public PhysicalNode getOwningNode() {
        throw new UnsupportedOperationException("Client actor shards are not owned by any nodes");
    }

    @Override
    public ActorRef getActorRef() {
        throw new UnsupportedOperationException("Client actor shards can't receive messages");
    }

    /**
     * A specialization of {@link ActorShard#sendMessage(ActorRef, ActorRef, Object)} for a
     * ClientActorShard. The sender is always ignored.
     */
    @Override
    public void sendMessage(ActorRef sender, ActorRef receiver, Object message) throws Exception {
        sendMessage(sender, ImmutableList.of(receiver), message);
    }

    /**
     * A specialization of {@link ActorShard#sendMessage(ActorRef, List, Object)} for a
     * ClientActorShard. The sender is always ignored.
     */
    @Override
    public void sendMessage(
            ActorRef sender, List<? extends ActorRef> receiver, Object message) throws Exception {
        MessageSerializer messageSerializer = getSerializer(message.getClass());
        // get the durable flag
        Message messageAnnotation = message.getClass().getAnnotation(Message.class);
        final boolean durable = (messageAnnotation == null) || messageAnnotation.durable();
        final int timeout =
                (messageAnnotation != null) ? messageAnnotation.timeout() : Message.NO_TIMEOUT;
        messageQueue.offer(new InternalMessageImpl(
                null,
                ImmutableList.copyOf(receiver),
                SerializationContext.serialize(messageSerializer, message),
                message.getClass().getName(),
                durable,
                timeout));
    }

    private <T> MessageSerializer<T> getSerializer(Class<T> messageClass) {
        Message messageAnnotation = messageClass.getAnnotation(Message.class);
        if (messageAnnotation != null) {
            SerializationFramework framework = serializationFrameworkCache
                    .getSerializationFramework(messageAnnotation.serializationFramework());
            return framework.getSerializer(messageClass);
        }
        return null;
    }

    @Override
    public void undeliverableMessage(
            InternalMessage undeliverableMessage, ActorRef receiverRef) throws Exception {
        throw new UnsupportedOperationException("Client actor shards can't receive responses");
    }

    @Override
    public void offerInternalMessage(InternalMessage message) {
        messageQueue.add(message);
    }

    @Override
    public PhysicalNode getPhysicalNode() {
        throw new UnsupportedOperationException(
                "Client message handlers are not associated to a physical node");
    }

    @Override
    public void handleMessage(InternalMessage message, MessageHandlerEventListener mhel) {
        mhel.onError(message, new UnsupportedOperationException(HANDLE_MESSAGES_ERROR));
    }

    @Override
    public void init() throws Exception {
        this.messageQueue = messageQueueFactory.create(actorPath, this);
    }

    @Override
    public void destroy() {
        this.messageQueue.destroy();
    }
}
