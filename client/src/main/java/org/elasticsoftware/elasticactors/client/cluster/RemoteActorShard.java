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

package org.elasticsoftware.elasticactors.client.cluster;

import com.google.common.collect.ImmutableList;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorShard;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.client.messaging.ActorSystemMessage;
import org.elasticsoftware.elasticactors.client.serialization.ActorSystemMessageSerializer;
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
import org.elasticsoftware.elasticactors.serialization.SerializationFrameworks;

import java.util.List;

final class RemoteActorShard implements ActorShard, MessageHandler {

    private static final PhysicalNode UNKNOWN_REMOTE_NODE = new PhysicalNode("UNKNOWN", null, false);

    private final ShardKey key;
    private final String actorPath;
    private final MessageQueueFactory messageQueueFactory;
    private final SerializationFrameworks serializationFrameworks;
    private final ActorRef myRef;

    private MessageQueue messageQueue;

    RemoteActorShard(
            String clusterName,
            ShardKey key,
            MessageQueueFactory messageQueueFactory,
            SerializationFrameworks serializationFrameworks) {
        this.key = key;
        this.messageQueueFactory = messageQueueFactory;
        this.serializationFrameworks = serializationFrameworks;
        this.actorPath = String.format("%s/shards/%d", key.getActorSystemName(), key.getShardId());
        this.myRef = new RemoteActorShardRef(clusterName, this, null);
    }

    @Override
    public ShardKey getKey() {
        return key;
    }

    @Override
    public PhysicalNode getOwningNode() {
        return getPhysicalNode();
    }

    @Override
    public ActorRef getActorRef() {
        return myRef;
    }

    /**
     * A specialization of {@link ActorShard#sendMessage(ActorRef, ActorRef, Object)} for a
     * RemoteActorShard. <br><br>
     * <strong>
     * The sender is always ignored.
     * </strong>
     */
    @Override
    public void sendMessage(ActorRef sender, ActorRef receiver, Object message) throws Exception {
        sendMessage(sender, ImmutableList.of(receiver), message);
    }

    /**
     * A specialization of {@link ActorShard#sendMessage(ActorRef, List, Object)} for a
     * RemoteActorShard. <br><br>
     * <strong>
     * The sender is always ignored.
     * </strong>
     */
    @Override
    public void sendMessage(
            ActorRef sender, List<? extends ActorRef> receiver, Object message) throws Exception {
        MessageSerializer<?> messageSerializer = getSerializer(message.getClass());
        final boolean durable = isDurable(message);
        final int timeout = getTimeout(message);
        String payloadClass = getPayloadClass(message);
        messageQueue.offer(new InternalMessageImpl(
                null,
                ImmutableList.copyOf(receiver),
                SerializationContext.serialize(messageSerializer, message),
                payloadClass,
                durable,
                timeout));
    }

    private boolean isDurable(Object message) {
        Message messageAnnotation = message.getClass().getAnnotation(Message.class);
        if (messageAnnotation != null) {
            return messageAnnotation.durable();
        }
        if (message instanceof ActorSystemMessage) {
            return ((ActorSystemMessage) message).isDurable();
        }
        return true;
    }

    private int getTimeout(Object message) {
        Message messageAnnotation = message.getClass().getAnnotation(Message.class);
        if (messageAnnotation != null) {
            return messageAnnotation.timeout();
        }
        if (message instanceof ActorSystemMessage) {
            return ((ActorSystemMessage) message).getTimeout();
        }
        return Message.NO_TIMEOUT;
    }

    private String getPayloadClass(Object message) {
        Message messageAnnotation = message.getClass().getAnnotation(Message.class);
        if (messageAnnotation == null && message instanceof ActorSystemMessage) {
            return ((ActorSystemMessage) message).getPayloadClass();
        }
        return message.getClass().getName();
    }

    @SuppressWarnings("unchecked")
    private <T> MessageSerializer<T> getSerializer(Class<T> messageClass) {
        Message messageAnnotation = messageClass.getAnnotation(Message.class);
        if (messageAnnotation != null) {
            SerializationFramework framework = serializationFrameworks
                    .getSerializationFramework(messageAnnotation.serializationFramework());
            return framework.getSerializer(messageClass);
        }
        if (ActorSystemMessage.class.isAssignableFrom(messageClass)) {
            return (MessageSerializer<T>) ActorSystemMessageSerializer.get();
        }
        return null;
    }

    @Override
    public void undeliverableMessage(
            InternalMessage undeliverableMessage, ActorRef receiverRef) throws Exception {
        // Do nothing
    }

    @Override
    public void offerInternalMessage(InternalMessage message) {
        messageQueue.add(message);
    }

    @Override
    public PhysicalNode getPhysicalNode() {
        return UNKNOWN_REMOTE_NODE;
    }

    @Override
    public void handleMessage(InternalMessage message, MessageHandlerEventListener mhel) {
        mhel.onError(
                message,
                new UnsupportedOperationException("Remote ActorSystem shards can't handle "
                        + "messages"));
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
