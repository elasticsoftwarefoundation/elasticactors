/*
 * Copyright 2013 - 2017 The Original Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsoftware.elasticactors.cluster;

import com.google.common.collect.ImmutableList;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorShard;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.InternalMessageImpl;
import org.elasticsoftware.elasticactors.messaging.MessageHandlerEventListener;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactory;
import org.elasticsoftware.elasticactors.serialization.Message;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;
import org.elasticsoftware.elasticactors.serialization.SerializationContext;

import java.util.List;

/**
 * @author Joost van de Wijgerd
 */

public final class RemoteActorShard extends AbstractActorContainer implements ActorShard {
    private final InternalActorSystem actorSystem;
    private final ShardKey shardKey;

    public RemoteActorShard(PhysicalNode remoteNode,
                            InternalActorSystem actorSystem,
                            int vNodeKey,
                            ActorRef myRef,
                            MessageQueueFactory messageQueueFactory) {
        super(messageQueueFactory,myRef,remoteNode);
        this.actorSystem = actorSystem;
        this.shardKey = new ShardKey(actorSystem.getName(), vNodeKey);
    }

    @Override
    public PhysicalNode getOwningNode() {
        return getPhysicalNode();
    }

    @Override
    public ShardKey getKey() {
        return shardKey;
    }

    public void sendMessage(ActorRef from, List<? extends ActorRef> to, Object message) throws Exception {
        MessageSerializer messageSerializer = actorSystem.getSerializer(message.getClass());
        // get the durable flag
        Message messageAnnotation = message.getClass().getAnnotation(Message.class);
        final boolean durable = (messageAnnotation == null) || messageAnnotation.durable();
        final int timeout = (messageAnnotation != null) ? messageAnnotation.timeout() : Message.NO_TIMEOUT;
        messageQueue.offer(new InternalMessageImpl(from, ImmutableList.copyOf(to), SerializationContext.serialize(messageSerializer,message),message.getClass().getName(),durable,timeout));
    }

    @Override
    public void undeliverableMessage(InternalMessage message, ActorRef receiverRef) throws Exception {
        // input is the message that cannot be delivered
        InternalMessageImpl undeliverableMessage = new InternalMessageImpl(receiverRef,
                                                                           message.getSender(),
                                                                           message.getPayload(),
                                                                           message.getPayloadClass(),
                                                                           message.isDurable(),
                                                                           true,
                                                                           message.getTimeout());
        messageQueue.offer(undeliverableMessage);
    }

    @Override
    public void handleMessage(InternalMessage message, MessageHandlerEventListener messageHandlerEventListener) {
        // nothing to do
    }
}
