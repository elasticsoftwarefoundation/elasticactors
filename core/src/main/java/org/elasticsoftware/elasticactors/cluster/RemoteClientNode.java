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
import org.elasticsoftware.elasticactors.*;
import org.elasticsoftware.elasticactors.messaging.*;
import org.elasticsoftware.elasticactors.serialization.Message;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;
import org.elasticsoftware.elasticactors.serialization.SerializationContext;

import java.util.List;

/**
 * @author Joost van de Wijgerd
 */

public final class RemoteClientNode implements ClientNode {
    private final InternalActorSystem actorSystem;
    private final String nodeId;
    private final MessageQueueFactory messageQueueFactory;
    private MessageQueue messageQueue;

    public RemoteClientNode(String nodeId,
                            InternalActorSystem actorSystem,
                            MessageQueueFactory messageQueueFactory) {
        this.messageQueueFactory = messageQueueFactory;
        this.actorSystem = actorSystem;
        this.nodeId = nodeId;
    }

    @Override
    public String getNodeId() {
        return nodeId;
    }

    @Override
    public ActorRef getActorRef() {
        return new RemoteClientActorRef(actorSystem, actorSystem.getParent().getClusterName(), actorSystem.getName(), this, null);
    }

    @Override
    public void sendMessage(ActorRef sender, ActorRef receiver, Object message) throws Exception {
        sendMessage(sender, ImmutableList.of(receiver), message);
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
    public void offerInternalMessage(InternalMessage message) {
        messageQueue.add(message);
    }

    @Override
    public void init() throws Exception {
        messageQueue = messageQueueFactory.create(getActorRef().getActorPath(), null);
    }

    @Override
    public void destroy() {
        messageQueue.destroy();
    }


}
