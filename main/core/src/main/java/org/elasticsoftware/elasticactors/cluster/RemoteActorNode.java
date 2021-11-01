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

import com.google.common.collect.ImmutableList;
import org.elasticsoftware.elasticactors.ActorNode;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.NodeKey;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.messaging.DefaultInternalMessage;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.MessageHandlerEventListener;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactory;
import org.elasticsoftware.elasticactors.serialization.Message;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;
import org.elasticsoftware.elasticactors.serialization.SerializationContext;

import java.util.List;

/**
 * @author Joost van de Wijgerd
 */

public final class RemoteActorNode extends MultiQueueAbstractActorContainer implements ActorNode {
    private final InternalActorSystem actorSystem;
    private final NodeKey nodeKey;

    public RemoteActorNode(PhysicalNode remoteNode,
                           InternalActorSystem actorSystem,
                           ActorRef myRef,
                           MessageQueueFactory messageQueueFactory) {
        super(messageQueueFactory, myRef, remoteNode, actorSystem.getNumberOfNodeQueues(), false);
        this.actorSystem = actorSystem;
        this.nodeKey = new NodeKey(actorSystem.getName(), remoteNode.getId());
    }

    @Override
    public NodeKey getKey() {
        return nodeKey;
    }

    @Override
    public void sendMessage(ActorRef from, List<? extends ActorRef> to, Object message) throws Exception {
        MessageSerializer messageSerializer = actorSystem.getSerializer(message.getClass());
        // get the durable flag
        Message messageAnnotation = message.getClass().getAnnotation(Message.class);
        final boolean durable = (messageAnnotation == null) || messageAnnotation.durable();
        final int timeout = (messageAnnotation != null) ? messageAnnotation.timeout() : Message.NO_TIMEOUT;
        offerInternalMessage(new DefaultInternalMessage(
            from,
            ImmutableList.copyOf(to),
            SerializationContext.serialize(messageSerializer, message),
            message.getClass().getName(),
            durable,
            timeout
        ));
    }

    @Override
    public void undeliverableMessage(InternalMessage message, ActorRef receiverRef) throws Exception {
        // input is the message that cannot be delivered
        DefaultInternalMessage undeliverableMessage = new DefaultInternalMessage(receiverRef,
                                                                           message.getSender(),
                                                                           message.getPayload(),
                                                                           message.getPayloadClass(),
                                                                           message.isDurable(),
                                                                           true,
                                                                           message.getTimeout());
        offerInternalMessage(undeliverableMessage);
    }

    @Override
    public void handleMessage(InternalMessage message, MessageHandlerEventListener messageHandlerEventListener) {
        // nothing to do
    }

    @Override
    public boolean isLocal() {
        return false;
    }
}
