/*
 * Copyright 2013 the original authors
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

import org.elasticsoftware.elasticactors.ActorNode;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.NodeKey;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.InternalMessageImpl;
import org.elasticsoftware.elasticactors.messaging.MessageHandlerEventListener;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactory;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;

/**
 * @author Joost van de Wijgerd
 */

public final class RemoteActorNode extends AbstractActorContainer implements ActorNode {
    private final InternalActorSystem actorSystem;
    private final NodeKey nodeKey;

    public RemoteActorNode(PhysicalNode remoteNode,
                           InternalActorSystem actorSystem,
                           ActorRef myRef,
                           MessageQueueFactory messageQueueFactory) {
        super(messageQueueFactory,myRef,remoteNode);
        this.actorSystem = actorSystem;
        this.nodeKey = new NodeKey(actorSystem.getName(), remoteNode.getId());
    }

    @Override
    public NodeKey getKey() {
        return nodeKey;
    }

    public void sendMessage(ActorRef from, ActorRef to, Object message) throws Exception {
        MessageSerializer messageSerializer = actorSystem.getSerializer(message.getClass());
        messageQueue.offer(new InternalMessageImpl(from, to, messageSerializer.serialize(message),
                                                   message.getClass().getName()));
    }

    @Override
    public void undeliverableMessage(InternalMessage message) throws Exception {
        // input is the message that cannot be delivered
        InternalMessageImpl undeliverableMessage = new InternalMessageImpl(message.getReceiver(),
                                                                           message.getSender(),
                                                                           message.getPayload(),
                                                                           message.getPayloadClass(),
                                                                           true,
                                                                           true);
        messageQueue.offer(undeliverableMessage);
    }

    @Override
    public void handleMessage(InternalMessage message, MessageHandlerEventListener messageHandlerEventListener) {
        // nothing to do
    }
}
