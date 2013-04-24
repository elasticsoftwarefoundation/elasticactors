/*
 * Copyright 2013 Joost van de Wijgerd
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

package org.elasterix.elasticactors.cluster;

import org.elasterix.elasticactors.ActorRef;
import org.elasterix.elasticactors.ActorShard;
import org.elasterix.elasticactors.PhysicalNode;
import org.elasterix.elasticactors.ShardKey;
import org.elasterix.elasticactors.messaging.InternalMessage;
import org.elasterix.elasticactors.messaging.InternalMessageImpl;
import org.elasterix.elasticactors.messaging.MessageHandlerEventListener;
import org.elasterix.elasticactors.messaging.MessageQueueFactory;
import org.elasterix.elasticactors.serialization.MessageSerializer;
import org.springframework.beans.factory.annotation.Configurable;

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
