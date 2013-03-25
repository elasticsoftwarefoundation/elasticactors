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

import org.elasterix.elasticactors.*;
import org.elasterix.elasticactors.messaging.*;
import org.elasterix.elasticactors.serialization.MessageSerializer;
import org.springframework.beans.factory.annotation.Configurable;

/**
 * @author Joost van de Wijgerd
 */
@Configurable
public final class RemoteActorShard implements ActorShard, MessageHandler {
    private final InternalActorSystem actorSystem;
    private final PhysicalNode remoteNode;
    private final ShardKey shardKey;
    private final MessageQueueFactory messageQueueFactory;
    private MessageQueue messageQueue;

    public RemoteActorShard(PhysicalNode remoteNode, InternalActorSystem actorSystem, int vNodeKey, MessageQueueFactory messageQueueFactory) {
        this.actorSystem = actorSystem;
        this.remoteNode = remoteNode;
        this.messageQueueFactory = messageQueueFactory;
        this.shardKey = new ShardKey(actorSystem.getName(), vNodeKey);
    }

    @Override
    public PhysicalNode getOwningNode() {
        return remoteNode;
    }

    @Override
    public ShardKey getKey() {
        return shardKey;
    }

    @Override
    public void init() throws Exception {
        this.messageQueue = messageQueueFactory.create(shardKey.toString(),this);
    }

    @Override
    public void destroy() {
        this.messageQueue.destroy();
    }

    @Override
    public ActorRef getActorRef() {
        throw new UnsupportedOperationException(String.format("Not meant to be called directly on %s",getClass().getSimpleName()));
    }

    public void sendMessage(ActorRef from, ActorRef to, Object message) throws Exception {
        MessageSerializer messageSerializer = actorSystem.getSerializer(message.getClass());
        messageQueue.offer(new InternalMessageImpl(from, to, messageSerializer.serialize(message),
                                                   message.getClass().getName()));
    }

    @Override
    public void offerInternalMessage(InternalMessage message) {
        // @todo: on scale out / fail over this will send the message to another node, the
        // messageQueue.add(message);
        // decide to do nothing for now
    }

    @Override
    public PhysicalNode getPhysicalNode() {
        return remoteNode;
    }

    @Override
    public void handleMessage(InternalMessage message, MessageHandlerEventListener messageHandlerEventListener) {
        // nothing to do
    }
}
