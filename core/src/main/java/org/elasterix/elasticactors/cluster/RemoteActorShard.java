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
import org.elasterix.elasticactors.messaging.InternalMessageImpl;
import org.elasterix.elasticactors.messaging.MessageQueue;
import org.elasterix.elasticactors.messaging.MessageQueueFactory;
import org.elasterix.elasticactors.serialization.MessageSerializer;
import org.springframework.beans.factory.annotation.Configurable;

/**
 * @author Joost van de Wijgerd
 */
@Configurable
public class RemoteActorShard implements ActorShard {
    private final ActorSystem actorSystem;
    private final PhysicalNode remoteNode;
    private final ShardKey shardKey;
    private final MessageQueueFactory messageQueueFactory;
    private MessageQueue messageQueue;

    public RemoteActorShard(PhysicalNode remoteNode, ActorSystem actorSystem, int vNodeKey, MessageQueueFactory messageQueueFactory) {
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
        this.messageQueue = messageQueueFactory.create(shardKey.toString(),null);
    }

    @Override
    public void destroy() {
        this.messageQueue.destroy();
    }

    public void sendMessage(ActorRef from, ActorRef to, Object message) throws Exception {
        MessageSerializer<Object> messageSerializer = actorSystem.getSerializer(message.getClass());
        messageQueue.offer(new InternalMessageImpl(from, to, messageSerializer.serialize(message),
                                                   message.getClass().getName()));
    }


}
