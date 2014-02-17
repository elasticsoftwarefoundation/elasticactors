/*
 * Copyright 2013 - 2014 The Original Authors
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

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorShard;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.messaging.*;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;

/**
 * @author Joost van de Wijgerd
 */

public final class RemoteActorSystemActorShard implements ActorShard, MessageHandler {
    private static final PhysicalNode UNKNOWN_REMOTE_NODE = new PhysicalNodeImpl("UNKNOWN",null,false);
    private final InternalActorSystem actorSystem;
    private final ShardKey shardKey;
    private final MessageQueueFactory messageQueueFactory;
    private final ActorRef myRef;
    private MessageQueue messageQueue;

    public RemoteActorSystemActorShard(InternalActorSystem actorSystem,
                                       String remoteClusterName,
                                       String remoteActorSystemName,
                                       int vNodeKey,
                                       MessageQueueFactory messageQueueFactory) {
        this.actorSystem = actorSystem;
        this.shardKey = new ShardKey(remoteActorSystemName, vNodeKey);
        this.myRef = new ActorShardRef(remoteClusterName,this);
        this.messageQueueFactory = messageQueueFactory;
    }

    @Override
    public PhysicalNode getOwningNode() {
        return getPhysicalNode();
    }

    @Override
    public ShardKey getKey() {
        return shardKey;
    }

    @Override
    public ActorRef getActorRef() {
        return myRef;
    }

    public void sendMessage(ActorRef from, ActorRef to, Object message) throws Exception {
        MessageSerializer messageSerializer = actorSystem.getSerializer(message.getClass());
        messageQueue.offer(new InternalMessageImpl(from, to, messageSerializer.serialize(message),message.getClass().getName()));
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
    public void offerInternalMessage(InternalMessage message) {
        messageQueue.add(message);
    }

    @Override
    public void init() throws Exception {
        this.messageQueue = messageQueueFactory.create(myRef.getActorPath(),this);
    }

    @Override
    public void destroy() {
        this.messageQueue.destroy();
    }

    @Override
    public PhysicalNode getPhysicalNode() {
        return UNKNOWN_REMOTE_NODE;
    }

    @Override
    public void handleMessage(InternalMessage message, MessageHandlerEventListener messageHandlerEventListener) {
        // nothing to do
    }
}
