/*
 * Copyright 2013 - 2025 The Original Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package org.elasticsoftware.elasticactors.cluster;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorShard;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.InternalMessageFactory;
import org.elasticsoftware.elasticactors.messaging.MessageHandlerEventListener;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author Joost van de Wijgerd
 */

public final class RemoteActorShard extends AbstractActorContainer implements ActorShard {

    private final static Logger staticLogger = LoggerFactory.getLogger(RemoteActorShard.class);

    private final InternalActorSystem actorSystem;
    private final ShardKey shardKey;

    public RemoteActorShard(PhysicalNode remoteNode,
                            InternalActorSystem actorSystem,
                            int vNodeKey,
                            ActorRef myRef,
                            MessageQueueFactory messageQueueFactory) {
        super(
            messageQueueFactory,
            myRef,
            remoteNode,
            actorSystem.getQueuesPerShard(),
            actorSystem.getMultiQueueHashSeed()
        );
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

    @Override
    public void sendMessage(ActorRef from, List<? extends ActorRef> to, Object message) throws Exception {
        InternalMessage internalMessage =
            InternalMessageFactory.createWithSerializedPayload(from, to, actorSystem, message);
        offerInternalMessage(internalMessage);
    }

    @Override
    public void undeliverableMessage(InternalMessage message, ActorRef receiverRef) throws Exception {
        // input is the message that cannot be delivered
        InternalMessage undeliverableMessage =
            InternalMessageFactory.copyForUndeliverableWithSerializedPayload(message, receiverRef);
        offerInternalMessage(undeliverableMessage);
    }

    @Override
    public void handleMessage(InternalMessage message, MessageHandlerEventListener messageHandlerEventListener) {
        // nothing to do
    }

    @Override
    protected Logger initLogger() {
        return staticLogger;
    }
}
