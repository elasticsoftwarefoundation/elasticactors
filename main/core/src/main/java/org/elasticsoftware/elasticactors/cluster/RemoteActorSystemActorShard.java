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
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorShard;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.InternalMessageFactory;
import org.elasticsoftware.elasticactors.messaging.MessageHandler;
import org.elasticsoftware.elasticactors.messaging.MessageHandlerEventListener;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactory;
import org.elasticsoftware.elasticactors.messaging.MessageQueueProxy;
import org.elasticsoftware.elasticactors.messaging.MultiMessageQueueProxy;
import org.elasticsoftware.elasticactors.messaging.MultiMessageQueueProxyHasher;
import org.elasticsoftware.elasticactors.messaging.SingleMessageQueueProxy;
import org.elasticsoftware.elasticactors.serialization.Message;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;
import org.elasticsoftware.elasticactors.serialization.SerializationFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author Joost van de Wijgerd
 */

public final class RemoteActorSystemActorShard implements ActorShard, MessageHandler {

    private final static Logger logger = LoggerFactory.getLogger(RemoteActorSystemActorShard.class);

    private static final PhysicalNode UNKNOWN_REMOTE_NODE = new PhysicalNode("UNKNOWN", null, false);
    private final InternalActorSystems actorSystems;
    private final ShardKey shardKey;
    private final MessageQueueProxy messageQueueProxy;
    private final ActorRef myRef;

    public RemoteActorSystemActorShard(
        InternalActorSystems actorSystems,
        String remoteClusterName,
        String remoteActorSystemName,
        int vNodeKey,
        MessageQueueFactory messageQueueFactory,
        int numberOfQueues,
        int multiQueueHashSeed)
    {
        this.actorSystems = actorSystems;
        this.shardKey = new ShardKey(remoteActorSystemName, vNodeKey);
        this.myRef = new ActorShardRef(actorSystems.get(null), remoteClusterName, this);
        this.messageQueueProxy = numberOfQueues <= 1
            ? new SingleMessageQueueProxy(messageQueueFactory, this, myRef)
            : new MultiMessageQueueProxy(
                new MultiMessageQueueProxyHasher(multiQueueHashSeed),
                messageQueueFactory,
                this,
                myRef,
                numberOfQueues
            );
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

    @Override
    public void sendMessage(ActorRef from, ActorRef to, Object message) throws Exception {
        sendMessage(from, ImmutableList.of(to), message);
    }

    @Override
    public void sendMessage(ActorRef from, List<? extends ActorRef> to, Object message) throws Exception {
        MessageSerializer messageSerializer = getSerializer(message.getClass());
        InternalMessage internalMessage = InternalMessageFactory.createWithSerializedPayload(
            from,
            to,
            messageSerializer,
            message
        );
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
    public void offerInternalMessage(InternalMessage message) {
        messageQueueProxy.offerInternalMessage(message);
    }

    @Override
    public void init() throws Exception {
        logger.info("Initializing Remote Actor Shard [{}]", shardKey);
        messageQueueProxy.init();
    }

    @Override
    public void destroy() {
        logger.info("Destroying Local Actor Shard [{}]", shardKey);
        messageQueueProxy.destroy();
    }

    @Override
    public PhysicalNode getPhysicalNode() {
        return UNKNOWN_REMOTE_NODE;
    }

    @Override
    public void handleMessage(InternalMessage message, MessageHandlerEventListener messageHandlerEventListener) {
        // nothing to do
    }

    private <T> MessageSerializer<T> getSerializer(Class<T> messageClass) {
        MessageSerializer<T> messageSerializer = actorSystems.getSystemMessageSerializer(messageClass);
        if(messageSerializer == null) {
            Message messageAnnotation = messageClass.getAnnotation(Message.class);
            if(messageAnnotation != null) {
                SerializationFramework framework = actorSystems.getSerializationFramework(messageAnnotation.serializationFramework());
                messageSerializer = framework.getSerializer(messageClass);
            }
        }
        return messageSerializer;
    }
}
