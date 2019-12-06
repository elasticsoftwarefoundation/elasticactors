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

package org.elasticsoftware.elasticactors.kafka;

import com.google.common.collect.ImmutableList;
import org.elasticsoftware.elasticactors.ActorNode;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.NodeKey;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.cluster.LocalClusterActorNodeRef;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.InternalMessageImpl;
import org.elasticsoftware.elasticactors.serialization.Message;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;
import org.elasticsoftware.elasticactors.serialization.SerializationAccessor;
import org.elasticsoftware.elasticactors.serialization.SerializationContext;

import java.io.IOException;
import java.util.List;

public final class KafkaActorNode implements ActorNode {
    private final NodeKey key;
    private final PhysicalNode node;
    private final KafkaActorThread actorThread;
    private final SerializationAccessor actorSystem;
    private final ActorRef myRef;

    public KafkaActorNode(PhysicalNode node, KafkaActorThread actorThread, InternalActorSystem actorSystem) {
        this.key = new NodeKey(actorSystem.getName(), node.getId());
        this.myRef = new LocalClusterActorNodeRef(actorSystem, actorSystem.getParent().getClusterName(), this);
        this.node = node;
        this.actorThread = actorThread;
        this.actorSystem = actorSystem;
        // if we are local, we need to ensure the actorThread will add our Topic
        if(node.isLocal()) {
            actorThread.assign(this, true);
        }
    }

    @Override
    public NodeKey getKey() {
        return key;
    }

    @Override
    public boolean isLocal() {
        return node.isLocal();
    }

    @Override
    public ActorRef getActorRef() {
        return myRef;
    }

    @Override
    public void sendMessage(ActorRef sender, ActorRef receiver, Object message) throws Exception {
        sendMessage(sender, ImmutableList.of(receiver), message);
    }

    public void sendMessage(ActorRef sender, ActorRef receiver, int partition, Object message) throws Exception {
        sendMessage(sender, ImmutableList.of(receiver), partition, message);
    }

    @Override
    public void sendMessage(ActorRef sender, List<? extends ActorRef> receivers, Object message) throws Exception {
        offerInternalMessage(createInternalMessage(sender, receivers, message));
    }

    public void sendMessage(ActorRef sender, List<? extends ActorRef> receivers, int partition, Object message) throws Exception {
        offerInternalMessage(partition, createInternalMessage(sender, receivers, message));
    }

    @Override
    public void undeliverableMessage(InternalMessage message, ActorRef receiverRef) throws Exception {
        InternalMessage undeliverableMessage = new InternalMessageImpl( receiverRef,
                message.getSender(),
                message.getPayload(),
                message.getPayloadClass(),
                message.isDurable(),
                true,
                message.getTimeout());
        offerInternalMessage(undeliverableMessage);
    }

    @Override
    public void offerInternalMessage(InternalMessage internalMessage) {
        // the calling thread can be a KafkaActorThread (meaning this is called as a side effect of handling another message)
        // or it is called from another thread in which case it is not part of an existing transaction
        // when the partition is not specified we use partition 0 as the default
        actorThread.send(key, 0, internalMessage);
    }

    public void offerInternalMessage(int partition, InternalMessage internalMessage) {
        actorThread.send(key, partition, internalMessage);
    }

    @Override
    public void init() throws Exception {

    }

    @Override
    public void destroy() {

    }

    KafkaActorThread getActorThread() {
        return actorThread;
    }

    void initializeServiceActors() {
        this.actorThread.initializeServiceActors();
    }

    private InternalMessage createInternalMessage(ActorRef from, List<? extends ActorRef> to, Object message) throws IOException {
        MessageSerializer<Object> messageSerializer = (MessageSerializer<Object>) actorSystem.getSerializer(message.getClass());
        if(messageSerializer == null) {
            throw new IllegalArgumentException("MessageSerializer not found for message of type "+message.getClass().getName());
        }
        // get the durable flag
        Message messageAnnotation = message.getClass().getAnnotation(Message.class);
        final boolean durable = (messageAnnotation != null) && messageAnnotation.durable();
        final int timeout = (messageAnnotation != null) ? messageAnnotation.timeout() : Message.NO_TIMEOUT;
        return new InternalMessageImpl(from, ImmutableList.copyOf(to), SerializationContext.serialize(messageSerializer, message),message.getClass().getName(),durable, timeout);
    }
}
