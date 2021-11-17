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
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorShard;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.cluster.ActorShardRef;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.cluster.scheduler.ScheduledMessage;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.InternalMessageFactory;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public final class KafkaActorShard implements ActorShard {
    private final ShardKey key;
    private final ActorRef myRef;
    private final AtomicReference<PhysicalNode> owningNode = new AtomicReference<>(null);
    private final KafkaActorThread actorThread;
    private final InternalActorSystem actorSystem;

    public KafkaActorShard(ShardKey key, KafkaActorThread actorThread, InternalActorSystem actorSystem) {
        this.key = key;
        this.myRef = new ActorShardRef(actorSystem, actorSystem.getParent().getClusterName(), this);
        this.actorThread = actorThread;
        this.actorSystem = actorSystem;
        // assign this shard to the thread
        this.actorThread.assign(this);
    }

    @Override
    public ShardKey getKey() {
        return key;
    }

    @Override
    public PhysicalNode getOwningNode() {
        return owningNode.get();
    }

    public void setOwningNode(PhysicalNode owningNode) {
        this.owningNode.set(owningNode);
    }

    @Override
    public ActorRef getActorRef() {
        return myRef;
    }

    @Override
    public void sendMessage(ActorRef sender, ActorRef receiver, Object message) throws Exception {
        sendMessage(sender, ImmutableList.of(receiver), message);
    }

    @Override
    public void sendMessage(ActorRef sender, List<? extends ActorRef> receivers, Object message) throws Exception {
        offerInternalMessage(createInternalMessage(sender, receivers, message));
    }

    @Override
    public void undeliverableMessage(InternalMessage message, ActorRef receiverRef) throws Exception {
        InternalMessage undeliverableMessage =
            InternalMessageFactory.copyForUndeliverableWithSerializedPayload(message, receiverRef);
        offerInternalMessage(undeliverableMessage);
    }

    @Override
    public void offerInternalMessage(InternalMessage internalMessage) {
        // the calling thread can be a KafkaActorThread (meaning this is called as a side effect of handling another message)
        // or it is called from another thread in which case it is not part of an existing transaction
        actorThread.send(key, internalMessage);
    }

    public void schedule(ScheduledMessage scheduledMessage) {
        // forward this to the ActorThread
        actorThread.schedule(key, scheduledMessage);
    }

    KafkaActorThread getActorThread() {
        return actorThread;
    }

    @Override
    public void init() throws Exception {

    }

    @Override
    public void destroy() {

    }

    private InternalMessage createInternalMessage(ActorRef from, List<? extends ActorRef> to, Object message) throws IOException {
        MessageSerializer<Object> messageSerializer = (MessageSerializer<Object>) actorSystem.getSerializer(message.getClass());
        return InternalMessageFactory.createWithSerializedPayload(
            from,
            to,
            messageSerializer,
            message
        );
    }
}
