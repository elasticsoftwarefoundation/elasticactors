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

package org.elasticsoftware.elasticactors.kafka.cluster;

import org.elasticsoftware.elasticactors.ActorContainer;
import org.elasticsoftware.elasticactors.ActorContainerRef;
import org.elasticsoftware.elasticactors.ActorContextHolder;
import org.elasticsoftware.elasticactors.ActorNode;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.MessageDeliveryException;
import org.elasticsoftware.elasticactors.cluster.BaseActorRef;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.kafka.KafkaActorNode;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;

import java.util.List;

/**
 * {@link ActorRef} that references an actor in the local cluster. This is a special Kafka specific implementation that
 * knows about the node topic partition (= KafkaActorThread) that runs the TempActor
 *
 * @author  Joost van de Wijgerd
 */
public final class LocalClusterPartitionedActorNodeRef extends BaseActorRef implements
        ActorContainerRef {
    private final KafkaActorNode node;
    private final int partition;
    private final ActorContainerWrapper actorContainer;

    public LocalClusterPartitionedActorNodeRef(InternalActorSystem actorSystem, String clusterName, KafkaActorNode node, int partition) {
        this(actorSystem, clusterName, node, partition, null);
    }

    public LocalClusterPartitionedActorNodeRef(InternalActorSystem actorSystem, String clusterName, KafkaActorNode node, int partition, String actorId) {
        super(actorSystem, clusterName, actorId, generateRefSpec(clusterName, node, partition, actorId));
        this.node = node;
        this.partition = partition;
        this.actorContainer = new ActorContainerWrapper();
    }

    public static String generateRefSpec(String clusterName, ActorNode node, int partition, String actorId) {
        if(actorId != null) {
            return "actor://" + clusterName + "/" + node.getKey().getActorSystemName() + "/nodes/" + node.getKey().getNodeId() + "/" + partition + "/" + actorId;
        } else {
            return "actor://" + clusterName + "/" + node.getKey().getActorSystemName() + "/nodes/" + node.getKey().getNodeId() + "/" + partition;
        }
    }

    @Override
    public String getActorPath() {
        return node.getKey().getActorSystemName() + "/nodes/" + node.getKey().getNodeId() + "/" + partition;
    }

    @Override
    public void tell(Object message, ActorRef sender) {
        try {
            node.sendMessage(sender,this, partition, message);
        } catch(MessageDeliveryException e) {
            throw e;
        } catch (Exception e) {
            throw new MessageDeliveryException("Unexpected Exception while sending message",e , false);
        }
    }

    @Override
    public void tell(Object message) {
        final ActorRef self = ActorContextHolder.getSelf();
        if(self != null) {
            tell(message,self);
        } else {
            throw new IllegalStateException("Cannot determine ActorRef(self). Only use this method while inside an ElasticActor Lifecycle or on(Message) method!");
        }
    }

    @Override
    public boolean isLocal() {
        return node.isLocal();
    }

    @Override
    public ActorContainer getActorContainer() {
        return actorContainer;
    }

    /**
     * We need this class in order to return the proper ActorRef (with the partition)
     *
     */
    private final class ActorContainerWrapper implements ActorContainer {
        @Override
        public ActorRef getActorRef() {
            return new LocalClusterPartitionedActorNodeRef(actorSystem, clusterName, node, partition);
        }

        @Override
        public void sendMessage(ActorRef sender, ActorRef receiver, Object message) throws Exception {
            node.sendMessage(sender, receiver, partition, message);
        }

        @Override
        public void sendMessage(ActorRef sender, List<? extends ActorRef> receiver, Object message) throws Exception {
            node.sendMessage(sender, receiver, partition, message);
        }

        @Override
        public void undeliverableMessage(InternalMessage undeliverableMessage, ActorRef receiverRef) throws Exception {
            node.undeliverableMessage(undeliverableMessage, receiverRef);
        }

        @Override
        public void offerInternalMessage(InternalMessage message) {
            node.offerInternalMessage(partition, message);
        }

        @Override
        public void init() throws Exception {
            // noop
        }

        @Override
        public void destroy() {
            // noop
        }
    }
}
