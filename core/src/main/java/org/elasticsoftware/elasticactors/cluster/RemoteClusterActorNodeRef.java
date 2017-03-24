/*
 * Copyright 2013 - 2017 The Original Authors
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

import org.elasticsoftware.elasticactors.*;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.internal.ActorNodeMessage;
import org.elasticsoftware.elasticactors.util.SerializationTools;
import org.reactivestreams.Publisher;

import java.util.List;

/**
 * {@link org.elasticsoftware.elasticactors.ActorRef} that references an actor in the local cluster
 *
 * @author  Joost van de Wijgerd
 */
public final class RemoteClusterActorNodeRef extends BaseActorRef implements ActorContainerRef, ActorContainer {
    private final ActorShard delegatingShard;
    private final String nodeId;

    public RemoteClusterActorNodeRef(InternalActorSystem actorSystem, String clusterName, ActorShard delegatingShard, String nodeId) {
        this(actorSystem, clusterName, delegatingShard, nodeId, null);
    }

    public RemoteClusterActorNodeRef(InternalActorSystem actorSystem, String clusterName, ActorShard delegatingShard, String nodeId, String actorId) {
        super(actorSystem, clusterName, actorId, generateRefSpec(clusterName, delegatingShard, nodeId, actorId));
        this.delegatingShard = delegatingShard;
        this.nodeId = nodeId;
    }

    public static String generateRefSpec(String clusterName, ActorShard delegatingShard, String nodeId, String actorId) {
        if(actorId != null) {
            return String.format("actor://%s/%s/nodes/%s/%s",
                    clusterName,delegatingShard.getKey().getActorSystemName(),
                    nodeId,actorId);
        } else {
            return String.format("actor://%s/%s/nodes/%s",
                    clusterName,delegatingShard.getKey().getActorSystemName(),
                    nodeId);
        }
    }


    @Override
    public String getActorPath() {
        return String.format("%s/nodes/%s",delegatingShard.getKey().getActorSystemName(),nodeId);
    }


    @Override
    public void tell(Object message, ActorRef sender) {
        try {
            // we need to delegate this through the actor shard (and wrap the original message)
            ActorNodeMessage actorNodeMessage = new ActorNodeMessage(nodeId, this, message);
            delegatingShard.sendMessage(sender, delegatingShard.getActorRef(), actorNodeMessage);
        } catch(MessageDeliveryException e) {
            throw e;
        } catch (Exception e) {
            throw new MessageDeliveryException("Unexpected Exception while sending message",e,false);
        }
    }

    @Override
    public void tell(Object message) {
        final ActorRef self = ActorContextHolder.getSelf();
        if(self != null) {
            tell(message,self);
        } else {
            throw new IllegalStateException("Cannot determine ActorRef(self) Only use this method while inside an ElasticActor Lifecycle or on(Message) method!");
        }
    }

    @Override
    public boolean isLocal() {
        return false;
    }

    @Override
    public ActorContainer getActorContainer() {
        return this;
    }

    @Override
    public ActorRef getActorRef() {
        return delegatingShard.getActorRef();
    }

    @Override
    public void sendMessage(ActorRef sender, ActorRef receiver, Object message) throws Exception {
        throw new UnsupportedOperationException("RemoteClusterActorNodeRef cannot be used to obtain a reference to the remote ActorNode");
    }

    @Override
    public void sendMessage(ActorRef sender, List<? extends ActorRef> receiver, Object message) throws Exception {
        throw new UnsupportedOperationException("RemoteClusterActorNodeRef cannot be used to obtain a reference to the remote ActorNode");
    }

    @Override
    public void undeliverableMessage(InternalMessage undeliverableMessage, ActorRef receiverRef) throws Exception {
        // we need special handling here, the message needs to be sent back as a ActorNodeMessage in order to be
        // delivered correctly. To do this we need to deserialize the payload (and then serialize it again). It is a
        // bit cumbersome at the moment
        Object message = SerializationTools.deserializeMessage(actorSystem, undeliverableMessage);
        delegatingShard.sendMessage(receiverRef, delegatingShard.getActorRef(), new ActorNodeMessage(nodeId, undeliverableMessage.getSender(), message, true));
    }

    @Override
    public void offerInternalMessage(InternalMessage message) {
        throw new UnsupportedOperationException("RemoteClusterActorNodeRef cannot be used to obtain a reference to the remote ActorNode");
    }

    @Override
    public void init() throws Exception {
        // do nothing
    }

    @Override
    public void destroy() {
        // do nothing
    }

    @Override
    public <T> Publisher<T> publisherOf(Class<T> messageClass) {
        return s -> s.onError(new UnsupportedOperationException("RemoteClusterActorNodeRef cannot be used to obtain a reference to the remote ActorNode"));
    }
}
