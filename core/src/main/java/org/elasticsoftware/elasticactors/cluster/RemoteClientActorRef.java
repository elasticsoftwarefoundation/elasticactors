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
 * {@link ActorRef} that references a (temp)actor in a remote client
 *
 * @author  Joost van de Wijgerd
 */
public final class RemoteClientActorRef extends BaseActorRef implements ActorContainerRef {
    private final ClientNode clientContainer;
    private final String actorSystemName;

    public RemoteClientActorRef(InternalActorSystem actorSystem,
                                String clusterName,
                                String actorSystemName,
                                ClientNode clientContainer,
                                String actorId) {
        super(actorSystem, clusterName, actorId, generateRefSpec(clusterName, actorSystemName, clientContainer.getNodeId(), actorId));
        this.clientContainer = clientContainer;
        this.actorSystemName = actorSystemName;
    }

    public static String generateRefSpec(String clusterName, String actorSystemName, String nodeId, String actorId) {
        if(actorId != null) {
            return String.format("actor://%s/%s/clients/%s/%s",
                    clusterName, actorSystemName,
                    nodeId,actorId);
        } else {
            return String.format("actor://%s/%s/clients/%s",
                    clusterName, actorSystemName, nodeId);
        }
    }


    @Override
    public String getActorPath() {
        return String.format("%s/clients/%s", actorSystemName, clientContainer.getNodeId());
    }


    @Override
    public void tell(Object message, ActorRef sender) {
        try {
            clientContainer.sendMessage(sender, clientContainer.getActorRef(), message);
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
        return clientContainer;
    }

    @Override
    public <T> Publisher<T> publisherOf(Class<T> messageClass) {
        return s -> s.onError(new UnsupportedOperationException("RemoteClusterActorNodeRef cannot be used to obtain a reference to the remote ActorNode"));
    }
}
