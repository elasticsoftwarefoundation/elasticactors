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

import org.elasticsoftware.elasticactors.*;
import org.reactivestreams.Publisher;

/**
 * {@link org.elasticsoftware.elasticactors.ActorRef} that references an actor in the local cluster
 *
 * @author  Joost van de Wijgerd
 */
public final class ServiceActorRef extends BaseActorRef implements ActorContainerRef {
    private final ActorNode node;

    public ServiceActorRef(InternalActorSystem actorSystem, String clusterName, ActorNode node, String serviceId) {
        super(actorSystem, clusterName, serviceId, generateRefSpec(clusterName, node, serviceId));
        this.node = node;
    }

    public static String generateRefSpec(String clusterName,ActorNode node,String serviceId) {
        return "actor://" + clusterName + "/" + node.getKey().getActorSystemName() + "/services/" + node.getKey().getNodeId() + "/" + serviceId;
    }


    @Override
    public String getActorPath() {
        return node.getKey().getActorSystemName() + "/services/" + node.getKey().getNodeId();
    }

    @Override
    public void tell(Object message, ActorRef sender) {
        try {
            node.sendMessage(sender,this,message);
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
            throw new IllegalStateException("Cannot determine ActorRef(self). Only use this method while inside an ElasticActor Lifecycle or on(Message) method!");
        }
    }

    @Override
    public <T> Publisher<T> publisherOf(Class<T> messageClass) {
        if(ActorContextHolder.hasActorContext()) {
            // since we are executing within the context of an actor, sending the exception to the subscriber would be
            // silently swallowed
            throw new UnsupportedOperationException("Services cannot be used as Publisher in the current implementation");
        } else {
            // notify the subscriber
            return s -> s.onError(new UnsupportedOperationException("Services cannot be used as Publisher in the current implementation"));
        }
    }

    @Override
    public boolean isLocal() {
        return node.isLocal();
    }

    @Override
    public ActorContainer getActorContainer() {
        return node;
    }
    
}
