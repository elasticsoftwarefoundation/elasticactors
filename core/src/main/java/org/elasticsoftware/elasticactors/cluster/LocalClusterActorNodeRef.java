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

/**
 * {@link org.elasticsoftware.elasticactors.ActorRef} that references an actor in the local cluster
 *
 * @author  Joost van de Wijgerd
 */
public final class LocalClusterActorNodeRef extends AbstractActorRef implements ActorContainerRef {
    private final String clusterName;
    private final ActorNode node;
    private final String actorId;
    private final String refSpec;

    public LocalClusterActorNodeRef(InternalActorSystem actorSystem, String clusterName, ActorNode node) {
        this(actorSystem, clusterName, node, null);
    }

    public LocalClusterActorNodeRef(InternalActorSystem actorSystem, String clusterName, ActorNode node, String actorId) {
        super(actorSystem);
        this.clusterName = clusterName;
        this.node = node;
        this.actorId = actorId;
        this.refSpec = generateRefSpec(clusterName, node, actorId);
    }

    public static String generateRefSpec(String clusterName, ActorNode node,String actorId) {
        if(actorId != null) {
            return String.format("actor://%s/%s/nodes/%s/%s",
                    clusterName,node.getKey().getActorSystemName(),
                    node.getKey().getNodeId(),actorId);
        } else {
            return String.format("actor://%s/%s/nodes/%s",
                    clusterName,node.getKey().getActorSystemName(),
                    node.getKey().getNodeId());
        }
    }



    @Override
    public String getActorCluster() {
        return clusterName;
    }

    @Override
    public String getActorPath() {
        return String.format("%s/nodes/%s",node.getKey().getActorSystemName(),node.getKey().getNodeId());
    }

    public String getActorId() {
        return actorId;
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
            throw new IllegalStateException("Cannot determine ActorRef(self) Only use this method while inside an ElasticActor Lifecycle or on(Message) method!");
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

    @Override
    public boolean equals(Object o) {
        return this == o || o instanceof ActorRef && this.toString().equals(o.toString());
    }

    @Override
    public int hashCode() {
        return this.refSpec.hashCode();
    }

    @Override
    public String toString() {
        return this.refSpec;
    }
}
