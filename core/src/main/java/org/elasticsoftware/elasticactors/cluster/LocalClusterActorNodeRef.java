/*
 * Copyright 2013 eBuddy BV
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

import org.apache.log4j.Logger;
import org.elasticsoftware.elasticactors.*;

/**
 * {@link org.elasticsoftware.elasticactors.ActorRef} that references an actor in the local cluster
 *
 * @author  Joost van de Wijgerd
 */
public final class LocalClusterActorNodeRef implements ActorRef, ActorContainerRef {
    private static final Logger logger = Logger.getLogger(LocalClusterActorNodeRef.class);
    private final String clusterName;
    private final ActorNode node;
    private final String actorId;

    public LocalClusterActorNodeRef(String clusterName, ActorNode node, String actorId) {
        this.clusterName = clusterName;
        this.node = node;
        this.actorId = actorId;
    }

    public LocalClusterActorNodeRef(String clusterName, ActorNode node) {
        this(clusterName, node, null);
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
        } catch (Exception e) {
            // @todo: notify sender of the failure
            logger.error(String.format("Failed to send message to %s",sender.toString()),e);
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
    public ActorContainer get() {
        return node;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LocalClusterActorNodeRef that = (LocalClusterActorNodeRef) o;

        if (actorId != null ? !actorId.equals(that.actorId) : that.actorId != null) return false;
        if (!clusterName.equals(that.clusterName)) return false;
        if (!node.equals(that.node)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = clusterName.hashCode();
        result = 31 * result + node.hashCode();
        result = 31 * result + (actorId != null ? actorId.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
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
}
