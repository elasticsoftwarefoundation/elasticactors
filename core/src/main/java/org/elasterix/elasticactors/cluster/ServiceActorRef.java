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

package org.elasterix.elasticactors.cluster;

import org.apache.log4j.Logger;
import org.elasterix.elasticactors.ActorContainer;
import org.elasterix.elasticactors.ActorContainerRef;
import org.elasterix.elasticactors.ActorNode;
import org.elasterix.elasticactors.ActorRef;

/**
 * {@link org.elasterix.elasticactors.ActorRef} that references an actor in the local cluster
 *
 * @author  Joost van de Wijgerd
 */
public final class ServiceActorRef implements ActorRef, ActorContainerRef {
    private static final Logger logger = Logger.getLogger(ServiceActorRef.class);
    private final String clusterName;
    private final ActorNode node;
    private final String serviceId;

    public ServiceActorRef(String clusterName, ActorNode node, String serviceId) {
        this.clusterName = clusterName;
        this.node = node;
        this.serviceId = serviceId;
    }

    @Override
    public String getActorPath() {
        return String.format("%s/services",node.getKey().getActorSystemName());
    }

    public String getActorId() {
        return serviceId;
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
    public ActorContainer get() {
        return node;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ServiceActorRef that = (ServiceActorRef) o;

        if (serviceId != null ? !serviceId.equals(that.serviceId) : that.serviceId != null) return false;
        if (!clusterName.equals(that.clusterName)) return false;
        if (!node.equals(that.node)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = clusterName.hashCode();
        result = 31 * result + node.hashCode();
        result = 31 * result + (serviceId != null ? serviceId.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return String.format("actor://%s/%s/services/%s",
                             clusterName,
                             node.getKey().getActorSystemName(),
                             serviceId);

    }
}
