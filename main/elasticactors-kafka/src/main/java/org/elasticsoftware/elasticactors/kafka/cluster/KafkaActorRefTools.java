/*
 * Copyright 2013 - 2024 The Original Authors
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

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.cluster.ActorRefTools;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.kafka.KafkaActorNode;

public final class KafkaActorRefTools extends ActorRefTools {
    private final KafkaInternalActorSystems actorSystems;

    public KafkaActorRefTools(KafkaInternalActorSystems actorSystems) {
        super(actorSystems);
        this.actorSystems = actorSystems;
    }

    @Override
    protected ActorRef handleNode(String[] components, String partitionAndActorId) {
        // actorId is actually <partition>/actorId
        String clusterName = components[0];
        String actorSystemName = components[1];
        int actorSeparator = partitionAndActorId.indexOf('/'); // can also be just the partition!
        int partition = actorSeparator != -1 ? Integer.parseInt(partitionAndActorId.substring(0, actorSeparator)) : Integer.parseInt(partitionAndActorId);
        String actorId = actorSeparator != -1 ? partitionAndActorId.substring(actorSeparator+1) : null;
        InternalActorSystem actorSystem = actorSystems.get(actorSystemName);
        final KafkaActorNode node = (KafkaActorNode) actorSystem.getNode(components[3]);
        if(node != null) {
            return actorSystems.createTempActorRef(node, partition, actorId);
        } else {
            // this node is currently down, send a disconnected ref
            return new DisconnectedPartitionedActorNodeRef(clusterName, actorSystemName, components[3], partition, actorId);
        }
    }
}
