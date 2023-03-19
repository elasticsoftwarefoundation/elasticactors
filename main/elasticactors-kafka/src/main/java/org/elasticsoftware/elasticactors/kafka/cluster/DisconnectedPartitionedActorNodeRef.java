/*
 * Copyright 2013 - 2023 The Original Authors
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

import org.elasticsoftware.elasticactors.cluster.BaseDisconnectedActorRef;

import jakarta.annotation.Nullable;

import static java.lang.String.format;

/**
 *
 * @author  Joost van de Wijgerd
 */
public final class DisconnectedPartitionedActorNodeRef extends BaseDisconnectedActorRef {
    private final String nodeId;
    private final int partition;

    DisconnectedPartitionedActorNodeRef(String clusterName, String actorSystemName, String nodeId, int partition, @Nullable String actorId) {
        super(actorId, clusterName, generateRefSpec(clusterName, actorSystemName, nodeId, partition, actorId), actorSystemName);
        this.nodeId = nodeId;
        this.partition = partition;
    }

    public static String generateRefSpec(String clusterName, String actorSystemName, String nodeId, int partition, String actorId) {
        if(actorId != null) {
            return "actor://" + clusterName + "/" + actorSystemName + "/nodes/" + nodeId + "/" + partition + "/" + actorId;
        } else {
            return "actor://" + clusterName + "/" + actorSystemName + "/nodes/" + nodeId + "/" + partition;
        }
    }

    @Override
    public String getActorPath() {
        return actorSystemName + "/nodes/" + nodeId + "/" + partition;
    }

    @Override
    protected String getExceptionMessage() {
        return format("Actor Node %s is not active, referenced actorId cannot be reached and probably doesn't exist anymore. It is a Bad Idea to serialize Temp Actor Refs",nodeId);
    }

}
