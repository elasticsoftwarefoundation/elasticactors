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

import jakarta.annotation.Nullable;

import static java.lang.String.format;

/**
 *
 * @author  Joost van de Wijgerd
 */
public final class DisconnectedRemoteActorNodeRef extends BaseDisconnectedActorRef {
    private final String nodeId;

    DisconnectedRemoteActorNodeRef(String clusterName, String actorSystemName, String nodeId, @Nullable String actorId) {
        super(actorId, clusterName, generateRefSpec(clusterName, actorSystemName, nodeId, actorId), actorSystemName);
        this.nodeId = nodeId;
    }

    public static String generateRefSpec(String clusterName, String actorSystemName, String nodeId,String actorId) {
        if(actorId != null) {
            return "actor://" + clusterName + "/" + actorSystemName + "/nodes/" + nodeId + "/" + actorId;
        } else {
            return "actor://" + clusterName + "/" + actorSystemName + "/nodes/" + nodeId;
        }
    }

    @Override
    public String getActorPath() {
        return actorSystemName + "/nodes/" + nodeId;
    }

    @Override
    protected String getExceptionMessage() {
        return format("Remote Actor Node %s cannot be reached, make sure to configure the remote actor system [%s] in the configuration",nodeId, clusterName);
    }
}
