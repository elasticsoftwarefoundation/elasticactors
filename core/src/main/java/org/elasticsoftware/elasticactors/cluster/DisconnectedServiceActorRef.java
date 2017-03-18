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

import static java.lang.String.format;

/**
 *
 * @author  Joost van de Wijgerd
 */
public final class DisconnectedServiceActorRef extends BaseDisconnectedActorRef {
    private final String nodeId;

    DisconnectedServiceActorRef(String clusterName, String actorSystemName, String nodeId, String serviceId) {
        super(serviceId, clusterName, generateRefSpec(clusterName, actorSystemName, nodeId, serviceId), actorSystemName);
        this.nodeId = nodeId;
    }

    public static String generateRefSpec(String clusterName, String actorSystemName, String nodeId,String actorId) {
        if(actorId != null) {
            return format("actor://%s/%s/services/%s/%s",clusterName,actorSystemName,nodeId,actorId);
        } else {
            return format("actor://%s/%s/services/%s",clusterName,actorSystemName,nodeId);
        }
    }

    @Override
    public String getActorPath() {
        return format("%s/services/%s",actorSystemName,nodeId);
    }

    @Override
    protected String getExceptionMessage() {
        return format("Actor Node %s is not active, referenced service cannot be reached right now",nodeId);
    }
}
