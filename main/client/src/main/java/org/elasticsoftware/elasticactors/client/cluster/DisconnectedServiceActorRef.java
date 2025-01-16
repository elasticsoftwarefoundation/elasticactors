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

package org.elasticsoftware.elasticactors.client.cluster;

import static java.lang.String.format;

final class DisconnectedServiceActorRef extends BaseDisconnectedActorRef {

    private final String nodeId;

    DisconnectedServiceActorRef(
            String clusterName,
            String actorSystemName,
            String nodeId,
            String serviceId) {
        super(
                serviceId,
                clusterName,
                generateRefSpec(clusterName, actorSystemName, nodeId, serviceId),
                actorSystemName);
        this.nodeId = nodeId;
    }

    public static String generateRefSpec(
            String clusterName,
            String actorSystemName,
            String nodeId,
            String actorId) {
        if (actorId != null) {
            return "actor://" + clusterName + "/" + actorSystemName + "/services/" + nodeId + "/" + actorId;
        } else {
            return "actor://" + clusterName + "/" + actorSystemName + "/services/" + nodeId;
        }
    }

    @Override
    public String getActorPath() {
        return actorSystemName + "/services/" + nodeId;
    }

    @Override
    protected String getExceptionMessage() {
        return format(
                "Cannot reach Remote Actor Service %s for node %s in cluster [%s] using a Remote Actor Reference",
                actorId,
                nodeId,
                clusterName);
    }
}
