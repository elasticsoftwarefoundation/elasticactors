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

final class DisconnectedRemoteActorShardRef extends BaseDisconnectedActorRef {

    private final int shardId;

    DisconnectedRemoteActorShardRef(
            String clusterName,
            String actorSystemName,
            String actorId,
            int shardId) {
        super(
                actorId,
                clusterName,
                generateRefSpec(clusterName, actorSystemName, shardId, actorId),
                actorSystemName);
        this.shardId = shardId;
    }

    public static String generateRefSpec(
            String clusterName,
            String actorSystemName,
            int shardId,
            String actorId) {
        if (actorId != null) {
            return "actor://" + clusterName + "/" + actorSystemName + "/shards/" + shardId + "/" + actorId;
        } else {
            return "actor://" + clusterName + "/" + actorSystemName + "/shards/" + shardId;
        }
    }

    @Override
    public String getActorPath() {
        return actorSystemName + "/shards/" + shardId;
    }

    @Override
    protected String getExceptionMessage() {
        return format(
                "Remote Actor Cluster %s is not configured, ensure a correct remote configuration",
                clusterName);
    }
}
