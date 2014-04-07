/*
 * Copyright 2013 - 2014 The Original Authors
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

import org.elasticsoftware.elasticactors.ActorNode;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.ActorSystems;

import static java.lang.String.format;

/**
 * @author Joost van de Wijgerd
 */
public final class ActorRefTools {
    private static final String EXCEPTION_FORMAT = "Invalid ActorRef, required spec: [actor://<cluster>/<actorSystem>/[shards|nodes|services]/<shardId>/<actorId (optional)>, actual spec: [%s]";

    private ActorRefTools() {
    }

    public static ActorRef parse(String refSpec, InternalActorSystems actorSystems) {
        // refSpec should look like: actor://<cluster>/<actorSystem>/shards/<shardId>/<actorId>
        if (refSpec.startsWith("actor://")) {
            int actorSeparatorIndex = 8;
            for (int i = 0; i < 3; i++) {
                int nextIndex = refSpec.indexOf('/', actorSeparatorIndex + 1);
                if (nextIndex == -1) {
                    throw new IllegalArgumentException(
                            format(EXCEPTION_FORMAT, refSpec));
                } else {
                    actorSeparatorIndex = nextIndex;
                }
            }
            int nextIndex = refSpec.indexOf('/', actorSeparatorIndex + 1);
            String actorId = (nextIndex == -1) ? null : refSpec.substring(nextIndex + 1);
            actorSeparatorIndex = (nextIndex == -1) ? actorSeparatorIndex : nextIndex;
            String[] components = (actorId == null) ? refSpec.substring(8).split("/") : refSpec.substring(8, actorSeparatorIndex).split("/");

            String clusterName = components[0];
            if (actorSystems.getClusterName().equals(clusterName)) {
                return handleLocalActorSystemReference(refSpec, components, actorId, actorSystems);
            } else {
                return handleRemoteActorSystemReference(refSpec, components, actorId, actorSystems);
            }


        } else {
            throw new IllegalArgumentException(format(EXCEPTION_FORMAT, refSpec));
        }

    }

    private static ActorRef handleLocalActorSystemReference(String refSpec, String[] components, String actorId, InternalActorSystems actorSystems) {
        String clusterName = components[0];
        String actorSystemName = components[1];
        InternalActorSystem actorSystem = actorSystems.get(actorSystemName);
        if (actorSystem == null) {
            throw new IllegalArgumentException(format("Unknown ActorSystem: %s", actorSystemName));
        }
        if ("shards".equals(components[2])) {
            int shardId = Integer.parseInt(components[3]);
            if (shardId >= actorSystem.getConfiguration().getNumberOfShards()) {
                throw new IllegalArgumentException(format("Unknown shard %d for ActorSystem %s. Available shards: %d", shardId, actorSystemName, actorSystem.getConfiguration().getNumberOfShards()));
            }
            return actorSystems.createPersistentActorRef(actorSystem.getShard(format("%s/shards/%d", actorSystemName, shardId)),actorId);
        } else if ("nodes".equals(components[2])) {
            //return new LocalClusterActorNodeRef(clusterName, actorSystem.getNode(components[3]), actorId);
            final ActorNode node = actorSystem.getNode(components[3]);
            if(node != null) {
                return actorSystems.createTempActorRef(node,actorId);
            } else {
                // this node is currently down, send a disconnected ref
                return new DisconnectedActorNodeRef(clusterName,actorSystemName,components[3],actorId);
            }
        } else if ("services".equals(components[2])) {
            //return new ServiceActorRef(clusterName, actorSystem.getNode(), (actorId == null) ? components[3] : format("%s/%s", components[3], actorId));
            // backwards compatibility check
            ActorNode node = actorSystem.getNode(components[3]);
            if(node == null) {
                // set to the local node
                node = actorSystem.getNode();
                // also patch the actorId (if there happened to be a slash in there)
                if(actorId == null) {
                    actorId = components[3];
                } else {
                    actorId = format("%s/%s",components[3],actorId);
                }
            }
            return actorSystems.createServiceActorRef(node, actorId);
        } else {
            throw new IllegalArgumentException(format(EXCEPTION_FORMAT, refSpec));
        }
    }

    private static ActorRef handleRemoteActorSystemReference(String refSpec, String[] components, String actorId, ActorSystems actorSystems) {
        String clusterName = components[0];
        String actorSystemName = components[1];
        if ("shards".equals(components[2])) {
            ActorSystem remoteActorSystem = actorSystems.getRemote(clusterName, actorSystemName);
            int shardId = Integer.parseInt(components[3]);
            if (remoteActorSystem == null) {
                // return a disconnected actor ref that throws exception on tell() so that the deserialization never fails
                // even when the remote actor cannot be reached
                return new DisconnectedRemoteActorShardRef(clusterName,actorSystemName,actorId,shardId);
            }
            return new ActorShardRef(clusterName, ((ShardAccessor) remoteActorSystem).getShard(format("%s/shards/%d", actorSystemName, shardId)), actorId);
        } else if ("nodes".equals(components[2])) {
            throw new IllegalArgumentException("Temporary Actors are not (yet) supported for Remote Actor System instances");
        } else if ("services".equals(components[2])) {
            throw new IllegalArgumentException("Service Actors are not (yet) supported for Remote Actor System instances");
        } else {
            throw new IllegalArgumentException(format(EXCEPTION_FORMAT, refSpec));
        }
    }

    public static boolean isService(ActorRef ref) {
        return ref instanceof ServiceActorRef;
    }
}
