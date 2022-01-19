/*
 *   Copyright 2013 - 2022 The Original Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.elasticsoftware.elasticactors.cluster;

import org.elasticsoftware.elasticactors.ActorNode;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorShard;
import org.elasticsoftware.elasticactors.ActorSystem;

import java.util.concurrent.ThreadLocalRandom;

import static java.lang.String.format;

/**
 * @author Joost van de Wijgerd
 */
public class ActorRefTools {
    private static final String EXCEPTION_FORMAT = "Invalid ActorRef, required spec: [actor://<cluster>/<actorSystem>/[shards|nodes|services]/<shardId>/<actorId (optional)>, actual spec: [%s]";
    private final InternalActorSystems actorSystems;

    public ActorRefTools(InternalActorSystems actorSystems) {
        this.actorSystems = actorSystems;
    }

    public final ActorRef parse(String refSpec) {
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
                return handleLocalActorSystemReference(refSpec, components, actorId);
            } else {
                return handleRemoteActorSystemReference(refSpec, components, actorId);
            }


        } else {
            throw new IllegalArgumentException(format(EXCEPTION_FORMAT, refSpec));
        }

    }

    private ActorRef handleLocalActorSystemReference(String refSpec, String[] components, String actorId) {
        String actorSystemName = components[1];
        InternalActorSystem actorSystem = actorSystems.get(actorSystemName);
        if (actorSystem == null) {
            throw new IllegalArgumentException(format("Unknown ActorSystem: %s", actorSystemName));
        }
        if ("shards".equals(components[2])) {
            return handleShard(components, actorId);
        } else if ("nodes".equals(components[2])) {
            return handleNode(components, actorId);
        } else if ("services".equals(components[2])) {
            return handleService(components, actorId);
        } else {
            throw new IllegalArgumentException(format(EXCEPTION_FORMAT, refSpec));
        }
    }

    protected ActorRef handleShard(String[] components, String actorId) {
        String actorSystemName = components[1];
        InternalActorSystem actorSystem = actorSystems.get(actorSystemName);
        int shardId = Integer.parseInt(components[3]);
        if (shardId >= actorSystem.getConfiguration().getNumberOfShards()) {
            throw new IllegalArgumentException(format("Unknown shard %d for ActorSystem %s. Available shards: %d", shardId, actorSystemName, actorSystem.getConfiguration().getNumberOfShards()));
        }
        return actorSystems.createPersistentActorRef(
            actorSystem.getShard(actorSystemName + "/shards/" + shardId),
            actorId);
    }

    protected ActorRef handleNode(String[] components, String actorId) {
        String clusterName = components[0];
        String actorSystemName = components[1];
        InternalActorSystem actorSystem = actorSystems.get(actorSystemName);
        final ActorNode node = actorSystem.getNode(components[3]);
        if(node != null) {
            return actorSystems.createTempActorRef(node, actorId);
        } else {
            // this node is currently down, send a disconnected ref
            return new DisconnectedActorNodeRef(clusterName,actorSystemName,components[3], actorId);
        }
    }

    protected ActorRef handleService(String[] components, String actorId) {
        String clusterName = components[0];
        String actorSystemName = components[1];
        InternalActorSystem actorSystem = actorSystems.get(actorSystemName);
        ActorNode node = actorSystem.getNode(components[3]);
        if(node == null) {
            return new DisconnectedServiceActorRef(clusterName,actorSystemName,components[3],actorId);
        } else {
            return actorSystems.createServiceActorRef(node, actorId);
        }
    }

    private ActorRef handleRemoteActorSystemReference(String refSpec, String[] components, String actorId) {
        if ("shards".equals(components[2])) {
            return handleRemoteShard(components, actorId);
        } else if ("nodes".equals(components[2])) {
            return handleRemoteNode(components, actorId);
        } else if ("services".equals(components[2])) {
            return handleRemoteService(components, actorId);
        } else {
            throw new IllegalArgumentException(format(EXCEPTION_FORMAT, refSpec));
        }
    }

    protected ActorRef handleRemoteShard(String[] components, String actorId) {
        String clusterName = components[0];
        String actorSystemName = components[1];
        ActorSystem remoteActorSystem = actorSystems.getRemote(clusterName, actorSystemName);
        int shardId = Integer.parseInt(components[3]);
        if (remoteActorSystem == null) {
            // return a disconnected actor ref that throws exception on tell() so that the deserialization never fails
            // even when the remote actor cannot be reached
            return new DisconnectedRemoteActorShardRef(clusterName,actorSystemName,actorId,shardId);
        }
        return new ActorShardRef(
            clusterName,
            ((ShardAccessor) remoteActorSystem).getShard(actorSystemName + "/shards/" + shardId),
            actorId,
            actorSystems.get(null)
        );
    }

    protected ActorRef handleRemoteNode(String[] components, String actorId) {
        String clusterName = components[0];
        String actorSystemName = components[1];
        ActorSystem remoteActorSystem = actorSystems.getRemote(clusterName, actorSystemName);
        if(remoteActorSystem == null) {
            return new DisconnectedRemoteActorNodeRef(clusterName, actorSystemName, components[3], actorId);
        } else {
            // get a random shard to use as a hub
            int randomShardId = ThreadLocalRandom.current().nextInt(((ShardAccessor) remoteActorSystem).getNumberOfShards());
            ActorShard actorShard = ((ShardAccessor) remoteActorSystem).getShard(randomShardId);
            return new RemoteClusterActorNodeRef(actorSystems.get(null), clusterName, actorShard, components[3], actorId);
        }
    }

    protected ActorRef handleRemoteService(String[] components, String actorId) {
        String clusterName = components[0];
        String actorSystemName = components[1];
        return new DisconnectedServiceActorRef(clusterName, actorSystemName, components[3], actorId);
    }

    public static boolean isService(ActorRef ref) {
        return ref instanceof ServiceActorRef;
    }
}
