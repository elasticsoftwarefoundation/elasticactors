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

package org.elasticsoftware.elasticactors.client.cluster;

import com.google.common.cache.Cache;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.cluster.ActorRefFactory;
import org.springframework.context.ApplicationContext;

import static java.lang.String.format;

public final class RemoteActorShardRefFactory implements ActorRefFactory {

    private static final String EXCEPTION_FORMAT =
            "Invalid ActorRef, required spec: "
                    + "[actor://<cluster>/<actorSystem>/[shards|nodes|services]/<shardId>/"
                    + "<actorId (optional)>, actual spec: [%s]";

    private final ApplicationContext applicationContext;
    private final Cache<String, ActorRef> actorRefCache;
    private RemoteActorSystems actorSystems;

    private RemoteActorSystems getActorSystems() {
        return actorSystems != null
                ? actorSystems
                : (actorSystems = applicationContext.getBean(RemoteActorSystems.class));
    }

    public RemoteActorShardRefFactory(ApplicationContext applicationContext, Cache<String, ActorRef> actorRefCache) {
        this.applicationContext = applicationContext;
        this.actorRefCache = actorRefCache;
    }

    @Override
    public ActorRef create(final String refSpec) {
        ActorRef actorRef = actorRefCache.getIfPresent(refSpec);
        if (actorRef == null) {
            actorRef = parse(refSpec);
            if (!(actorRef instanceof BaseDisconnectedActorRef)) {
                actorRefCache.put(refSpec, actorRef);
            }
        }
        return actorRef;
    }

    public ActorRef parse(String refSpec) {
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
            String[] components = (actorId == null)
                    ? refSpec.substring(8).split("/")
                    : refSpec.substring(8, actorSeparatorIndex).split("/");

            return handleRemoteActorSystemReference(refSpec, components, actorId);

        } else {
            throw new IllegalArgumentException(format(EXCEPTION_FORMAT, refSpec));
        }

    }

    private ActorRef handleRemoteActorSystemReference(
            String refSpec,
            String[] components,
            String actorId) {
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

    private ActorRef handleRemoteShard(String[] components, String actorId) {
        String clusterName = components[0];
        String actorSystemName = components[1];
        RemoteActorSystemInstance remoteActorSystem =
                getActorSystems().get(clusterName, actorSystemName);
        int shardId = Integer.parseInt(components[3]);
        if (remoteActorSystem == null) {
            // return a disconnected actor ref that throws exception on tell() so that the
            // deserialization never fails even when the remote actor cannot be reached
            return new DisconnectedRemoteActorShardRef(
                    clusterName,
                    actorSystemName,
                    actorId,
                    shardId);
        }
        return new RemoteActorShardRef(
                clusterName,
                remoteActorSystem.getShard(actorSystemName + "/shards/" + shardId),
                actorId);
    }

    private ActorRef handleRemoteNode(String[] components, String actorId) {
        String clusterName = components[0];
        String actorSystemName = components[1];
        String nodeId = components[3];
        return new DisconnectedRemoteActorNodeRef(
                clusterName,
                actorSystemName,
                nodeId,
                actorId);
    }

    private ActorRef handleRemoteService(String[] components, String actorId) {
        String clusterName = components[0];
        String actorSystemName = components[1];
        return new DisconnectedServiceActorRef(
                clusterName,
                actorSystemName,
                components[3],
                actorId);
    }

}
