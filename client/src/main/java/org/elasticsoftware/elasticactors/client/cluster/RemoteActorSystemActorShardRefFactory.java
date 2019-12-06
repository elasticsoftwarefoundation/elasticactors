/*
 *   Copyright 2013 - 2019 The Original Authors
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
import org.elasticsoftware.elasticactors.cluster.ShardAccessor;

import static java.lang.String.format;

public final class RemoteActorSystemActorShardRefFactory implements ActorRefFactory {

    private static final String EXCEPTION_FORMAT =
            "Invalid ActorRef, required spec: "
                    + "[actor://<cluster>/<actorSystem>/[shards|nodes|services]/<shardId>/"
                    + "<actorId (optional)>, actual spec: [%s]";

    private final ShardAccessor shardAccessor;
    private final Cache<String, ActorRef> actorRefCache;

    public RemoteActorSystemActorShardRefFactory(
            ShardAccessor shardAccessor,
            Cache<String, ActorRef> actorRefCache) {
        this.shardAccessor = shardAccessor;
        this.actorRefCache = actorRefCache;
    }

    @Override
    public ActorRef create(final String refSpec) {
        ActorRef actorRef = actorRefCache.getIfPresent(refSpec);
        if (actorRef == null) {
            actorRef = parse(refSpec);
            actorRefCache.put(refSpec, actorRef);
        }
        return actorRef;
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
            throw new UnsupportedOperationException("Remote ActorSystem cannot resolve nodes");
        } else if ("services".equals(components[2])) {
            throw new UnsupportedOperationException("Remote ActorSystem cannot resolve services");
        } else {
            throw new IllegalArgumentException(format(EXCEPTION_FORMAT, refSpec));
        }
    }

    protected ActorRef handleRemoteShard(String[] components, String actorId) {
        String clusterName = components[0];
        String actorSystemName = components[1];
        int shardId = Integer.parseInt(components[3]);
        return new RemoteActorSystemActorShardRef(
                clusterName,
                shardAccessor.getShard(format("%s/shards/%d", actorSystemName, shardId)),
                actorId);
    }

}
