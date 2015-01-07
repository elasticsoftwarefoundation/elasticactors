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

import org.elasticsoftware.elasticactors.ActorContainer;
import org.elasticsoftware.elasticactors.ActorContainerRef;
import org.elasticsoftware.elasticactors.ActorRef;

import static java.lang.String.format;

/**
 * @author Joost van de Wijgerd
 */
public final class DisconnectedRemoteActorShardRef implements ActorRef,ActorContainerRef {
    public static final String REFSPEC_FORMAT = "actor://%s/%s/shards/%d/%s";
    private final String clusterName;
    private final String actorSystemName;
    private final String actorId;
    private final int shardId;
    private final String refSpec;

    public DisconnectedRemoteActorShardRef(String clusterName, String actorSystemName, String actorId, int shardId) {
        this.clusterName = clusterName;
        this.actorSystemName = actorSystemName;
        this.actorId = actorId;
        this.shardId = shardId;
        this.refSpec = format(REFSPEC_FORMAT,clusterName,actorSystemName,shardId,actorId);
    }


    @Override
    public String getActorPath() {
        return format("%s/shards/%d", actorSystemName, shardId);
    }

    @Override
    public String getActorId() {
        return actorId;
    }

    @Override
    public void tell(Object message, ActorRef sender) {
        tell(message);
    }

    @Override
    public void tell(Object message) throws IllegalStateException {
        throw new IllegalStateException(format("Remote Actor Cluster %s is not configured, ensure a correct remote configuration in the config.yaml",clusterName));
    }

    @Override
    public ActorContainer get() {
        throw new IllegalStateException(format("Remote Actor Cluster %s is not configured, ensure a correct remote configuration in the config.yaml",clusterName));
    }

    @Override
    public String toString() {
        return this.refSpec;
    }

    @Override
    public boolean equals(Object o) {
        return this == o || o instanceof ActorRef && this.toString().equals(o.toString());
    }

    @Override
    public int hashCode() {
        return this.refSpec.hashCode();
    }
}
