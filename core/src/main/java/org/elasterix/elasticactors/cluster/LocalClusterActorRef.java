/*
 * Copyright 2013 Joost van de Wijgerd
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

package org.elasterix.elasticactors.cluster;

import org.elasterix.elasticactors.ActorRef;
import org.elasterix.elasticactors.ActorShard;

/**
 * {@link org.elasterix.elasticactors.ActorRef} that references an actor in the local cluster
 *
 * @author  Joost van de Wijgerd
 */
public final class LocalClusterActorRef implements ActorRef {
    private final String clusterName;
    private final ActorShard shard;
    private final String actorId;

    public LocalClusterActorRef(String clusterName, ActorShard shard, String actorId) {
        this.clusterName = clusterName;
        this.shard = shard;
        this.actorId = actorId;
    }

    public LocalClusterActorRef(String clusterName,ActorShard shard) {
        this(clusterName, shard,null);
    }

    @Override
    public void tell(Object message, ActorRef sender) throws Exception {
        shard.sendMessage(sender,this,message);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LocalClusterActorRef that = (LocalClusterActorRef) o;

        if (actorId != null ? !actorId.equals(that.actorId) : that.actorId != null) return false;
        if (!clusterName.equals(that.clusterName)) return false;
        if (!shard.equals(that.shard)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = clusterName.hashCode();
        result = 31 * result + shard.hashCode();
        result = 31 * result + (actorId != null ? actorId.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        if(actorId != null) {
            return String.format("actor://%s/%s/shards/%d/%s",
                                 clusterName,shard.getKey().getActorSystemName(),
                                 shard.getKey().getShardId(),actorId);
        } else {
            return String.format("actor://%s/%s/shards/%d",
                                 clusterName,shard.getKey().getActorSystemName(),
                                 shard.getKey().getShardId());
        }
    }
}
