/*
 * Copyright 2013 eBuddy BV
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

import org.apache.log4j.Logger;
import org.elasticsoftware.elasticactors.ActorContainer;
import org.elasticsoftware.elasticactors.ActorContainerRef;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorShard;
import org.elasticsoftware.elasticactors.TypedActor;

/**
 * {@link org.elasticsoftware.elasticactors.ActorRef} that references an actor in the local cluster
 *
 * @author  Joost van de Wijgerd
 */
public final class LocalClusterActorShardRef implements ActorRef, ActorContainerRef {
    private static final Logger logger = Logger.getLogger(LocalClusterActorShardRef.class);
    private final String clusterName;
    private final ActorShard shard;
    private final String actorId;

    public LocalClusterActorShardRef(String clusterName, ActorShard shard, String actorId) {
        this.clusterName = clusterName;
        this.shard = shard;
        this.actorId = actorId;
    }

    public LocalClusterActorShardRef(String clusterName, ActorShard shard) {
        this(clusterName, shard, null);
    }

    @Override
    public String getActorPath() {
        return String.format("%s/shards/%d",shard.getKey().getActorSystemName(),shard.getKey().getShardId());
    }

    public String getActorId() {
        return actorId;
    }

    @Override
    public void tell(Object message, ActorRef sender) {
        try {
            shard.sendMessage(sender,this,message);
        } catch (Exception e) {
            logger.error(String.format("Failed to send message to %s", 
            		sender != null ? sender.toString() : "null"), e);
            if(sender != null && sender instanceof TypedActor<?>) {
            	try {
            		((TypedActor<?>) sender).onUndeliverable(sender, message);
            	} catch (Exception e1) {
                    logger.error(String.format("Failed onUndeliverable(%s,%s)", 
                    		sender != null ? sender.toString() : "null", message), e1);
            	}
            }
        }
    }

    @Override
    public ActorContainer get() {
        return shard;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LocalClusterActorShardRef that = (LocalClusterActorShardRef) o;

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
