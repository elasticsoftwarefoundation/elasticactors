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
import org.elasticsoftware.elasticactors.*;

import javax.annotation.Nullable;

/**
 * {@link org.elasticsoftware.elasticactors.ActorRef} that references an actor in the local cluster
 *
 * @author  Joost van de Wijgerd
 */
public final class ActorShardRef implements ActorRef, ActorContainerRef {
    private static final Logger logger = Logger.getLogger(ActorShardRef.class);
    private final String clusterName;
    private final ActorShard shard;
    private final String actorId;

    public ActorShardRef(String clusterName, ActorShard shard,@Nullable String actorId) {
        this.clusterName = clusterName;
        this.shard = shard;
        this.actorId = actorId;
    }

    public ActorShardRef(String clusterName, ActorShard shard) {
        this(clusterName, shard, null);
    }

    public static String generateRefSpec(String clusterName,ActorShard shard,@Nullable String actorId) {
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
                    logger.error(String.format("Failed onUndeliverable(%s,%s)", sender.toString(), message), e1);
            	}
            }
        }
    }

    @Override
    public void tell(Object message) {
        final ActorRef self = ActorContextHolder.getSelf();
        if(self != null) {
            tell(message,self);
        } else {
            throw new IllegalStateException("Cannot determine ActorRef(self) Only use this method while inside an ElasticActor Lifecycle or on(Message) method!");
        }
    }

    @Override
    public ActorContainer get() {
        return shard;
    }

    @Override
    public boolean equals(Object o) {
        return this == o || o instanceof ActorRef && this.toString().equals(o.toString());
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public String toString() {
        return generateRefSpec(this.clusterName,this.shard,this.actorId);
    }
}
