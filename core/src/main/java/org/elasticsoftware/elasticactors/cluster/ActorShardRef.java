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

import org.elasticsoftware.elasticactors.*;

import javax.annotation.Nullable;

/**
 * {@link org.elasticsoftware.elasticactors.ActorRef} that references an actor in the local cluster
 *
 * @author  Joost van de Wijgerd
 */
public final class ActorShardRef implements ActorRef, ActorContainerRef {
    private final ActorShard shard;
    private final String actorId;
    private final String refSpec;

    public ActorShardRef(String clusterName, ActorShard shard,@Nullable String actorId) {
        this.shard = shard;
        this.actorId = actorId;
        this.refSpec = generateRefSpec(clusterName, shard, actorId);
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
        } catch(MessageDeliveryException e) {
            throw e;
        } catch (Exception e) {
            throw new MessageDeliveryException("Unexpected Exception while sending message",e,false);
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
    public boolean isLocal() {
        return shard.getOwningNode().isLocal();
    }

    @Override
    public ActorContainer get() {
        return shard;
    }

    @Override
    public boolean equals(Object o) {
        return this == o || o instanceof ActorRef && this.refSpec.equals(o.toString());
    }

    @Override
    public int hashCode() {
        return this.refSpec.hashCode();
    }

    @Override
    public String toString() {
        return this.refSpec;
    }
}
