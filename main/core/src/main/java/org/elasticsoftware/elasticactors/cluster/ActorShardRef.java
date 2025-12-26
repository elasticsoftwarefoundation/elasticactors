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

package org.elasticsoftware.elasticactors.cluster;

import jakarta.annotation.Nullable;
import org.elasticsoftware.elasticactors.*;

/**
 * {@link org.elasticsoftware.elasticactors.ActorRef} that references an actor in the local cluster
 *
 * @author  Joost van de Wijgerd
 */
public final class ActorShardRef extends BaseActorRef implements ActorContainerRef {
    private final ActorShard shard;

    public ActorShardRef(String clusterName, ActorShard shard,@Nullable String actorId, InternalActorSystem actorSystem) {
        super(actorSystem, clusterName, actorId, generateRefSpec(clusterName, shard, actorId));
        this.shard = shard;
    }

    public ActorShardRef(InternalActorSystem actorSystem, String clusterName, ActorShard shard) {
        this(clusterName, shard, null, actorSystem);
    }

    public static String generateRefSpec(String clusterName,ActorShard shard,@Nullable String actorId) {
        if(actorId != null) {
            return "actor://" + clusterName + "/" + shard.getKey().getActorSystemName() + "/shards/" + shard.getKey().getShardId() + "/" + actorId;
        } else {
            return "actor://" + clusterName + "/" + shard.getKey().getActorSystemName() + "/shards/" + shard.getKey().getShardId();
        }
    }

    @Override
    public String getActorPath() {
        return shard.getKey().getActorSystemName() + "/shards/" + shard.getKey().getShardId();
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
            throw new IllegalStateException("Cannot determine ActorRef(self). Only use this method while inside an ElasticActor Lifecycle or on(Message) method!");
        }
    }

    @Override
    public boolean isLocal() {
        return shard.getOwningNode().isLocal();
    }

    @Override
    public ActorContainer getActorContainer() {
        return shard;
    }

}
