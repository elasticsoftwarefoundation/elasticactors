/*
 * Copyright 2013 - 2023 The Original Authors
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

package org.elasticsoftware.elasticactors.cluster.strategies;

import org.elasticsoftware.elasticactors.ActorShard;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.cluster.ClusterService;
import org.elasticsoftware.elasticactors.cluster.messaging.ShardReleasedMessage;
import org.elasticsoftware.elasticactors.cluster.protobuf.Clustering;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author Joost van de Wijgerd
 */
public final class RunningNodeScaleUpStrategy extends MultiNodeScaleUpStrategy {
    private final ClusterService clusterService;

    public RunningNodeScaleUpStrategy(LinkedBlockingQueue<ShardReleasedMessage> shardReleasedMessages, ClusterService clusterService) {
        super(shardReleasedMessages);
        this.clusterService = clusterService;
    }

    @Override
    public void signalRelease(ActorShard localShard, PhysicalNode nextOwner) throws Exception {
        // send the release signal
        Clustering.ClusterMessage.Builder clusterMessageBuilder = Clustering.ClusterMessage.newBuilder();
        Clustering.ShardReleased.Builder shardReleasedBuilder = Clustering.ShardReleased.newBuilder();
        shardReleasedBuilder.setActorSystem(localShard.getKey().getActorSystemName());
        shardReleasedBuilder.setShardId(localShard.getKey().getShardId());
        clusterMessageBuilder.setShardReleased(shardReleasedBuilder);
        byte[] messageBytes = clusterMessageBuilder.build().toByteArray();
        clusterService.sendMessage(nextOwner.getId(),messageBytes);
        logger.info("Releasing Shard {} to Node {}",localShard.getKey(),nextOwner.getId());
    }

}
