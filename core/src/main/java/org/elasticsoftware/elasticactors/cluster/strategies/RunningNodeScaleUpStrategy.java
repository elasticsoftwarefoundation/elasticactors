package org.elasticsoftware.elasticactors.cluster.strategies;

import org.elasticsoftware.elasticactors.ActorShard;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.cluster.ClusterService;
import org.elasticsoftware.elasticactors.cluster.messaging.ShardReleasedMessage;
import org.elasticsoftware.elasticactors.cluster.protobuf.Clustering;

import java.util.concurrent.LinkedBlockingQueue;

import static java.lang.String.format;

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
        logger.info(format("Releasing Shard %s to Node %s",localShard.getKey().toString(),nextOwner.getId()));
    }

}
